"""Servicio de forwarding: despacho de paquetes entrantes y reenvío/salida.

``ForwardingService`` no abre ni acepta sockets — eso es responsabilidad de
``node.py`` , que:
    1. Acepta conexiones TCP entrantes y las guarda en ``connections``
       (``dict[node_id, socket]``).
    2. Por cada línea NDJSON que llega de un vecino, llama a
       ``forwarding_service.handle_incoming(linea, node_id_del_vecino)``.

``ForwardingService`` sí decide, para cada paquete, si se entrega
localmente, se reenvía, o se descarta, y expone ``send_to``/``send_user_message``
como la API de salida que usará tanto ``node.py`` (CLI) como este mismo
servicio internamente.
"""

from __future__ import annotations

import logging
from typing import Any

from src.packet import Packet
from src.routing import RoutingService

logger = logging.getLogger(__name__)


class ForwardingService:
    """Procesa paquetes entrantes y decide su reenvío según el modo activo."""

    def __init__(
        self,
        self_id: str,
        mode: str,
        routing_service: RoutingService,
        connections: dict[str, Any],
    ) -> None:
        if mode not in ("dijkstra", "flooding", "lsr"):
            raise ValueError(f"mode inválido: {mode!r}")

        self.self_id = self_id
        self.mode = mode
        self.routing_service = routing_service
        self.connections = connections

        self._handlers = {
            "hello": self._handle_hello,
            "echo": self._handle_echo,
            "message": self._handle_message,
            "info": self._handle_info,
        }

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Punto de enganche para trabajo propio del servicio de forwarding.

        El accept-loop TCP real vive en ``node.py`` (que posee el socket
        servidor y las conexiones salientes); este método no abre sockets.
        Se deja como no-op explícito para que ``node.py`` pueda llamarlo de
        forma simétrica a ``routing_service.start()`` sin condicionales, y
        como lugar natural para inicializar trabajo futuro (p. ej. métricas)
        sin cambiar la interfaz pública.
        """
        logger.debug(
            "[%s] ForwardingService listo (accept-loop TCP a cargo de node.py)",
            self.self_id,
        )

    # ------------------------------------------------------------------
    # Entrada: despacho de paquetes recibidos
    # ------------------------------------------------------------------
    def handle_incoming(self, raw_line: str | bytes, sender_node_id: str) -> None:
        """Parsea y despacha una línea NDJSON recibida de ``sender_node_id``.

        Un paquete inválido o con TTL agotado se descarta silenciosamente
        (share.md §3.3 / §4.5): nunca debe tumbar el nodo ni la conexión.
        """
        packet = Packet.from_json(raw_line)
        if packet is None:
            logger.debug("[%s] paquete JSON inválido de %s, descartado", self.self_id, sender_node_id)
            return

        if packet.is_expired:
            logger.debug("[%s] paquete %s con ttl agotado, descartado", self.self_id, packet.id)
            return

        handler = self._handlers.get(packet.type)
        if handler is None:
            logger.debug("[%s] tipo de paquete desconocido: %s", self.self_id, packet.type)
            return

        try:
            handler(packet, sender_node_id)
        except Exception:  # noqa: BLE001 - un paquete malformado no debe tumbar el nodo
            logger.exception(
                "[%s] error procesando paquete %s de %s", self.self_id, packet.id, sender_node_id
            )

    def _handle_hello(self, packet: Packet, sender_node_id: str) -> None:
        echo = Packet.make_echo(
            proto=self.mode,
            from_node=self.self_id,
            to=packet.from_node,
            hello_packet=packet,
        )
        self.send_to(packet.from_node, echo)

    def _handle_echo(self, packet: Packet, sender_node_id: str) -> None:
        neighbor = self.routing_service.neighbors.get(packet.from_node)
        if neighbor is None:
            logger.debug("[%s] echo de vecino desconocido: %s", self.self_id, packet.from_node)
            return
        self.routing_service.handle_echo(packet, neighbor)

    def _handle_message(self, packet: Packet, sender_node_id: str) -> None:
        if self.mode == "flooding":
            # Dedupe primero: un duplicado ni se entrega ni se reenvía
            # (share.md §8: "se descarta sin reprocesar ni reenviar").
            if not self.routing_service.flooding_router.should_forward(packet.id):
                return

        if packet.to == self.self_id:
            self._deliver_message(packet)
            return

        self.forward_message(packet, sender_node_id)

    def _handle_info(self, packet: Packet, sender_node_id: str) -> None:
        if self.mode != "lsr":
            # info solo tiene semántica en lsr; en otros modos se ignora.
            logger.debug("[%s] paquete info recibido fuera de modo lsr, ignorado", self.self_id)
            return

        is_new = self.routing_service.handle_info(packet)
        if is_new:
            self._reflood_info(packet, sender_node_id)

    def _deliver_message(self, packet: Packet) -> None:
        hops = packet.get_hops()
        origin = hops[0] if hops else packet.from_node
        print(f"[{self.self_id}] mensaje de {origin}: {packet.payload}")

    # ------------------------------------------------------------------
    # Reenvío
    # ------------------------------------------------------------------
    def forward_message(self, packet: Packet, sender_node_id: str | None) -> None:
        """Reenvía un ``message`` que no es para este nodo, según el modo activo."""
        packet.decrement_ttl()
        if packet.is_expired:
            logger.debug("[%s] mensaje %s expiró al reenviarlo, descartado", self.self_id, packet.id)
            return
        packet.update_sender(self.self_id)
        packet.add_hop(self.self_id)

        if self.mode == "flooding":
            targets = self.routing_service.flooding_router.get_flood_targets(
                self.routing_service.neighbors, sender_id=sender_node_id
            )
            for target in targets:
                self.send_to(target, packet)
            return

        # dijkstra / lsr: un único next-hop precomputado.
        next_hop = self.routing_service.get_next_hop(packet.to)
        if next_hop is None:
            logger.debug("[%s] sin ruta hacia %s, mensaje %s descartado", self.self_id, packet.to, packet.id)
            return
        self.send_to(next_hop, packet)

    def _reflood_info(self, packet: Packet, sender_node_id: str | None) -> None:
        """Reenvía (por flooding) un LSP nuevo a todos los vecinos activos.

        Los LSP se difunden siempre por flooding, incluso en topologías
        donde el modo activo usa Dijkstra/LSR para mensajes de usuario
        (share.md §11.3 / §12.3).
        """
        packet.decrement_ttl()
        if packet.is_expired:
            logger.debug("[%s] LSP %s expiró al reflood-earlo, descartado", self.self_id, packet.id)
            return
        packet.update_sender(self.self_id)

        targets = self.routing_service.flooding_router.get_flood_targets(
            self.routing_service.neighbors, sender_id=sender_node_id
        )
        for target in targets:
            self.send_to(target, packet)

    # ------------------------------------------------------------------
    # Salida
    # ------------------------------------------------------------------
    def send_to(self, node_id: str, packet: Packet) -> bool:
        """Serializa ``packet`` y lo escribe en el socket asociado a ``node_id``.

        Retorna ``True`` si se escribió sin error, ``False`` en cualquier
        otro caso (sin conexión activa, o error de socket).
        """
        sock = self.connections.get(node_id)
        if sock is None:
            logger.warning("[%s] sin conexión activa hacia %s, paquete %s descartado", self.self_id, node_id, packet.id)
            return False
        try:
            sock.sendall(packet.to_json().encode("utf-8"))
            return True
        except OSError:
            logger.exception("[%s] error de socket enviando a %s", self.self_id, node_id)
            return False

    def send_user_message(self, to: str, text: str) -> None:
        """Crea y despacha un ``message`` de usuario hacia ``to``."""
        ttl = self.routing_service.initial_ttl
        packet = Packet.make_message(
            proto=self.mode, from_node=self.self_id, to=to, text=text, ttl=ttl
        )

        if to == self.self_id:
            self._deliver_message(packet)
            return

        if self.mode == "flooding":
            # Registrar el id propio evita reprocesar nuestra propia difusión
            # si nos llega de vuelta por un ciclo en la malla.
            self.routing_service.flooding_router.should_forward(packet.id)
            targets = self.routing_service.flooding_router.get_flood_targets(
                self.routing_service.neighbors, sender_id=None
            )
            for target in targets:
                self.send_to(target, packet)
            return

        next_hop = self.routing_service.get_next_hop(to)
        if next_hop is None:
            logger.warning("[%s] sin ruta hacia %s, mensaje de usuario descartado", self.self_id, to)
            return
        self.send_to(next_hop, packet)

    def __repr__(self) -> str:
        return f"ForwardingService(self_id={self.self_id}, mode={self.mode})"
