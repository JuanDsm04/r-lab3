"""Servicio de ruteo: health-check de vecinos y mantenimiento de rutas.

``RoutingService`` es el hilo "cerebro" del nodo: no toca sockets
directamente (eso es responsabilidad de ``ForwardingService`` / ``node.py``),
sino que decide *hacia dónde* debe ir cada paquete y mantiene vivo el
health-check con los vecinos vía ``hello``/``echo``.

Contrato de integración (``node.py``):
    ``send_callback`` es una función ``(node_id: str, packet: Packet) -> None``
    que efectivamente escribe el paquete en el socket del vecino. En la
    práctica será ``forwarding_service.send_to``. Como ``RoutingService`` y
    ``ForwardingService`` se referencian mutuamente, ``node.py`` puede optar
    por:
        1. Construir primero ``forwarding_service`` con ``routing_service=None``
           y asignarlo después, o
        2. Pasar un *closure* (``lambda node_id, pkt: forwarding_service.send_to(node_id, pkt)``)
           que resuelve ``forwarding_service`` en el momento de la llamada,
           construyendo ``routing_service`` primero.

    ``neighbors`` es el ``dict[node_id, Neighbor]`` que produce
    ``config_loader.load_config``.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Optional

from src.algorithms.dijkstra import compute_routing_table, get_next_hop as dijkstra_next_hop
from src.algorithms.flooding import FloodingRouter
from src.algorithms.lsr import LSRRouter
from src.neighbor import Neighbor
from src.packet import Packet

logger = logging.getLogger(__name__)

SendCallback = Callable[[str, Packet], Any]


class RoutingService:
    """Mantiene vecinos vivos y la(s) tabla(s) de ruteo del nodo.

    Un ``RoutingService`` se instancia una vez por nodo y su comportamiento
    varía según ``mode`` (``dijkstra`` | ``flooding`` | ``lsr``):

    - ``dijkstra``: la tabla se calcula una sola vez al arrancar, a partir de
      la topología estática cargada por ``config_loader``.
    - ``flooding``: no hay tabla; solo importa el estado de los vecinos y el
      ``FloodingRouter`` (deduplicación + selección de vecinos activos).
    - ``lsr``: usa ``FloodingRouter`` para difundir sus propios LSP y
      ``LSRRouter`` (que internamente llama a Dijkstra) para reconstruir la
      topología y calcular rutas.
    """

    def __init__(
        self,
        self_id: str,
        mode: str,
        config: dict[str, Any],
        neighbors: dict[str, Neighbor],
        send_callback: SendCallback,
    ) -> None:
        if mode not in ("dijkstra", "flooding", "lsr"):
            raise ValueError(f"mode inválido: {mode!r}")

        self.self_id = self_id
        self.mode = mode
        self.config = config
        self.neighbors = neighbors
        self.send_callback = send_callback

        params = config["params"]
        self.initial_ttl: int = params["initial_ttl"]
        self.hello_interval_sec: float = params["hello_interval_sec"]
        self.hello_timeout_sec: float = params["hello_timeout_sec"]
        self.dedup_cache_ttl_sec: float = params["dedup_cache_ttl_sec"]

        # FloodingRouter se usa tanto en modo flooding puro como para
        # difundir los LSP en modo lsr (sección 11.3 de share.md).
        self.flooding_router: Optional[FloodingRouter] = (
            FloodingRouter(cache_ttl_sec=self.dedup_cache_ttl_sec)
            if mode in ("flooding", "lsr")
            else None
        )
        self.lsr_router: Optional[LSRRouter] = LSRRouter() if mode == "lsr" else None
        self.dijkstra_table: dict[str, dict[str, Any]] = {}

        self._lsp_seq = 0
        # Estado de hello/echo pendiente por vecino, para detectar timeouts:
        # {node_id: {"last_sent_seq": int, "last_acked_seq": int}}
        self._hello_state: dict[str, dict[str, int]] = {}

        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._hello_thread: Optional[threading.Thread] = None
        self._pending_timers: list[threading.Timer] = []

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Arranca el servicio: calcula rutas iniciales y lanza el hilo de hello."""
        if self.mode == "dijkstra":
            topology = self.config.get("topology", {})
            self.dijkstra_table = compute_routing_table(self.self_id, topology)
            logger.info(
                "[%s] tabla dijkstra calculada: %d destinos",
                self.self_id,
                len(self.dijkstra_table),
            )

        self._stop_event.clear()
        self._hello_thread = threading.Thread(
            target=self.run_hello_loop,
            name=f"routing-hello-{self.self_id}",
            daemon=True,
        )
        self._hello_thread.start()

        if self.mode == "lsr":
            # Regla de arranque (share.md §14): en lsr, cada nodo anuncia
            # su LSP propio al iniciar.
            self.announce_lsp()

    def stop(self) -> None:
        """Detiene el hilo de hello y cancela timers de timeout pendientes."""
        self._stop_event.set()
        with self._lock:
            for timer in self._pending_timers:
                timer.cancel()
            self._pending_timers.clear()
        if self._hello_thread is not None:
            self._hello_thread.join(timeout=self.hello_interval_sec + 1)

    # ------------------------------------------------------------------
    # Health-check: hello / echo
    # ------------------------------------------------------------------
    def run_hello_loop(self) -> None:
        """Envía ``hello`` a todos los vecinos cada ``hello_interval_sec``.

        Se envía incluso a vecinos marcados como caídos, para poder detectar
        su recuperación (share.md §10).
        """
        while not self._stop_event.is_set():
            for neighbor in list(self.neighbors.values()):
                try:
                    self._send_hello(neighbor)
                except Exception:  # noqa: BLE001 - un vecino no debe tumbar el loop
                    logger.exception("[%s] error enviando hello a %s", self.self_id, neighbor.node_id)
            self._stop_event.wait(self.hello_interval_sec)

    def _send_hello(self, neighbor: Neighbor) -> None:
        seq = neighbor.next_hello_seq()
        packet = Packet.make_hello(
            proto=self.mode, from_node=self.self_id, to=neighbor.node_id, seq=seq
        )
        with self._lock:
            state = self._hello_state.setdefault(
                neighbor.node_id, {"last_sent_seq": 0, "last_acked_seq": 0}
            )
            state["last_sent_seq"] = seq

        self._safe_send(neighbor.node_id, packet)

        timer = threading.Timer(
            self.hello_timeout_sec, self._check_hello_timeout, args=(neighbor, seq)
        )
        timer.daemon = True
        with self._lock:
            # Purga timers ya disparados antes de acumular el nuevo, para no
            # crecer indefinidamente en corridas largas.
            self._pending_timers = [t for t in self._pending_timers if t.is_alive()]
            self._pending_timers.append(timer)
        timer.start()

    def _check_hello_timeout(self, neighbor: Neighbor, seq: int) -> None:
        with self._lock:
            state = self._hello_state.get(neighbor.node_id)
            already_acked = state is not None and state["last_acked_seq"] >= seq
        if not already_acked and not self._stop_event.is_set():
            self.handle_hello_timeout(neighbor)

    def handle_echo(self, packet: Packet, neighbor: Neighbor) -> None:
        """Procesa un ``echo`` válido: actualiza RTT y reanima al vecino si aplica.

        ``ForwardingService`` es quien resuelve el objeto ``Neighbor`` a partir
        de ``packet.from`` y llama a este método.
        """
        if packet.type != "echo":
            return

        seq = packet.payload.get("seq")
        sent_at = packet.payload.get("sent_at")
        if not isinstance(seq, int):
            return

        with self._lock:
            state = self._hello_state.setdefault(
                neighbor.node_id, {"last_sent_seq": 0, "last_acked_seq": 0}
            )
            if seq <= state["last_acked_seq"]:
                # echo viejo o fuera de contexto (share.md §10): se ignora.
                return
            state["last_acked_seq"] = seq

        was_up = neighbor.is_up
        neighbor.record_echo(sent_at)

        if not was_up and neighbor.is_up and self.mode == "lsr":
            logger.info("[%s] vecino %s recuperado", self.self_id, neighbor.node_id)
            self.announce_lsp()

    def handle_hello_timeout(self, neighbor: Neighbor) -> None:
        """Registra un timeout de ``hello`` y reanuncia el LSP si el vecino cae."""
        became_down = neighbor.record_hello_timeout()
        if became_down:
            logger.warning("[%s] vecino %s marcado como caído", self.self_id, neighbor.node_id)
            if self.mode == "lsr":
                self.announce_lsp()

    # ------------------------------------------------------------------
    # LSR: anuncio de LSP propio y manejo de LSP recibidos
    # ------------------------------------------------------------------
    def announce_lsp(self) -> Optional[Packet]:
        """Construye, aplica localmente y difunde el LSP propio (solo modo lsr).

        Retorna el ``Packet`` enviado (útil para pruebas) o ``None`` si el
        modo no es ``lsr``.
        """
        if self.mode != "lsr":
            return None

        with self._lock:
            self._lsp_seq += 1
            seq = self._lsp_seq

        lsp = self.lsr_router.build_lsp(self.self_id, seq, self.neighbors)
        self._recompute_lsr_routes()

        packet = Packet.make_info(
            proto=self.mode,
            from_node=self.self_id,
            origin=self.self_id,
            seq=lsp["seq"],
            neighbors=lsp["neighbors"],
            ttl=self.initial_ttl,
        )

        targets = self.flooding_router.get_flood_targets(self.neighbors, sender_id=None)
        for target in targets:
            self._safe_send(target, packet)

        logger.info(
            "[%s] LSP propio anunciado (seq=%d, vecinos activos=%s)",
            self.self_id,
            seq,
            sorted(lsp["neighbors"]),
        )
        return packet

    def handle_info(self, packet: Packet) -> bool:
        """Aplica un LSP recibido a la LSDB y recalcula rutas si es nuevo.

        Retorna ``True`` si el LSP era nuevo (y por lo tanto debe
        reflood-earse), ``False`` si era viejo/repetido.
        """
        if self.mode != "lsr" or packet.type != "info":
            return False

        is_new = self.lsr_router.update_from_packet(packet)
        if is_new:
            self._recompute_lsr_routes()
            logger.info(
                "[%s] LSDB actualizada con LSP de %s (seq=%d)",
                self.self_id,
                packet.payload["origin"],
                packet.payload["seq"],
            )
        return is_new

    def _recompute_lsr_routes(self) -> None:
        if self.lsr_router is not None:
            self.lsr_router.compute_routes(self.self_id)

    # ------------------------------------------------------------------
    # Consulta de rutas (usado por ForwardingService)
    # ------------------------------------------------------------------
    def get_next_hop(self, destination: str) -> Optional[str]:
        """Retorna el ``node_id`` del siguiente salto hacia ``destination``.

        En modo ``flooding`` no existe next-hop (retorna ``None`` siempre);
        el forwarding en ese modo debe usar ``flooding_router.get_flood_targets``.
        """
        if self.mode == "dijkstra":
            return dijkstra_next_hop(self.dijkstra_table, destination)
        if self.mode == "lsr":
            return self.lsr_router.get_next_hop(destination)
        return None

    def get_routing_table(self) -> dict[str, dict[str, Any]]:
        """Snapshot de la tabla de ruteo actual, para diagnóstico/CLI."""
        if self.mode == "dijkstra":
            return dict(self.dijkstra_table)
        if self.mode == "lsr":
            return dict(self.lsr_router.routing_table)
        return {}

    # ------------------------------------------------------------------
    # Envío interno
    # ------------------------------------------------------------------
    def _safe_send(self, node_id: str, packet: Packet) -> None:
        try:
            self.send_callback(node_id, packet)
        except Exception:  # noqa: BLE001 - un fallo de envío no debe tumbar el nodo
            logger.exception("[%s] error enviando paquete a %s", self.self_id, node_id)

    def __repr__(self) -> str:
        return f"RoutingService(self_id={self.self_id}, mode={self.mode})"
