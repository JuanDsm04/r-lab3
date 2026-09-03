"""Ensamblaje de un nodo ejecutable sobre conexiones TCP persistentes."""

from __future__ import annotations

import logging
import socket
import threading
from pathlib import Path
from typing import Callable

from src.config_loader import load_config
from src.forwarding import ForwardingService
from src.neighbor import Neighbor
from src.packet import Packet
from src.routing import RoutingService


logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT_SEC = 1.0
_CONNECT_RETRY_SEC = 0.25
_ACCEPT_TIMEOUT_SEC = 0.5
_MAX_PACKET_BYTES = 1024 * 1024


class _PeerConnection:
    """Socket saliente con escritura atómica y notificación de fallos."""

    def __init__(
        self,
        peer_id: str,
        sock: socket.socket,
        on_failure: Callable[[str, "_PeerConnection"], None],
    ) -> None:
        self.peer_id = peer_id
        self._socket = sock
        self._on_failure = on_failure
        self._send_lock = threading.Lock()
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def sendall(self, data: bytes) -> None:
        """Evita que dos hilos entrelacen paquetes sobre el mismo TCP stream."""
        failed = False
        with self._send_lock:
            if self._closed:
                raise OSError(f"conexión hacia {self.peer_id} cerrada")
            try:
                self._socket.sendall(data)
            except OSError:
                self._close_socket()
                failed = True

        if failed:
            self._on_failure(self.peer_id, self)
            raise OSError(f"falló la conexión hacia {self.peer_id}")

    def close(self) -> None:
        with self._send_lock:
            self._close_socket()

    def _close_socket(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._socket.close()


class Node:
    """Une configuración, routing, forwarding y transporte TCP de un nodo."""

    def __init__(self, config_path: str | Path) -> None:
        self.config = load_config(config_path)
        self.self_id: str = self.config["node_id"]
        self.mode: str = self.config["mode"]
        self.neighbors: dict[str, Neighbor] = self.config["neighbors"]

        logging.basicConfig(
            level=getattr(logging, self.config["params"]["log_level"]),
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )

        # ForwardingService conserva esta misma referencia; por eso las
        # reconexiones pueden reemplazar sockets sin reconstruir servicios.
        self.connections: dict[str, _PeerConnection] = {}

        def send_callback(node_id: str, packet: Packet) -> bool:
            return self.forwarding.send_to(node_id, packet)

        self.routing = RoutingService(
            self_id=self.self_id,
            mode=self.mode,
            config=self.config,
            neighbors=self.neighbors,
            send_callback=send_callback,
        )
        self.forwarding = ForwardingService(
            self_id=self.self_id,
            mode=self.mode,
            routing_service=self.routing,
            connections=self.connections,
        )

        self._stop_event = threading.Event()
        self._state_lock = threading.RLock()
        self._listener: socket.socket | None = None
        self._accepted_sockets: set[socket.socket] = set()
        self._accept_thread: threading.Thread | None = None
        self._connector_threads: list[threading.Thread] = []
        self._reader_threads: list[threading.Thread] = []
        self._started = False
        self._routing_started = False

    # ------------------------------------------------------------------
    # Ciclo de vida y transporte
    # ------------------------------------------------------------------
    def listen_for_connections(self) -> None:
        """Abre el servidor TCP y deja el accept-loop en un hilo daemon."""
        with self._state_lock:
            if self._listener is not None:
                return

            listen = self.config["listen"]
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                server.bind((listen["host"], listen["port"]))
                server.listen()
                server.settimeout(_ACCEPT_TIMEOUT_SEC)
            except Exception:
                server.close()
                raise

            self._listener = server
            self._accept_thread = threading.Thread(
                target=self._accept_loop,
                name=f"tcp-accept-{self.self_id}",
                daemon=True,
            )
            self._accept_thread.start()

        logger.info(
            "[%s] escuchando en %s:%s",
            self.self_id,
            listen["host"],
            listen["port"],
        )

    def connect_to_neighbors(self) -> None:
        """Inicia un ciclo de conexión/reconexión para cada vecino."""
        with self._state_lock:
            if self._connector_threads:
                return
            for neighbor in self.neighbors.values():
                thread = threading.Thread(
                    target=self._connect_loop,
                    args=(neighbor,),
                    name=f"tcp-connect-{self.self_id}-{neighbor.node_id}",
                    daemon=True,
                )
                self._connector_threads.append(thread)
                thread.start()

    def start(self, interactive: bool = True) -> None:
        """Arranca el nodo y, por defecto, ejecuta el CLI en el hilo actual.

        ``interactive=False`` sirve para pruebas o para integrar el nodo en
        otro programa; en ese caso el método retorna con los servicios vivos.
        """
        with self._state_lock:
            if self._started:
                raise RuntimeError(f"el nodo {self.self_id} ya está iniciado")
            self._started = True
            self._stop_event.clear()

        try:
            self.listen_for_connections()
            self.connect_to_neighbors()
            self.forwarding.start()

            # RoutingService calcula Dijkstra o anuncia el LSP inicial según
            # el modo, además de lanzar el health-check.
            self._routing_started = True
            self.routing.start()

            logger.info("[%s] nodo iniciado en modo %s", self.self_id, self.mode)
            if interactive:
                try:
                    self.run_cli()
                finally:
                    self.stop()
        except Exception:
            self.stop()
            raise

    def stop(self) -> None:
        """Detiene servicios, cierra sockets y espera brevemente a los hilos."""
        with self._state_lock:
            if not self._started and self._listener is None:
                return
            self._stop_event.set()
            self._started = False

        if self._routing_started:
            self.routing.stop()
            self._routing_started = False

        with self._state_lock:
            listener = self._listener
            self._listener = None
            outgoing = list(self.connections.values())
            self.connections.clear()
            incoming = list(self._accepted_sockets)

        if listener is not None:
            listener.close()
        for connection in outgoing:
            connection.close()
        for incoming_socket in incoming:
            self._close_socket(incoming_socket)

        current = threading.current_thread()
        threads = [
            self._accept_thread,
            *self._connector_threads,
            *self._reader_threads,
        ]
        for thread in threads:
            if thread is not None and thread is not current and thread.is_alive():
                thread.join(timeout=1.0)

        with self._state_lock:
            self._accept_thread = None
            self._connector_threads.clear()
            self._reader_threads.clear()
            self._accepted_sockets.clear()

        logger.info("[%s] nodo detenido", self.self_id)

    def _accept_loop(self) -> None:
        while not self._stop_event.is_set():
            listener = self._listener
            if listener is None:
                return
            try:
                client, address = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self._stop_event.is_set():
                    logger.exception("[%s] error aceptando conexión", self.self_id)
                return

            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            client.settimeout(_ACCEPT_TIMEOUT_SEC)
            with self._state_lock:
                self._accepted_sockets.add(client)
                thread = threading.Thread(
                    target=self._read_connection,
                    args=(client, address),
                    name=f"tcp-read-{self.self_id}-{address[0]}:{address[1]}",
                    daemon=True,
                )
                self._reader_threads.append(thread)
            thread.start()

    def _read_connection(
        self, client: socket.socket, address: tuple[str, int]
    ) -> None:
        """Lee varias líneas NDJSON; una línea inválida no corta el stream."""
        buffer = bytearray()
        discarding_oversized_line = False
        try:
            while not self._stop_event.is_set():
                try:
                    chunk = client.recv(65536)
                except socket.timeout:
                    continue
                if not chunk:
                    break
                buffer.extend(chunk)

                while True:
                    newline = buffer.find(b"\n")
                    if newline < 0:
                        break
                    raw_line = bytes(buffer[: newline + 1])
                    del buffer[: newline + 1]

                    if discarding_oversized_line:
                        discarding_oversized_line = False
                        continue
                    if len(raw_line) > _MAX_PACKET_BYTES:
                        logger.warning(
                            "[%s] paquete demasiado grande desde %s:%s, descartado",
                            self.self_id,
                            *address,
                        )
                        continue

                    parsed = Packet.from_json(raw_line)
                    sender_node_id = parsed.from_node if parsed is not None else ""
                    self.forwarding.handle_incoming(raw_line, sender_node_id)

                if len(buffer) > _MAX_PACKET_BYTES:
                    logger.warning(
                        "[%s] paquete demasiado grande desde %s:%s, descartado",
                        self.self_id,
                        *address,
                    )
                    buffer.clear()
                    discarding_oversized_line = True
        except (OSError, ValueError):
            if not self._stop_event.is_set():
                logger.debug(
                    "[%s] conexión entrante cerrada desde %s:%s",
                    self.self_id,
                    *address,
                )
        finally:
            with self._state_lock:
                self._accepted_sockets.discard(client)
                current = threading.current_thread()
                if current in self._reader_threads:
                    self._reader_threads.remove(current)
            self._close_socket(client)

    def _connect_loop(self, neighbor: Neighbor) -> None:
        while not self._stop_event.is_set():
            with self._state_lock:
                current = self.connections.get(neighbor.node_id)
            if current is not None and not current.closed:
                self._stop_event.wait(_CONNECT_RETRY_SEC)
                continue

            try:
                raw_socket = socket.create_connection(
                    neighbor.address(), timeout=_CONNECT_TIMEOUT_SEC
                )
                raw_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                raw_socket.settimeout(None)
                connection = _PeerConnection(
                    neighbor.node_id, raw_socket, self._drop_connection
                )
                self._register_connection(neighbor.node_id, connection)
            except OSError:
                logger.debug(
                    "[%s] no se pudo conectar a %s (%s:%s); se reintentará",
                    self.self_id,
                    neighbor.node_id,
                    neighbor.host,
                    neighbor.port,
                )

            self._stop_event.wait(_CONNECT_RETRY_SEC)

    def _register_connection(
        self, node_id: str, connection: _PeerConnection
    ) -> None:
        with self._state_lock:
            if self._stop_event.is_set():
                connection.close()
                return
            previous = self.connections.get(node_id)
            self.connections[node_id] = connection

        if previous is not None and previous is not connection:
            previous.close()
        logger.info("[%s] conectado con vecino %s", self.self_id, node_id)

        # Si el enlace apareció después del anuncio de arranque, el nuevo
        # vecino también debe recibir el estado local para que LSR converja.
        if self.mode == "lsr" and self._routing_started:
            self.routing.announce_lsp()

    def _drop_connection(
        self, node_id: str, failed_connection: _PeerConnection
    ) -> None:
        with self._state_lock:
            if self.connections.get(node_id) is failed_connection:
                self.connections.pop(node_id, None)
        failed_connection.close()

    @staticmethod
    def _close_socket(sock: socket.socket) -> None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    # ------------------------------------------------------------------
    # CLI
    # ------------------------------------------------------------------
    def run_cli(self) -> None:
        """Procesa comandos hasta recibir ``quit``, EOF o Ctrl+C."""
        print(f"Nodo {self.self_id} listo (modo {self.mode}). Escriba 'help'.")
        while not self._stop_event.is_set():
            try:
                line = input(f"[{self.self_id}]> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                return

            if not line:
                continue
            command, *rest = line.split(maxsplit=1)
            command = command.lower()

            if command == "quit":
                return
            if command == "help":
                self._print_help()
            elif command == "send":
                self._cli_send(rest[0] if rest else "")
            elif command == "table":
                self._print_routing_table()
            elif command == "neighbors":
                self._print_neighbors()
            else:
                print(f"Comando desconocido: {command}. Use 'help'.")

    def _cli_send(self, arguments: str) -> None:
        parts = arguments.split(maxsplit=1)
        if len(parts) != 2 or not parts[1].strip():
            print("Uso: send <destino> <mensaje>")
            return
        self.forwarding.send_user_message(parts[0], parts[1])

    def _print_routing_table(self) -> None:
        table = self.routing.get_routing_table()
        if self.mode == "flooding":
            print("Flooding no utiliza una tabla global de rutas.")
            return
        if not table:
            print("No hay rutas conocidas.")
            return

        print(f"{'Destino':<16} {'Siguiente salto':<18} Costo")
        for destination, route in sorted(table.items()):
            print(
                f"{destination:<16} {str(route['next_hop']):<18} "
                f"{route['cost']:g}"
            )

    def _print_neighbors(self) -> None:
        if not self.neighbors:
            print("Este nodo no tiene vecinos configurados.")
            return
        print(f"{'Vecino':<12} {'Estado':<8} {'Costo':<10} RTT")
        for neighbor in sorted(self.neighbors.values(), key=lambda item: item.node_id):
            rtt = (
                "N/D"
                if neighbor.last_rtt_sec < 0
                else f"{neighbor.last_rtt_sec * 1000:.2f} ms"
            )
            status = "UP" if neighbor.is_up else "DOWN"
            print(f"{neighbor.node_id:<12} {status:<8} {neighbor.cost:<10g} {rtt}")

    @staticmethod
    def _print_help() -> None:
        print("Comandos:")
        print("  send <destino> <mensaje>  Envía un mensaje")
        print("  table                     Muestra la tabla de rutas")
        print("  neighbors                 Muestra el estado de vecinos")
        print("  quit                      Cierra el nodo")

    def __enter__(self) -> "Node":
        self.start(interactive=False)
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.stop()
