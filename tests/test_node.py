"""Pruebas del ensamblaje TCP real implementado por ``Node``."""

from __future__ import annotations

import json
import socket
import threading
import time
from pathlib import Path

from src.node import Node
from src.packet import Packet


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _write_config(
    directory: Path,
    node_id: str,
    port: int,
    neighbors: list[dict[str, object]],
) -> Path:
    path = directory / f"node_{node_id}.json"
    path.write_text(
        json.dumps(
            {
                "node_id": node_id,
                "listen": {"host": "127.0.0.1", "port": port},
                "mode": "flooding",
                "neighbors": neighbors,
                "params": {
                    "initial_ttl": 5,
                    "hello_interval_sec": 0.1,
                    "hello_timeout_sec": 0.05,
                    "hello_max_failures": 3,
                    "dedup_cache_ttl_sec": 5,
                    "log_level": "WARNING",
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def _wait_for(predicate, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


def test_nodes_connect_after_out_of_order_start_and_exchange_message(tmp_path):
    port_a, port_b = _free_port(), _free_port()
    path_a = _write_config(
        tmp_path,
        "A",
        port_a,
        [{"node_id": "B", "host": "127.0.0.1", "port": port_b, "cost": 1}],
    )
    path_b = _write_config(
        tmp_path,
        "B",
        port_b,
        [{"node_id": "A", "host": "127.0.0.1", "port": port_a, "cost": 1}],
    )

    node_a, node_b = Node(path_a), Node(path_b)
    delivered = threading.Event()
    received: list[str] = []

    def record_delivery(packet: Packet) -> None:
        received.append(packet.payload)
        delivered.set()

    node_b.forwarding._deliver_message = record_delivery
    try:
        # A inicia cuando B aún no escucha; el conector debe reintentar.
        node_a.start(interactive=False)
        node_b.start(interactive=False)

        assert _wait_for(
            lambda: "B" in node_a.connections and "A" in node_b.connections
        )
        node_a.forwarding.send_user_message("B", "hola por TCP")

        assert delivered.wait(2.0)
        assert received == ["hola por TCP"]
    finally:
        node_a.stop()
        node_b.stop()

    assert node_a.connections == {}
    assert node_b.connections == {}


def test_invalid_ndjson_line_does_not_close_incoming_connection(tmp_path):
    port = _free_port()
    path = _write_config(tmp_path, "B", port, [])
    node = Node(path)
    delivered = threading.Event()
    received: list[str] = []

    def record_delivery(packet: Packet) -> None:
        received.append(packet.payload)
        delivered.set()

    node.forwarding._deliver_message = record_delivery
    try:
        node.start(interactive=False)
        packet = Packet.make_message(
            proto="flooding", from_node="A", to="B", text="línea válida", ttl=5
        )
        with socket.create_connection(("127.0.0.1", port), timeout=1.0) as client:
            client.sendall(b"{json-invalido}\n")
            client.sendall(packet.to_json().encode("utf-8"))
            assert delivered.wait(2.0)

        assert received == ["línea válida"]
    finally:
        node.stop()

