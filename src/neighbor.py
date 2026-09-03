"""Estado y health-check de un vecino directo."""

from __future__ import annotations

import math
import threading
import time
from typing import Any


class Neighbor:
    """Representa un vecino configurado y su estado de disponibilidad."""

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        cost: int | float,
        max_failures: int = 3,
    ) -> None:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id debe ser un string no vacío")
        if not isinstance(host, str) or not host.strip():
            raise ValueError("host debe ser un string no vacío")
        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
            raise ValueError("port debe ser un entero entre 1 y 65535")
        if (
            not isinstance(cost, (int, float))
            or isinstance(cost, bool)
            or not math.isfinite(cost)
            or cost < 0
        ):
            raise ValueError("cost debe ser un número finito no negativo")
        if (
            not isinstance(max_failures, int)
            or isinstance(max_failures, bool)
            or max_failures < 1
        ):
            raise ValueError("max_failures debe ser un entero positivo")

        self.node_id = node_id.strip()
        self.host = host.strip()
        self.port = port
        self.cost = float(cost)
        self.max_failures = max_failures

        self.is_up = True
        self.consecutive_failures = 0
        self.last_rtt_sec = -1.0
        self.last_seen = -1.0

        self._hello_seq = 0
        self._lock = threading.RLock()

    def next_hello_seq(self) -> int:
        """Incrementa y retorna la secuencia monotónica para este vecino."""
        with self._lock:
            self._hello_seq += 1
            return self._hello_seq

    @property
    def current_seq(self) -> int:
        """Retorna la última secuencia de ``hello`` emitida."""
        with self._lock:
            return self._hello_seq

    def update_rtt(self, sent_at: int | float) -> float:
        """Calcula y guarda ``now - sent_at`` para un ``echo`` válido."""
        if (
            not isinstance(sent_at, (int, float))
            or isinstance(sent_at, bool)
            or not math.isfinite(sent_at)
            or sent_at < 0
        ):
            raise ValueError("sent_at debe ser un timestamp válido")

        with self._lock:
            rtt = time.time() - float(sent_at)
            self.last_rtt_sec = rtt
            return rtt

    def record_echo(self, sent_at: int | float) -> float:
        """Registra un ``echo``, reinicia fallos y recupera al vecino."""
        with self._lock:
            rtt = self.update_rtt(sent_at)
            self.last_seen = time.time()
            self.consecutive_failures = 0
            self.is_up = True
            return rtt

    def record_hello_timeout(self) -> bool:
        """Registra un timeout y retorna si el vecino acaba de caer."""
        with self._lock:
            self.consecutive_failures += 1
            if self.is_up and self.consecutive_failures >= self.max_failures:
                self.is_up = False
                return True
            return False

    def mark_up(self) -> None:
        """Fuerza el estado activo y reinicia los fallos consecutivos."""
        with self._lock:
            self.is_up = True
            self.consecutive_failures = 0

    def mark_down(self) -> None:
        """Fuerza el estado inactivo."""
        with self._lock:
            self.is_up = False

    def address(self) -> tuple[str, int]:
        """Retorna la dirección utilizable por un socket TCP."""
        return self.host, self.port

    def to_dict(self) -> dict[str, Any]:
        """Retorna una instantánea serializable del estado."""
        with self._lock:
            return {
                "node_id": self.node_id,
                "host": self.host,
                "port": self.port,
                "cost": self.cost,
                "is_up": self.is_up,
                "consecutive_failures": self.consecutive_failures,
                "last_rtt_sec": (
                    round(self.last_rtt_sec, 6) if self.last_rtt_sec >= 0 else None
                ),
                "last_seen": self.last_seen if self.last_seen >= 0 else None,
            }

    def __repr__(self) -> str:
        status = "UP" if self.is_up else "DOWN"
        rtt = "N/A" if self.last_rtt_sec < 0 else f"{self.last_rtt_sec:.4f}s"
        return (
            f"Neighbor(id={self.node_id}, {self.host}:{self.port}, "
            f"cost={self.cost:g}, status={status}, rtt={rtt})"
        )
