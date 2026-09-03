"""Difusión por inundación con deduplicación por identificador de paquete."""

from __future__ import annotations

import math
import threading
import time
from typing import Any, Iterable, Mapping


DEFAULT_CACHE_TTL_SEC = 60.0


class FloodingRouter:
    """Decide qué paquetes vale la pena reenviar y hacia qué vecinos.

    El nodo no conoce una topología global: solo recuerda los ``id`` que ya
    procesó para cortar los ciclos y consulta el estado de sus vecinos directos
    para elegir los destinos de cada difusión.
    """

    def __init__(self, cache_ttl_sec: float = DEFAULT_CACHE_TTL_SEC) -> None:
        if (
            not isinstance(cache_ttl_sec, (int, float))
            or isinstance(cache_ttl_sec, bool)
            or not math.isfinite(cache_ttl_sec)
            or cache_ttl_sec <= 0
        ):
            raise ValueError("cache_ttl_sec debe ser un número positivo")

        self.cache_ttl_sec = float(cache_ttl_sec)
        self.seen_ids: dict[str, float] = {}

        self._last_sweep = time.time()
        self._lock = threading.RLock()

    def should_forward(self, packet_id: str) -> bool:
        """Registra el ``id`` y retorna ``False`` si el paquete ya fue procesado."""
        if not isinstance(packet_id, str) or not packet_id.strip():
            raise ValueError("packet_id debe ser un string no vacío")

        with self._lock:
            self._sweep_if_due()
            if packet_id in self.seen_ids:
                return False
            self.seen_ids[packet_id] = time.time()
            return True

    def has_seen(self, packet_id: str) -> bool:
        """Consulta la caché sin registrar el ``id``."""
        with self._lock:
            return packet_id in self.seen_ids

    def expire_cache(self, ttl_sec: float | None = None) -> int:
        """Descarta las entradas más viejas que el TTL y retorna cuántas eliminó."""
        ttl = self.cache_ttl_sec if ttl_sec is None else float(ttl_sec)
        if not math.isfinite(ttl) or ttl <= 0:
            raise ValueError("ttl_sec debe ser un número positivo")

        with self._lock:
            deadline = time.time() - ttl
            expired = [
                packet_id
                for packet_id, seen_at in self.seen_ids.items()
                if seen_at < deadline
            ]
            for packet_id in expired:
                del self.seen_ids[packet_id]
            self._last_sweep = time.time()
            return len(expired)

    def clear_cache(self) -> None:
        """Vacía la caché de deduplicación."""
        with self._lock:
            self.seen_ids.clear()
            self._last_sweep = time.time()

    @property
    def cache_size(self) -> int:
        """Cantidad de identificadores recordados en este momento."""
        with self._lock:
            return len(self.seen_ids)

    def get_flood_targets(
        self,
        neighbors: Mapping[str, Any] | Iterable[Any],
        sender_id: str | None = None,
    ) -> list[str]:
        """Retorna los vecinos activos a los que difundir, sin incluir al emisor.

        Acepta el diccionario ``node_id -> Neighbor`` que arma el cargador de
        configuración o cualquier iterable de vecinos.
        """
        targets = [
            neighbor.node_id
            for neighbor in _iter_neighbors(neighbors)
            if neighbor.is_up and neighbor.node_id != sender_id
        ]
        return sorted(set(targets))

    def _sweep_if_due(self) -> None:
        """Limpia la caché cuando pasó al menos un TTL desde la última pasada."""
        if time.time() - self._last_sweep >= self.cache_ttl_sec:
            self.expire_cache()

    def __repr__(self) -> str:
        return (
            f"FloodingRouter(cache={self.cache_size} ids, "
            f"ttl={self.cache_ttl_sec:g}s)"
        )


def _iter_neighbors(neighbors: Mapping[str, Any] | Iterable[Any]) -> Iterable[Any]:
    """Normaliza el contenedor de vecinos a una secuencia de objetos ``Neighbor``."""
    if isinstance(neighbors, Mapping):
        return neighbors.values()
    if isinstance(neighbors, (str, bytes)):
        raise TypeError("neighbors debe ser un mapa o una colección de vecinos")
    return neighbors
