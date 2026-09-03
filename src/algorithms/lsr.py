"""Link State Routing: base de datos de enlaces y derivación de la topología."""

from __future__ import annotations

import math
import threading
from typing import Any, Iterable, Mapping

from src.algorithms.dijkstra import compute_routing_table


class LSRRouter:
    """Mantiene la LSDB y deriva de ella las rutas del nodo.

    Cada entrada de la LSDB guarda el último LSP conocido de un origen con su
    secuencia. La topología se reconstruye uniendo esas entradas y las rutas se
    resuelven con Dijkstra sobre el grafo resultante.
    """

    def __init__(self) -> None:
        self.lsdb: dict[str, dict[str, Any]] = {}
        self.routing_table: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def update_lsdb(
        self,
        origin: str,
        seq: int,
        neighbors: Mapping[str, Any],
    ) -> bool:
        """Aplica un LSP y retorna ``True`` solo si aporta información nueva.

        Un LSP con secuencia menor o igual a la conocida para ese origen se
        descarta sin modificar la LSDB, que es lo que corta el reflooding.
        """
        origin = _require_node_id(origin, "origin")
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            raise ValueError("seq debe ser un entero no negativo")
        links = _normalize_links(neighbors)

        with self._lock:
            known = self.lsdb.get(origin)
            if known is not None and seq <= known["seq"]:
                return False
            self.lsdb[origin] = {"seq": seq, "neighbors": links}
            return True

    def update_from_packet(self, packet: Any) -> bool:
        """Aplica el LSP que transporta un paquete ``info`` ya validado."""
        if getattr(packet, "type", None) != "info":
            raise ValueError("solo un paquete info transporta un LSP")
        payload = packet.payload
        return self.update_lsdb(
            payload["origin"], payload["seq"], payload["neighbors"]
        )

    def build_topology(self) -> dict[str, dict[str, float]]:
        """Reconstruye el grafo conocido a partir de los LSP almacenados.

        Los nodos anunciados por un vecino pero que todavía no publicaron su
        propio LSP aparecen sin enlaces salientes, de modo que siguen siendo
        destinos alcanzables mientras la red converge.
        """
        with self._lock:
            topology: dict[str, dict[str, float]] = {
                origin: dict(entry["neighbors"]) for origin, entry in self.lsdb.items()
            }

        for adjacent in list(topology.values()):
            for node in adjacent:
                topology.setdefault(node, {})
        return topology

    def compute_routes(self, self_id: str) -> dict[str, dict[str, Any]]:
        """Recalcula y guarda la tabla de ruteo del nodo sobre la LSDB actual."""
        self_id = _require_node_id(self_id, "self_id")
        table = compute_routing_table(self_id, self.build_topology())
        with self._lock:
            self.routing_table = table
        return table

    def get_next_hop(self, destination: str) -> str | None:
        """Retorna el vecino de salida hacia ``destination``, o ``None`` si no hay ruta."""
        with self._lock:
            entry = self.routing_table.get(destination)
        return entry["next_hop"] if entry else None

    def build_lsp(
        self,
        self_id: str,
        seq: int,
        active_neighbors: Mapping[str, Any] | Iterable[Any],
    ) -> dict[str, Any]:
        """Arma el payload del LSP propio y lo aplica a la LSDB local.

        Solo se anuncian los vecinos activos, por lo que un vecino caído
        desaparece del LSP y deja de ser considerado en las rutas de los demás.
        """
        self_id = _require_node_id(self_id, "self_id")
        links = _active_links(active_neighbors)
        self.update_lsdb(self_id, seq, links)
        return {"origin": self_id, "seq": seq, "neighbors": links}

    def get_sequence(self, origin: str) -> int | None:
        """Retorna la última secuencia conocida de un origen."""
        with self._lock:
            entry = self.lsdb.get(origin)
        return entry["seq"] if entry else None

    @property
    def known_nodes(self) -> list[str]:
        """Nodos que ya publicaron un LSP, en orden alfabético."""
        with self._lock:
            return sorted(self.lsdb)

    def __repr__(self) -> str:
        return (
            f"LSRRouter(lsdb={len(self.lsdb)} orígenes, "
            f"rutas={len(self.routing_table)})"
        )


def _require_node_id(value: Any, label: str) -> str:
    """Valida un identificador de nodo y lo retorna sin espacios sobrantes."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} debe ser un string no vacío")
    return value.strip()


def _normalize_links(neighbors: Mapping[str, Any]) -> dict[str, float]:
    """Valida un mapa ``vecino -> costo`` y lo copia con costos flotantes."""
    if not isinstance(neighbors, Mapping):
        raise TypeError("neighbors debe ser un mapa de vecino a costo")

    links: dict[str, float] = {}
    for node_id, cost in neighbors.items():
        node_id = _require_node_id(node_id, "el ID de un vecino")
        if (
            not isinstance(cost, (int, float))
            or isinstance(cost, bool)
            or not math.isfinite(cost)
            or cost < 0
        ):
            raise ValueError(f"el costo hacia {node_id} debe ser un número no negativo")
        links[node_id] = float(cost)
    return links


def _active_links(
    neighbors: Mapping[str, Any] | Iterable[Any],
) -> dict[str, float]:
    """Extrae ``vecino -> costo`` de los vecinos activos de cualquier contenedor.

    Acepta el diccionario ``node_id -> Neighbor`` del cargador de configuración,
    una colección de vecinos, o un mapa de costos ya resuelto.
    """
    if isinstance(neighbors, Mapping):
        values = list(neighbors.values())
        if all(isinstance(value, (int, float)) for value in values):
            return _normalize_links(neighbors)
        candidates: Iterable[Any] = values
    elif isinstance(neighbors, (str, bytes)):
        raise TypeError("active_neighbors debe ser un mapa o una colección de vecinos")
    else:
        candidates = neighbors

    return _normalize_links(
        {
            neighbor.node_id: neighbor.cost
            for neighbor in candidates
            if neighbor.is_up
        }
    )
