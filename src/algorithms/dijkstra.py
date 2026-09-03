"""Cálculo de rutas de costo mínimo sobre una topología conocida."""

from __future__ import annotations

import heapq
import math
from typing import Any


def compute_routing_table(
    source: str,
    topology: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Retorna la tabla de ruteo de ``source`` como ``destino -> {next_hop, cost}``.

    La topología se interpreta como un grafo dirigido, de modo que un enlace
    anunciado en un solo sentido sigue siendo utilizable. Los destinos sin ruta
    quedan fuera de la tabla y el propio ``source`` nunca aparece en ella.
    """
    if not isinstance(source, str) or not source.strip():
        raise ValueError("source debe ser un string no vacío")
    _validate_topology(topology)

    source = source.strip()
    if source not in topology:
        return {}

    costs: dict[str, float] = {source: 0.0}
    next_hops: dict[str, str] = {}
    visited: set[str] = set()
    queue: list[tuple[float, str]] = [(0.0, source)]

    while queue:
        cost, node = heapq.heappop(queue)
        if node in visited:
            continue
        visited.add(node)

        for neighbor, link_cost in topology.get(node, {}).items():
            if neighbor in visited or neighbor == source:
                continue
            candidate_cost = cost + float(link_cost)
            candidate_hop = neighbor if node == source else next_hops[node]
            known_cost = costs.get(neighbor, math.inf)
            # Ante empate se conserva el next hop menor para que la tabla sea
            # reproducible entre nodos que comparten la misma topología.
            if candidate_cost < known_cost or (
                candidate_cost == known_cost
                and candidate_hop < next_hops[neighbor]
            ):
                costs[neighbor] = candidate_cost
                next_hops[neighbor] = candidate_hop
                heapq.heappush(queue, (candidate_cost, neighbor))

    return {
        destination: {"next_hop": hop, "cost": costs[destination]}
        for destination, hop in sorted(next_hops.items())
    }


def get_next_hop(table: dict[str, dict[str, Any]], destination: str) -> str | None:
    """Consulta la tabla y retorna el vecino de salida, o ``None`` si no hay ruta."""
    entry = table.get(destination)
    return entry["next_hop"] if entry else None


def format_table(source: str, table: dict[str, dict[str, Any]]) -> str:
    """Arma una vista legible de la tabla para diagnóstico por consola."""
    lines = [f"Tabla de ruteo de {source}", f"{'DESTINO':<10}{'NEXT HOP':<12}COSTO"]
    if not table:
        lines.append("(sin rutas conocidas)")
    for destination, entry in sorted(table.items()):
        lines.append(
            f"{destination:<10}{entry['next_hop']:<12}{entry['cost']:g}"
        )
    return "\n".join(lines)


def _validate_topology(topology: Any) -> None:
    """Verifica que la topología sea un mapa de costos finitos no negativos."""
    if not isinstance(topology, dict):
        raise TypeError("topology debe ser un diccionario")

    for node, adjacent in topology.items():
        if not isinstance(node, str) or not node.strip():
            raise ValueError("cada nodo de la topología debe tener un ID válido")
        if not isinstance(adjacent, dict):
            raise TypeError(f"los vecinos de {node!r} deben ser un diccionario")

        for neighbor, cost in adjacent.items():
            if not isinstance(neighbor, str) or not neighbor.strip():
                raise ValueError(f"ID de vecino inválido en {node!r}")
            if (
                not isinstance(cost, (int, float))
                or isinstance(cost, bool)
                or not math.isfinite(cost)
                or cost < 0
            ):
                raise ValueError(
                    f"el costo {node}-{neighbor} debe ser un número finito no negativo"
                )
