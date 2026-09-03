"""Pruebas del cálculo de rutas mínimas usado por Dijkstra y LSR."""

import unittest

from src.algorithms.dijkstra import compute_routing_table, get_next_hop


def build_undirected(edges: dict[tuple[str, str], float]) -> dict[str, dict[str, float]]:
    """Expande una lista de aristas a la topología dirigida equivalente."""
    topology: dict[str, dict[str, float]] = {}
    for (left, right), cost in edges.items():
        topology.setdefault(left, {})[right] = cost
        topology.setdefault(right, {})[left] = cost
    return topology


# Topología del mapa de conexiones del enunciado, usada como caso de referencia.
LAB_TOPOLOGY = build_undirected(
    {
        ("A", "B"): 7,
        ("A", "I"): 1,
        ("A", "C"): 7,
        ("B", "F"): 2,
        ("I", "D"): 6,
        ("C", "D"): 5,
        ("F", "D"): 1,
        ("F", "H"): 4,
        ("F", "G"): 3,
        ("D", "E"): 1,
        ("E", "G"): 4,
    }
)

TRIANGLE = build_undirected({("A", "B"): 4, ("A", "C"): 1, ("B", "C"): 2})


class DijkstraTests(unittest.TestCase):
    def test_prefers_the_cheapest_path_over_the_direct_link(self) -> None:
        table = compute_routing_table("A", TRIANGLE)

        self.assertEqual(table["B"], {"next_hop": "C", "cost": 3.0})
        self.assertEqual(table["C"], {"next_hop": "C", "cost": 1.0})

    def test_source_is_never_its_own_destination(self) -> None:
        table = compute_routing_table("A", TRIANGLE)
        self.assertNotIn("A", table)

    def test_lab_topology_routes_every_node(self) -> None:
        table = compute_routing_table("A", LAB_TOPOLOGY)

        self.assertEqual(sorted(table), ["B", "C", "D", "E", "F", "G", "H", "I"])
        self.assertEqual(table["I"], {"next_hop": "I", "cost": 1.0})
        self.assertEqual(table["D"], {"next_hop": "I", "cost": 7.0})
        self.assertEqual(table["F"], {"next_hop": "I", "cost": 8.0})
        self.assertEqual(table["G"], {"next_hop": "I", "cost": 11.0})
        self.assertEqual(table["B"], {"next_hop": "B", "cost": 7.0})

    def test_every_node_reaches_every_other_node(self) -> None:
        for source in LAB_TOPOLOGY:
            table = compute_routing_table(source, LAB_TOPOLOGY)
            expected = sorted(set(LAB_TOPOLOGY) - {source})
            self.assertEqual(sorted(table), expected)
            for entry in table.values():
                self.assertIn(entry["next_hop"], LAB_TOPOLOGY[source])

    def test_next_hop_is_always_a_direct_neighbor(self) -> None:
        table = compute_routing_table("A", LAB_TOPOLOGY)
        for entry in table.values():
            self.assertIn(entry["next_hop"], {"B", "C", "I"})

    def test_unreachable_nodes_stay_out_of_the_table(self) -> None:
        topology = build_undirected({("A", "B"): 1, ("C", "D"): 1})
        table = compute_routing_table("A", topology)

        self.assertEqual(sorted(table), ["B"])
        self.assertIsNone(get_next_hop(table, "D"))

    def test_links_announced_in_one_direction_are_usable(self) -> None:
        topology = {"A": {"B": 1}, "B": {}}
        self.assertEqual(compute_routing_table("A", topology)["B"]["cost"], 1.0)
        self.assertEqual(compute_routing_table("B", topology), {})

    def test_unknown_source_produces_an_empty_table(self) -> None:
        self.assertEqual(compute_routing_table("Z", TRIANGLE), {})

    def test_ties_resolve_to_the_lower_next_hop(self) -> None:
        topology = build_undirected({("A", "B"): 1, ("A", "C"): 1, ("B", "D"): 1, ("C", "D"): 1})
        table = compute_routing_table("A", topology)

        self.assertEqual(table["D"], {"next_hop": "B", "cost": 2.0})

    def test_isolated_node_has_no_routes(self) -> None:
        topology = {"A": {}, "B": {"A": 1}}
        self.assertEqual(compute_routing_table("A", topology), {})

    def test_invalid_topologies_are_rejected(self) -> None:
        with self.assertRaises(TypeError):
            compute_routing_table("A", ["A", "B"])
        with self.assertRaises(TypeError):
            compute_routing_table("A", {"A": 5})
        with self.assertRaises(ValueError):
            compute_routing_table("A", {"A": {"B": -1}})
        with self.assertRaises(ValueError):
            compute_routing_table("A", {"A": {"B": float("inf")}})
        with self.assertRaises(ValueError):
            compute_routing_table("", TRIANGLE)


if __name__ == "__main__":
    unittest.main()
