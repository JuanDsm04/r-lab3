"""Pruebas de la LSDB, la topología derivada y las rutas de Link State Routing."""

import unittest

from src.algorithms.lsr import LSRRouter
from src.neighbor import Neighbor
from src.packet import Packet


# Enlaces del mapa de conexiones del enunciado, vistos desde cada nodo.
LAB_LINKS = {
    "A": {"B": 7, "C": 7, "I": 1},
    "B": {"A": 7, "F": 2},
    "C": {"A": 7, "D": 5},
    "D": {"C": 5, "E": 1, "F": 1, "I": 6},
    "E": {"D": 1, "G": 4},
    "F": {"B": 2, "D": 1, "G": 3, "H": 4},
    "G": {"E": 4, "F": 3},
    "H": {"F": 4},
    "I": {"A": 1, "D": 6},
}


def build_neighbors(links: dict[str, int]) -> dict[str, Neighbor]:
    """Arma el diccionario ``node_id -> Neighbor`` que entrega la configuración."""
    return {
        node_id: Neighbor(node_id, "127.0.0.1", 5000 + index, cost)
        for index, (node_id, cost) in enumerate(links.items())
    }


def converged_router() -> LSRRouter:
    """Retorna un router que ya recibió el LSP de todos los nodos del mapa."""
    router = LSRRouter()
    for origin, links in LAB_LINKS.items():
        router.update_lsdb(origin, 1, links)
    return router


class LSDBTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = LSRRouter()

    def test_the_first_lsp_of_an_origin_is_new(self) -> None:
        self.assertTrue(self.router.update_lsdb("B", 1, {"A": 4}))
        self.assertEqual(self.router.lsdb["B"], {"seq": 1, "neighbors": {"A": 4.0}})

    def test_a_repeated_sequence_is_discarded(self) -> None:
        self.router.update_lsdb("B", 7, {"A": 4})
        self.assertFalse(self.router.update_lsdb("B", 7, {"A": 99}))
        self.assertEqual(self.router.lsdb["B"]["neighbors"], {"A": 4.0})

    def test_an_older_sequence_is_discarded(self) -> None:
        self.router.update_lsdb("B", 7, {"A": 4})
        self.assertFalse(self.router.update_lsdb("B", 3, {"A": 99}))
        self.assertEqual(self.router.get_sequence("B"), 7)

    def test_a_newer_sequence_replaces_the_previous_links(self) -> None:
        self.router.update_lsdb("B", 7, {"A": 4, "C": 2})
        self.assertTrue(self.router.update_lsdb("B", 8, {"A": 4}))
        self.assertEqual(self.router.lsdb["B"], {"seq": 8, "neighbors": {"A": 4.0}})

    def test_each_origin_keeps_its_own_sequence(self) -> None:
        self.router.update_lsdb("B", 9, {"A": 4})
        self.assertTrue(self.router.update_lsdb("C", 1, {"A": 1}))
        self.assertEqual(self.router.known_nodes, ["B", "C"])
        self.assertIsNone(self.router.get_sequence("Z"))

    def test_an_info_packet_feeds_the_lsdb(self) -> None:
        packet = Packet.make_info("lsr", "B", origin="B", seq=3, neighbors={"A": 4}, ttl=5)

        self.assertTrue(self.router.update_from_packet(packet))
        self.assertFalse(self.router.update_from_packet(packet))
        self.assertEqual(self.router.get_sequence("B"), 3)

    def test_only_info_packets_carry_an_lsp(self) -> None:
        hello = Packet.make_hello("lsr", "A", "B", seq=1)
        with self.assertRaises(ValueError):
            self.router.update_from_packet(hello)

    def test_invalid_lsps_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.router.update_lsdb("", 1, {})
        with self.assertRaises(ValueError):
            self.router.update_lsdb("B", -1, {})
        with self.assertRaises(ValueError):
            self.router.update_lsdb("B", 1, {"A": -4})
        with self.assertRaises(TypeError):
            self.router.update_lsdb("B", 1, ["A"])


class TopologyTests(unittest.TestCase):
    def test_the_topology_joins_every_stored_lsp(self) -> None:
        router = converged_router()
        topology = router.build_topology()

        self.assertEqual(sorted(topology), sorted(LAB_LINKS))
        self.assertEqual(topology["F"], {"B": 2.0, "D": 1.0, "G": 3.0, "H": 4.0})

    def test_announced_nodes_without_their_own_lsp_are_reachable(self) -> None:
        router = LSRRouter()
        router.update_lsdb("A", 1, {"B": 4})
        topology = router.build_topology()

        self.assertEqual(topology, {"A": {"B": 4.0}, "B": {}})

    def test_the_topology_is_a_copy_of_the_lsdb(self) -> None:
        router = LSRRouter()
        router.update_lsdb("A", 1, {"B": 4})
        router.build_topology()["A"]["B"] = 99

        self.assertEqual(router.lsdb["A"]["neighbors"], {"B": 4.0})

    def test_an_empty_lsdb_produces_an_empty_topology(self) -> None:
        self.assertEqual(LSRRouter().build_topology(), {})


class RouteTests(unittest.TestCase):
    def test_routes_match_the_shortest_paths_of_the_map(self) -> None:
        router = converged_router()
        table = router.compute_routes("A")

        self.assertEqual(sorted(table), ["B", "C", "D", "E", "F", "G", "H", "I"])
        self.assertEqual(table["D"], {"next_hop": "I", "cost": 7.0})
        self.assertEqual(table["H"], {"next_hop": "I", "cost": 12.0})
        self.assertEqual(router.get_next_hop("G"), "I")

    def test_the_table_is_stored_for_later_lookups(self) -> None:
        router = converged_router()
        self.assertIsNone(router.get_next_hop("D"))

        router.compute_routes("A")
        self.assertEqual(router.get_next_hop("D"), "I")

    def test_a_partial_lsdb_still_routes_the_known_part(self) -> None:
        router = LSRRouter()
        router.update_lsdb("A", 1, {"B": 7, "I": 1})
        router.update_lsdb("I", 1, {"A": 1, "D": 6})
        table = router.compute_routes("A")

        self.assertEqual(sorted(table), ["B", "D", "I"])
        self.assertEqual(table["D"], {"next_hop": "I", "cost": 7.0})
        self.assertIsNone(router.get_next_hop("H"))

    def test_a_lost_link_reroutes_the_traffic(self) -> None:
        """Al caer el enlace I-D, el trayecto barato pasa a ser A-B-F-D."""
        router = converged_router()
        self.assertEqual(router.compute_routes("A")["D"], {"next_hop": "I", "cost": 7.0})

        router.update_lsdb("I", 2, {"A": 1})
        router.update_lsdb("D", 2, {"C": 5, "E": 1, "F": 1})
        table = router.compute_routes("A")

        self.assertEqual(table["D"], {"next_hop": "B", "cost": 10.0})
        self.assertEqual(table["I"], {"next_hop": "I", "cost": 1.0})

    def test_an_unknown_source_has_no_routes(self) -> None:
        self.assertEqual(converged_router().compute_routes("Z"), {})


class OwnLSPTests(unittest.TestCase):
    def test_the_lsp_only_announces_active_neighbors(self) -> None:
        router = LSRRouter()
        neighbors = build_neighbors({"B": 7, "C": 7, "I": 1})
        neighbors["C"].mark_down()

        lsp = router.build_lsp("A", 4, neighbors)
        self.assertEqual(lsp, {"origin": "A", "seq": 4, "neighbors": {"B": 7.0, "I": 1.0}})

    def test_building_the_lsp_registers_it_in_the_local_lsdb(self) -> None:
        router = LSRRouter()
        router.build_lsp("A", 4, build_neighbors({"B": 7}))

        self.assertEqual(router.get_sequence("A"), 4)
        self.assertEqual(router.build_topology()["A"], {"B": 7.0})

    def test_the_node_routes_to_its_own_neighbors_right_after_announcing(self) -> None:
        router = LSRRouter()
        router.build_lsp("A", 1, build_neighbors({"B": 7, "I": 1}))
        table = router.compute_routes("A")

        self.assertEqual(table["B"], {"next_hop": "B", "cost": 7.0})
        self.assertEqual(table["I"], {"next_hop": "I", "cost": 1.0})

    def test_a_recovered_neighbor_returns_to_the_next_lsp(self) -> None:
        router = LSRRouter()
        neighbors = build_neighbors({"B": 7, "I": 1})
        neighbors["B"].mark_down()
        self.assertEqual(router.build_lsp("A", 1, neighbors)["neighbors"], {"I": 1.0})

        neighbors["B"].mark_up()
        self.assertEqual(
            router.build_lsp("A", 2, neighbors)["neighbors"], {"B": 7.0, "I": 1.0}
        )

    def test_a_node_without_active_neighbors_announces_an_empty_lsp(self) -> None:
        router = LSRRouter()
        neighbors = build_neighbors({"B": 7})
        neighbors["B"].mark_down()

        self.assertEqual(router.build_lsp("A", 1, neighbors)["neighbors"], {})

    def test_a_plain_cost_map_is_also_accepted(self) -> None:
        router = LSRRouter()
        lsp = router.build_lsp("A", 1, {"B": 7, "I": 1})

        self.assertEqual(lsp["neighbors"], {"B": 7.0, "I": 1.0})

    def test_the_lsp_payload_travels_in_an_info_packet(self) -> None:
        router = LSRRouter()
        lsp = router.build_lsp("A", 1, build_neighbors({"B": 7, "I": 1}))
        packet = Packet.make_info("lsr", "A", ttl=5, **lsp)
        restored = Packet.from_json(packet.to_json())

        self.assertEqual(restored.to, "*")
        self.assertEqual(restored.payload, lsp)


if __name__ == "__main__":
    unittest.main()
