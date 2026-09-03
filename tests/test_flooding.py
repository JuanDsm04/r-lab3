"""Pruebas de la deduplicación y de la selección de destinos en flooding."""

import unittest

from src.algorithms.flooding import FloodingRouter
from src.neighbor import Neighbor
from src.packet import Packet


def build_neighbors(*specs: tuple[str, int]) -> dict[str, Neighbor]:
    """Arma el diccionario ``node_id -> Neighbor`` que entrega la configuración."""
    return {
        node_id: Neighbor(node_id, "127.0.0.1", port, cost=1)
        for node_id, port in specs
    }


class FloodingDedupeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = FloodingRouter(cache_ttl_sec=60)

    def test_first_sighting_forwards_and_the_repeat_does_not(self) -> None:
        self.assertTrue(self.router.should_forward("packet-1"))
        self.assertFalse(self.router.should_forward("packet-1"))
        self.assertFalse(self.router.should_forward("packet-1"))

    def test_distinct_packets_are_independent(self) -> None:
        self.assertTrue(self.router.should_forward("packet-1"))
        self.assertTrue(self.router.should_forward("packet-2"))
        self.assertEqual(self.router.cache_size, 2)

    def test_has_seen_does_not_register_the_id(self) -> None:
        self.assertFalse(self.router.has_seen("packet-1"))
        self.assertEqual(self.router.cache_size, 0)
        self.assertTrue(self.router.should_forward("packet-1"))
        self.assertTrue(self.router.has_seen("packet-1"))

    def test_expired_entries_leave_the_cache(self) -> None:
        self.router.should_forward("packet-1")
        self.assertEqual(self.router.expire_cache(ttl_sec=0.0001), 0)

        self.router.seen_ids["packet-1"] -= 10
        self.assertEqual(self.router.expire_cache(ttl_sec=5), 1)
        self.assertEqual(self.router.cache_size, 0)

    def test_a_packet_forwards_again_once_its_entry_expired(self) -> None:
        self.router.should_forward("packet-1")
        self.router.seen_ids["packet-1"] -= 120
        self.router.expire_cache()

        self.assertTrue(self.router.should_forward("packet-1"))

    def test_cache_sweeps_itself_when_the_ttl_elapsed(self) -> None:
        router = FloodingRouter(cache_ttl_sec=0.01)
        router.should_forward("packet-1")
        router.seen_ids["packet-1"] -= 10
        router._last_sweep -= 10

        self.assertTrue(router.should_forward("packet-2"))
        self.assertNotIn("packet-1", router.seen_ids)

    def test_invalid_arguments_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.router.should_forward("")
        with self.assertRaises(ValueError):
            self.router.expire_cache(ttl_sec=0)
        with self.assertRaises(ValueError):
            FloodingRouter(cache_ttl_sec=-1)


class FloodingTargetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = FloodingRouter()
        self.neighbors = build_neighbors(("B", 5001), ("C", 5002), ("D", 5003))

    def test_the_sender_never_receives_the_packet_back(self) -> None:
        targets = self.router.get_flood_targets(self.neighbors, "B")
        self.assertEqual(targets, ["C", "D"])

    def test_a_locally_originated_packet_reaches_every_neighbor(self) -> None:
        targets = self.router.get_flood_targets(self.neighbors, None)
        self.assertEqual(targets, ["B", "C", "D"])

    def test_downed_neighbors_are_skipped(self) -> None:
        self.neighbors["C"].mark_down()
        self.assertEqual(self.router.get_flood_targets(self.neighbors, "B"), ["D"])

    def test_a_recovered_neighbor_returns_to_the_targets(self) -> None:
        self.neighbors["C"].mark_down()
        self.neighbors["C"].mark_up()
        self.assertEqual(self.router.get_flood_targets(self.neighbors, "B"), ["C", "D"])

    def test_a_list_of_neighbors_is_also_accepted(self) -> None:
        targets = self.router.get_flood_targets(list(self.neighbors.values()), "D")
        self.assertEqual(targets, ["B", "C"])

    def test_a_node_without_active_neighbors_floods_nothing(self) -> None:
        for neighbor in self.neighbors.values():
            neighbor.mark_down()
        self.assertEqual(self.router.get_flood_targets(self.neighbors, None), [])


class FloodingWithPacketsTests(unittest.TestCase):
    def test_a_diamond_topology_does_not_loop(self) -> None:
        """B y C difunden la misma copia hacia D, que solo la procesa una vez."""
        packet = Packet.make_message("flooding", "A", "D", "hola", ttl=5)
        routers = {node: FloodingRouter() for node in ("B", "C", "D")}

        self.assertTrue(routers["B"].should_forward(packet.id))
        self.assertTrue(routers["C"].should_forward(packet.id))
        self.assertTrue(routers["D"].should_forward(packet.id))
        self.assertFalse(routers["D"].should_forward(packet.id))

    def test_the_id_survives_the_hops_that_rewrite_the_sender(self) -> None:
        packet = Packet.make_message("flooding", "A", "D", "hola", ttl=5)
        original_id = packet.id
        packet.decrement_ttl().update_sender("B")
        packet.add_hop("B")

        self.assertEqual(packet.id, original_id)
        self.assertEqual(packet.get_hops(), ["A", "B"])


if __name__ == "__main__":
    unittest.main()
