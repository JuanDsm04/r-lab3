"""Pruebas del envelope y de los cuatro tipos del protocolo v1."""

import json
import unittest

from src.packet import PROTOCOL_VERSION, Packet


class PacketTests(unittest.TestCase):
    def test_message_round_trip_uses_compact_ndjson(self) -> None:
        packet = Packet.make_message("flooding", "A", "C", "¡Hola red!", ttl=5)
        raw = packet.to_json()
        restored = Packet.from_json(raw)

        self.assertTrue(raw.endswith("\n"))
        self.assertEqual(raw.count("\n"), 1)
        self.assertNotIn(": ", raw)
        self.assertIsNotNone(restored)
        self.assertEqual(restored.to_dict(), packet.to_dict())
        self.assertEqual(restored.payload, "¡Hola red!")

    def test_generated_ids_are_unique(self) -> None:
        first = Packet.make_message("flooding", "A", "B", "uno", ttl=5)
        second = Packet.make_message("flooding", "A", "B", "dos", ttl=5)
        self.assertNotEqual(first.id, second.id)

    def test_optional_fields_receive_protocol_defaults(self) -> None:
        raw = json.dumps(
            {
                "id": "packet-1",
                "proto": "flooding",
                "type": "message",
                "from": "A",
                "to": "B",
                "ttl": 3,
                "payload": "hola",
            }
        )
        packet = Packet.from_json(raw)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.version, PROTOCOL_VERSION)
        self.assertEqual(packet.headers, [])

    def test_ttl_can_be_decremented_and_expire(self) -> None:
        packet = Packet.make_message("flooding", "A", "B", "hola", ttl=1)
        result = packet.decrement_ttl()
        self.assertIs(result, packet)
        self.assertEqual(packet.ttl, 0)
        self.assertTrue(packet.is_expired)

    def test_sender_and_hop_trace_change_when_forwarding(self) -> None:
        packet = Packet.make_message("lsr", "A", "C", "hola", ttl=5)
        packet.update_sender("B")
        packet.add_hop("B")
        self.assertEqual(packet.from_node, "B")
        self.assertEqual(packet.get_hops(), ["A", "B"])

    def test_hello_and_echo_share_sequence_and_sent_time(self) -> None:
        hello = Packet.make_hello("lsr", "A", "B", seq=7)
        echo = Packet.make_echo("lsr", "B", "A", hello)
        self.assertEqual(hello.ttl, 1)
        self.assertEqual(echo.ttl, 1)
        self.assertEqual(echo.payload["seq"], hello.payload["seq"])
        self.assertEqual(echo.payload["sent_at"], hello.payload["sent_at"])
        self.assertGreaterEqual(echo.payload["echoed_at"], echo.payload["sent_at"])

    def test_info_uses_broadcast_and_canonical_payload(self) -> None:
        packet = Packet.make_info(
            "lsr", "A", origin="A", seq=3, neighbors={"B": 4, "C": 1}, ttl=10
        )
        self.assertEqual(packet.to, "*")
        self.assertTrue(packet.is_broadcast)
        self.assertEqual(
            packet.payload,
            {"origin": "A", "seq": 3, "neighbors": {"B": 4, "C": 1}},
        )

    def test_invalid_packets_are_discarded(self) -> None:
        invalid_packets = [
            "no es json",
            "[]",
            "null",
            '{"id":"incompleto"}',
            json.dumps(
                {
                    "id": "x", "proto": "desconocido", "type": "message",
                    "from": "A", "to": "B", "ttl": 5, "payload": "hola",
                }
            ),
            json.dumps(
                {
                    "id": "x", "proto": "flooding", "type": "message",
                    "from": "A", "to": "B", "ttl": True, "payload": "hola",
                }
            ),
            json.dumps(
                {
                    "id": "x", "proto": "flooding", "type": "message",
                    "from": "A", "to": "B", "ttl": 5, "headers": {},
                    "payload": "hola",
                }
            ),
            json.dumps(
                {
                    "id": "x", "proto": "flooding", "type": "message",
                    "from": "A", "to": "B", "ttl": 5,
                    "payload": {"text": "no debe ser objeto"},
                }
            ),
            json.dumps(
                {
                    "id": "x", "proto": "lsr", "type": "info", "from": "A",
                    "to": "B", "ttl": 5,
                    "payload": {"origin": "A", "seq": 1, "neighbors": {}},
                }
            ),
        ]
        for raw in invalid_packets:
            with self.subTest(raw=raw):
                self.assertIsNone(Packet.from_json(raw))

    def test_invalid_utf8_is_discarded(self) -> None:
        self.assertIsNone(Packet.from_json(b"\xff\xfe"))

    def test_constructor_rejects_wrong_payload_shape(self) -> None:
        with self.assertRaises(TypeError):
            Packet("flooding", "message", "A", "B", 5, {"text": "hola"})


if __name__ == "__main__":
    unittest.main()
