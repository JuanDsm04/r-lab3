"""Modelo y serialización NDJSON de los paquetes del protocolo v1."""

from __future__ import annotations

import json
import math
import time
import uuid
from typing import Any


PROTOCOL_VERSION = 1
VALID_PROTOS = frozenset({"dijkstra", "flooding", "lsr"})
VALID_TYPES = frozenset({"hello", "echo", "message", "info"})


def _is_number(value: object) -> bool:
    """Indica si ``value`` es un número finito, pero no un booleano."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


class Packet:
    """Representa el envelope común usado por todos los modos de la red.

    ``from_node`` es el emisor inmediato del salto actual; ``to`` conserva el
    destino final. El identificador no se modifica cuando el paquete se
    reenvía.
    """

    def __init__(
        self,
        proto: str,
        type_: str,
        from_node: str,
        to: str,
        ttl: int,
        payload: Any,
        headers: list[dict[str, Any]] | None = None,
        packet_id: str | None = None,
        version: int = PROTOCOL_VERSION,
    ) -> None:
        self.version = version
        self.id = packet_id if packet_id is not None else str(uuid.uuid4())
        self.proto = proto
        self.type = type_
        self.from_node = from_node
        self.to = to
        self.ttl = ttl
        self.headers = [] if headers is None else headers
        self.payload = payload

        self._validate()

    def _validate(self) -> None:
        """Valida el envelope y la forma del payload según ``type``."""
        if (
            not isinstance(self.version, int)
            or isinstance(self.version, bool)
            or self.version != PROTOCOL_VERSION
        ):
            raise ValueError(f"version no soportada: {self.version!r}")
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("id debe ser un string no vacío")
        if self.proto not in VALID_PROTOS:
            raise ValueError(f"proto inválido: {self.proto!r}")
        if self.type not in VALID_TYPES:
            raise ValueError(f"type inválido: {self.type!r}")
        if not isinstance(self.from_node, str) or not self.from_node.strip():
            raise ValueError("from debe ser un string no vacío")
        if not isinstance(self.to, str) or not self.to.strip():
            raise ValueError("to debe ser un string no vacío")
        if not isinstance(self.ttl, int) or isinstance(self.ttl, bool):
            raise TypeError("ttl debe ser un entero")
        if not isinstance(self.headers, list):
            raise TypeError("headers debe ser una lista")

        for header in self.headers:
            if not isinstance(header, dict):
                raise TypeError("cada elemento de headers debe ser un objeto")
            if "hops" in header:
                hops = header["hops"]
                if not isinstance(hops, list) or not all(
                    isinstance(hop, str) and hop for hop in hops
                ):
                    raise TypeError("headers[].hops debe ser una lista de strings")

        validators = {
            "hello": self._validate_hello,
            "echo": self._validate_echo,
            "message": self._validate_message,
            "info": self._validate_info,
        }
        validators[self.type]()

    def _validate_hello(self) -> None:
        if self.ttl != 1:
            raise ValueError("un paquete hello debe tener ttl=1")
        self._validate_timed_payload(required={"seq", "sent_at"})

    def _validate_echo(self) -> None:
        if self.ttl != 1:
            raise ValueError("un paquete echo debe tener ttl=1")
        self._validate_timed_payload(required={"seq", "sent_at", "echoed_at"})

    def _validate_timed_payload(self, required: set[str]) -> None:
        if not isinstance(self.payload, dict):
            raise TypeError(f"payload de {self.type} debe ser un objeto")
        if not required.issubset(self.payload):
            missing = sorted(required - self.payload.keys())
            raise ValueError(f"payload de {self.type} incompleto: faltan {missing}")
        seq = self.payload["seq"]
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            raise TypeError("payload.seq debe ser un entero no negativo")
        for field in required - {"seq"}:
            if not _is_number(self.payload[field]) or self.payload[field] < 0:
                raise TypeError(f"payload.{field} debe ser un timestamp válido")

    def _validate_message(self) -> None:
        if not isinstance(self.payload, str):
            raise TypeError("payload de message debe ser texto plano")

    def _validate_info(self) -> None:
        if self.to != "*":
            raise ValueError('un paquete info debe usar to="*"')
        if not isinstance(self.payload, dict):
            raise TypeError("payload de info debe ser un objeto")

        required = {"origin", "seq", "neighbors"}
        if not required.issubset(self.payload):
            missing = sorted(required - self.payload.keys())
            raise ValueError(f"payload de info incompleto: faltan {missing}")

        origin = self.payload["origin"]
        seq = self.payload["seq"]
        neighbors = self.payload["neighbors"]
        if not isinstance(origin, str) or not origin.strip():
            raise TypeError("payload.origin debe ser un string no vacío")
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            raise TypeError("payload.seq debe ser un entero no negativo")
        if not isinstance(neighbors, dict):
            raise TypeError("payload.neighbors debe ser un objeto")
        for node_id, cost in neighbors.items():
            if not isinstance(node_id, str) or not node_id.strip():
                raise TypeError("cada ID en payload.neighbors debe ser un string")
            if not _is_number(cost) or cost < 0:
                raise TypeError("cada costo en payload.neighbors debe ser no negativo")

    @property
    def is_expired(self) -> bool:
        """Retorna ``True`` cuando el paquete ya no puede procesarse."""
        return self.ttl <= 0

    @property
    def is_broadcast(self) -> bool:
        """Retorna ``True`` para el broadcast lógico de LSPs."""
        return self.to == "*"

    def decrement_ttl(self) -> Packet:
        """Descuenta un salto y retorna el mismo paquete."""
        self.ttl -= 1
        return self

    def update_sender(self, new_sender: str) -> Packet:
        """Actualiza el emisor inmediato antes de reenviar."""
        if not isinstance(new_sender, str) or not new_sender.strip():
            raise ValueError("new_sender debe ser un string no vacío")
        self.from_node = new_sender
        return self

    def add_hop(self, node_id: str) -> None:
        """Agrega un nodo a ``headers[].hops`` o crea la traza."""
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id debe ser un string no vacío")
        for header in self.headers:
            if "hops" in header:
                header["hops"].append(node_id)
                return
        self.headers.append({"hops": [node_id]})

    def get_hops(self) -> list[str]:
        """Retorna una copia de la traza; retorna una lista vacía si no existe."""
        for header in self.headers:
            if "hops" in header:
                return list(header["hops"])
        return []

    def to_dict(self) -> dict[str, Any]:
        """Convierte el paquete al envelope canónico."""
        return {
            "version": self.version,
            "id": self.id,
            "proto": self.proto,
            "type": self.type,
            "from": self.from_node,
            "to": self.to,
            "ttl": self.ttl,
            "headers": self.headers,
            "payload": self.payload,
        }

    def to_json(self) -> str:
        """Serializa como una única línea JSON compacta terminada en ``\n``."""
        return json.dumps(
            self.to_dict(), separators=(",", ":"), ensure_ascii=False
        ) + "\n"

    @classmethod
    def from_json(cls, line: str | bytes) -> Packet | None:
        """Deserializa una línea y retorna ``None`` si cualquier campo es inválido."""
        if isinstance(line, bytes):
            try:
                line = line.decode("utf-8")
            except UnicodeDecodeError:
                return None
        if not isinstance(line, str):
            return None

        try:
            data = json.loads(line)
            if not isinstance(data, dict):
                return None

            required = {"id", "proto", "type", "from", "to", "ttl", "payload"}
            if not required.issubset(data):
                return None

            return cls(
                proto=data["proto"],
                type_=data["type"],
                from_node=data["from"],
                to=data["to"],
                ttl=data["ttl"],
                payload=data["payload"],
                headers=data.get("headers", []),
                packet_id=data["id"],
                version=data.get("version", PROTOCOL_VERSION),
            )
        except (json.JSONDecodeError, TypeError, ValueError):
            return None

    @classmethod
    def make_hello(cls, proto: str, from_node: str, to: str, seq: int) -> Packet:
        """Construye un health-check dirigido a un vecino inmediato."""
        return cls(
            proto=proto,
            type_="hello",
            from_node=from_node,
            to=to,
            ttl=1,
            payload={"seq": seq, "sent_at": time.time()},
        )

    @classmethod
    def make_echo(
        cls,
        proto: str,
        from_node: str,
        to: str,
        hello_packet: Packet,
    ) -> Packet:
        """Construye la respuesta a un ``hello`` conservando secuencia y tiempo."""
        if not isinstance(hello_packet, Packet) or hello_packet.type != "hello":
            raise ValueError("hello_packet debe ser un Packet de tipo hello")
        return cls(
            proto=proto,
            type_="echo",
            from_node=from_node,
            to=to,
            ttl=1,
            payload={
                "seq": hello_packet.payload["seq"],
                "sent_at": hello_packet.payload["sent_at"],
                "echoed_at": time.time(),
            },
        )

    @classmethod
    def make_message(
        cls,
        proto: str,
        from_node: str,
        to: str,
        text: str,
        ttl: int,
        with_trace: bool = True,
    ) -> Packet:
        """Construye un mensaje de usuario con traza opcional."""
        headers = [{"hops": [from_node]}] if with_trace else []
        return cls(proto, "message", from_node, to, ttl, text, headers=headers)

    @classmethod
    def make_info(
        cls,
        proto: str,
        from_node: str,
        origin: str,
        seq: int,
        neighbors: dict[str, int | float],
        ttl: int,
    ) -> Packet:
        """Construye un LSP destinado al broadcast lógico."""
        return cls(
            proto=proto,
            type_="info",
            from_node=from_node,
            to="*",
            ttl=ttl,
            payload={"origin": origin, "seq": seq, "neighbors": neighbors},
        )

    def __repr__(self) -> str:
        return (
            f"Packet(id={self.id[:8]}..., proto={self.proto}, type={self.type}, "
            f"from={self.from_node}, to={self.to}, ttl={self.ttl})"
        )
