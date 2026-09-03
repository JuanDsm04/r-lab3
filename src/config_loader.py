"""Carga y valida configuraciones de nodos y topologías estáticas."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from src.neighbor import Neighbor


DEFAULT_PARAMS: dict[str, Any] = {
    "initial_ttl": 10,
    "hello_interval_sec": 5.0,
    "hello_timeout_sec": 3.0,
    "hello_max_failures": 3,
    "dedup_cache_ttl_sec": 60.0,
    "log_level": "INFO",
}
VALID_MODES = frozenset({"dijkstra", "flooding", "lsr"})
VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


def load_config(path: str | Path) -> dict[str, Any]:
    """Lee un JSON y retorna una configuración validada lista para usar."""
    config_path = Path(path).expanduser()
    raw = _load_json(config_path, "configuración")
    if not isinstance(raw, dict):
        raise ValueError(f"[{config_path}] la configuración debe ser un objeto JSON")

    _validate_required_fields(raw, config_path)
    params = _build_params(raw["params"], config_path)

    node_id = raw["node_id"].strip()
    neighbors: dict[str, Neighbor] = {}
    for index, entry in enumerate(raw["neighbors"]):
        neighbor = _build_neighbor(entry, index, config_path, params, node_id)
        if neighbor.node_id in neighbors:
            raise ValueError(
                f"[{config_path}] node_id de vecino duplicado: {neighbor.node_id!r}"
            )
        neighbors[neighbor.node_id] = neighbor

    config: dict[str, Any] = {
        "node_id": node_id,
        "listen": {
            "host": raw["listen"]["host"].strip(),
            "port": raw["listen"]["port"],
        },
        "mode": raw["mode"],
        "neighbors": neighbors,
        "params": params,
    }

    if raw["mode"] == "dijkstra":
        topology_path = Path(raw["topology_file"]).expanduser()
        if not topology_path.is_absolute():
            topology_path = config_path.parent / topology_path
        topology_path = topology_path.resolve()
        config["topology_file"] = str(topology_path)
        config["topology"] = load_topology(topology_path)

    return config


def load_topology(path: str | Path) -> dict[str, dict[str, float]]:
    """Carga una topología ``nodo -> vecino -> costo`` para Dijkstra."""
    topology_path = Path(path).expanduser()
    raw = _load_json(topology_path, "topología")
    if not isinstance(raw, dict) or not raw:
        raise ValueError(f"[{topology_path}] la topología debe ser un objeto no vacío")

    topology: dict[str, dict[str, float]] = {}
    for node, adjacent in raw.items():
        if not isinstance(node, str) or not node.strip():
            raise ValueError(f"[{topology_path}] cada nodo debe tener un ID válido")
        if not isinstance(adjacent, dict):
            raise ValueError(f"[{topology_path}] vecinos de {node!r} deben ser un objeto")

        clean_adjacent: dict[str, float] = {}
        for neighbor, cost in adjacent.items():
            if not isinstance(neighbor, str) or not neighbor.strip():
                raise ValueError(f"[{topology_path}] ID de vecino inválido en {node!r}")
            if neighbor == node:
                raise ValueError(f"[{topology_path}] {node!r} no puede enlazarse consigo mismo")
            _require_non_negative_number(cost, f"costo {node}->{neighbor}", topology_path)
            clean_adjacent[neighbor] = float(cost)
        topology[node] = clean_adjacent

    referenced = {neighbor for adjacent in topology.values() for neighbor in adjacent}
    missing_nodes = referenced - topology.keys()
    if missing_nodes:
        raise ValueError(
            f"[{topology_path}] faltan nodos referenciados: {sorted(missing_nodes)}"
        )
    return topology


def _load_json(path: Path, label: str) -> Any:
    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Archivo de {label} no encontrado: {path}") from None
    except json.JSONDecodeError as error:
        raise ValueError(f"JSON inválido en {label} '{path}': {error}") from error


def _validate_required_fields(raw: dict[str, Any], path: Path) -> None:
    required = {"node_id", "listen", "mode", "neighbors", "params"}
    missing = required - raw.keys()
    if missing:
        raise ValueError(f"[{path}] faltan campos obligatorios: {sorted(missing)}")

    if not isinstance(raw["node_id"], str) or not raw["node_id"].strip():
        raise ValueError(f"[{path}] node_id debe ser un string no vacío")
    if raw["mode"] not in VALID_MODES:
        raise ValueError(f"[{path}] mode debe ser uno de {sorted(VALID_MODES)}")

    listen = raw["listen"]
    if not isinstance(listen, dict):
        raise ValueError(f"[{path}] listen debe ser un objeto")
    if not isinstance(listen.get("host"), str) or not listen["host"].strip():
        raise ValueError(f"[{path}] listen.host debe ser un string no vacío")
    _require_port(listen.get("port"), "listen.port", path)

    if not isinstance(raw["neighbors"], list):
        raise ValueError(f"[{path}] neighbors debe ser una lista")
    if not isinstance(raw["params"], dict):
        raise ValueError(f"[{path}] params debe ser un objeto")

    if raw["mode"] == "dijkstra":
        topology_file = raw.get("topology_file")
        if not isinstance(topology_file, str) or not topology_file.strip():
            raise ValueError(f"[{path}] dijkstra requiere topology_file")


def _build_params(raw: dict[str, Any], path: Path) -> dict[str, Any]:
    params = {**DEFAULT_PARAMS, **raw}
    initial_ttl = params["initial_ttl"]
    max_failures = params["hello_max_failures"]
    if not isinstance(initial_ttl, int) or isinstance(initial_ttl, bool) or initial_ttl < 1:
        raise ValueError(f"[{path}] params.initial_ttl debe ser un entero positivo")
    if not isinstance(max_failures, int) or isinstance(max_failures, bool) or max_failures < 1:
        raise ValueError(
            f"[{path}] params.hello_max_failures debe ser un entero positivo"
        )

    for field in ("hello_interval_sec", "hello_timeout_sec", "dedup_cache_ttl_sec"):
        value = params[field]
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"[{path}] params.{field} debe ser un número positivo")
        params[field] = float(value)

    level = params["log_level"]
    if not isinstance(level, str) or level.upper() not in VALID_LOG_LEVELS:
        raise ValueError(f"[{path}] params.log_level no es válido")
    params["log_level"] = level.upper()
    return params


def _build_neighbor(
    entry: Any,
    index: int,
    path: Path,
    params: dict[str, Any],
    self_id: str,
) -> Neighbor:
    label = f"neighbors[{index}]"
    if not isinstance(entry, dict):
        raise ValueError(f"[{path}] {label} debe ser un objeto")
    required = {"node_id", "host", "port", "cost"}
    missing = required - entry.keys()
    if missing:
        raise ValueError(f"[{path}] {label} omite {sorted(missing)}")
    if not isinstance(entry["node_id"], str) or not entry["node_id"].strip():
        raise ValueError(f"[{path}] {label}.node_id debe ser un string no vacío")
    if entry["node_id"].strip() == self_id:
        raise ValueError(f"[{path}] un nodo no puede declararse como su propio vecino")
    if not isinstance(entry["host"], str) or not entry["host"].strip():
        raise ValueError(f"[{path}] {label}.host debe ser un string no vacío")
    _require_port(entry["port"], f"{label}.port", path)
    _require_non_negative_number(entry["cost"], f"{label}.cost", path)
    return Neighbor(
        node_id=entry["node_id"],
        host=entry["host"],
        port=entry["port"],
        cost=entry["cost"],
        max_failures=params["hello_max_failures"],
    )


def _require_port(value: Any, label: str, path: Path) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 65535:
        raise ValueError(f"[{path}] {label} debe ser un entero entre 1 y 65535")


def _require_non_negative_number(value: Any, label: str, path: Path) -> None:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"[{path}] {label} debe ser un número finito no negativo")


def print_config_summary(config: dict[str, Any]) -> None:
    """Imprime un resumen breve para diagnóstico durante el ensamblaje."""
    params = config["params"]
    print(f"Node ID  : {config['node_id']}")
    print(f"Mode     : {config['mode']}")
    print(f"Listen   : {config['listen']['host']}:{config['listen']['port']}")
    print(f"Neighbors: {list(config['neighbors'])}")
    print(f"TTL init : {params['initial_ttl']}")
    print(
        "Hello    : "
        f"cada {params['hello_interval_sec']}s, "
        f"timeout {params['hello_timeout_sec']}s, "
        f"máximo {params['hello_max_failures']} fallos"
    )

