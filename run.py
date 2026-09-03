"""CLI de entrada para ejecutar un nodo del laboratorio."""

from __future__ import annotations

import argparse
import sys

from src.node import Node


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ejecuta un nodo del simulador de routing sobre TCP."
    )
    parser.add_argument(
        "config",
        help="ruta al JSON del nodo, por ejemplo config/node_A_lsr.json",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    node: Node | None = None
    try:
        node = Node(args.config)
        node.start()
        return 0
    except (FileNotFoundError, ValueError, OSError, RuntimeError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    finally:
        if node is not None:
            node.stop()


if __name__ == "__main__":
    raise SystemExit(main())

