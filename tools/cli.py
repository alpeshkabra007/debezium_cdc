"""Command line interface for managing the CDC pipeline connectors.

This is a thin ``argparse`` wrapper around
:class:`tools.connectors.KafkaConnectClient`. It supports registering,
listing, inspecting, restarting and deleting Kafka Connect connectors.

Examples
--------
Register both demo connectors::

    python -m tools.cli register mysql-source.json
    python -m tools.cli register jdbc-sql-server-sink.json

List and inspect::

    python -m tools.cli list
    python -m tools.cli status mysql-connector
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

try:  # allow running both as ``python -m tools.cli`` and ``python tools/cli.py``
    from tools.connectors import (
        DEFAULT_BASE_URL,
        ConnectError,
        KafkaConnectClient,
    )
except ImportError:  # pragma: no cover - exercised only via direct invocation
    import os

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from tools.connectors import (
        DEFAULT_BASE_URL,
        ConnectError,
        KafkaConnectClient,
    )


def build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        prog="tools.cli",
        description="Manage Kafka Connect connectors for the CDC pipeline.",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Kafka Connect REST base URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Per-request timeout in seconds (default: 10)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    register = subparsers.add_parser(
        "register", help="Register a connector from a JSON config file"
    )
    register.add_argument("config", help="Path to the connector JSON config file")

    subparsers.add_parser("list", help="List all registered connectors")

    status = subparsers.add_parser("status", help="Show a connector's status")
    status.add_argument("name", help="Connector name")

    delete = subparsers.add_parser("delete", help="Delete a connector")
    delete.add_argument("name", help="Connector name")

    restart = subparsers.add_parser("restart", help="Restart a connector")
    restart.add_argument("name", help="Connector name")

    return parser


def _print_json(value: object) -> None:
    """Pretty-print a JSON-serialisable value to stdout."""
    print(json.dumps(value, indent=2, sort_keys=True))


def run(args: argparse.Namespace) -> int:
    """Execute the parsed command and return a process exit code."""
    client = KafkaConnectClient(base_url=args.base_url, timeout=args.timeout)

    try:
        if args.command == "register":
            result = client.register(args.config)
            print(f"Registered connector from {args.config}")
            if result is not None:
                _print_json(result)
        elif args.command == "list":
            for name in client.list_connectors():
                print(name)
        elif args.command == "status":
            _print_json(client.status(args.name))
        elif args.command == "delete":
            client.delete(args.name)
            print(f"Deleted connector {args.name}")
        elif args.command == "restart":
            client.restart(args.name)
            print(f"Restarted connector {args.name}")
        else:  # pragma: no cover - argparse enforces valid commands
            raise ConnectError(f"Unknown command: {args.command}")
    except ConnectError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point. Returns a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
