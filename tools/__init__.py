"""Tooling around the Debezium SQL Server -> MySQL CDC pipeline.

This package provides:

- :mod:`tools.connectors` -- a thin client for the Kafka Connect REST API.
- :mod:`tools.cli` -- an ``argparse`` command line wrapper around the client.
- :mod:`tools.verify_replication` -- a source/target replication verifier.
"""

__all__ = ["connectors", "cli", "verify_replication"]

__version__ = "0.1.0"
