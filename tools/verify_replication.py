"""End-to-end replication verifier for the CDC pipeline.

This tool exercises the SQL Server -> MySQL Debezium pipeline by mutating a
row in the *source* database and polling the *target* database until the
change is applied (or a timeout elapses). It then reports ``PASS`` or
``FAIL``.

The database drivers (``pyodbc`` for SQL Server, ``pymysql`` for MySQL) are
imported *lazily* inside the functions that use them, so this module always
imports cleanly even when the drivers -- or their system dependencies -- are
not installed. That keeps the pure comparison logic unit-testable without any
live infrastructure.

The polling / comparison core is intentionally factored into pure functions
(:func:`rows_match` and :func:`poll_until`) that take callables and plain data
so they can be tested in isolation.
"""

from __future__ import annotations

import argparse
import time
from typing import Any, Callable, Dict, Iterable, Optional, Sequence

# An operation applied to the source row: one of these string literals.
OP_INSERT = "insert"
OP_UPDATE = "update"
OP_DELETE = "delete"
OPERATIONS = (OP_INSERT, OP_UPDATE, OP_DELETE)


# ---------------------------------------------------------------------------
# Pure logic (unit-testable, no DB required)
# ---------------------------------------------------------------------------


def rows_match(
    source_row: Optional[Dict[str, Any]],
    target_row: Optional[Dict[str, Any]],
    keys: Sequence[str],
) -> bool:
    """Return ``True`` if ``target_row`` matches ``source_row`` on ``keys``.

    Only the columns named in ``keys`` are compared, which lets callers ignore
    columns that legitimately differ between the two stores (for example
    database-managed timestamps).

    Semantics
    ---------
    * If *both* rows are ``None`` the row is considered replicated as a delete
      (it is absent on both sides) -> ``True``.
    * If exactly one row is ``None`` they cannot match -> ``False``.
    * Otherwise every key must be present in both rows and compare equal.

    A missing key on either side is treated as a mismatch rather than raising,
    so partially-projected target rows fail cleanly instead of crashing the
    poll loop.
    """
    if source_row is None and target_row is None:
        return True
    if source_row is None or target_row is None:
        return False

    for key in keys:
        if key not in source_row or key not in target_row:
            return False
        if source_row[key] != target_row[key]:
            return False
    return True


def poll_until(
    predicate: Callable[[], bool],
    timeout: float,
    interval: float,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> bool:
    """Poll ``predicate`` until it is truthy or ``timeout`` seconds elapse.

    ``predicate`` is always evaluated at least once. Between attempts the
    function waits ``interval`` seconds. The ``sleep`` and ``now`` callables
    are injectable so the loop can be driven deterministically in tests
    without real wall-clock delays.

    Returns
    -------
    bool
        ``True`` if ``predicate`` became truthy within the deadline, else
        ``False``.
    """
    deadline = now() + timeout
    while True:
        if predicate():
            return True
        if now() >= deadline:
            return False
        sleep(interval)


def expected_target_row(
    source_row: Optional[Dict[str, Any]],
    operation: str,
) -> Optional[Dict[str, Any]]:
    """Return the row expected on the target given the source ``operation``.

    For inserts and updates the target should hold ``source_row``; for a
    delete the row should be absent (``None``).
    """
    if operation not in OPERATIONS:
        raise ValueError(f"Unknown operation: {operation!r}")
    if operation == OP_DELETE:
        return None
    return source_row


# ---------------------------------------------------------------------------
# Database access (lazy driver imports -- not exercised in unit tests)
# ---------------------------------------------------------------------------


def _connect_sqlserver(cfg: "argparse.Namespace"):  # pragma: no cover
    """Open a SQL Server connection using ``pyodbc`` (imported lazily)."""
    import pyodbc

    conn_str = (
        f"DRIVER={{{cfg.mssql_driver}}};"
        f"SERVER={cfg.mssql_host},{cfg.mssql_port};"
        f"DATABASE={cfg.mssql_db};"
        f"UID={cfg.mssql_user};"
        f"PWD={cfg.mssql_password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=cfg.connect_timeout)


def _connect_mysql(cfg: "argparse.Namespace"):  # pragma: no cover
    """Open a MySQL connection using ``pymysql`` (imported lazily)."""
    import pymysql
    from pymysql.cursors import DictCursor

    return pymysql.connect(
        host=cfg.mysql_host,
        port=cfg.mysql_port,
        user=cfg.mysql_user,
        password=cfg.mysql_password,
        database=cfg.mysql_db,
        connect_timeout=cfg.connect_timeout,
        cursorclass=DictCursor,
    )


def _fetch_target_row(
    conn: Any,
    table: str,
    key_column: str,
    key_value: Any,
) -> Optional[Dict[str, Any]]:  # pragma: no cover
    """Fetch a single row from the target table keyed by ``key_column``."""
    sql = f"SELECT * FROM {table} WHERE {key_column} = %s"
    with conn.cursor() as cursor:
        cursor.execute(sql, (key_value,))
        row = cursor.fetchone()
    return dict(row) if row is not None else None


def apply_source_change(
    conn: Any,
    table: str,
    operation: str,
    key_column: str,
    row: Dict[str, Any],
) -> None:  # pragma: no cover
    """Apply ``operation`` to ``row`` in the source table and commit."""
    if operation not in OPERATIONS:
        raise ValueError(f"Unknown operation: {operation!r}")

    with conn.cursor() as cursor:
        if operation == OP_INSERT:
            columns = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            cursor.execute(
                f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                list(row.values()),
            )
        elif operation == OP_UPDATE:
            assignments = ", ".join(
                f"{col} = ?" for col in row if col != key_column
            )
            values = [row[col] for col in row if col != key_column]
            values.append(row[key_column])
            cursor.execute(
                f"UPDATE {table} SET {assignments} WHERE {key_column} = ?",
                values,
            )
        else:  # OP_DELETE
            cursor.execute(
                f"DELETE FROM {table} WHERE {key_column} = ?",
                [row[key_column]],
            )
    conn.commit()


def verify(cfg: "argparse.Namespace") -> bool:  # pragma: no cover
    """Run the full replication check and return ``True`` on PASS.

    Applies the requested change to the source, then polls the target until
    the expected state is observed or the timeout elapses.
    """
    source = _connect_sqlserver(cfg)
    target = _connect_mysql(cfg)
    try:
        row = {cfg.key_column: cfg.key_value}
        row.update(_parse_set_values(cfg.set_values))

        print(
            f"Applying {cfg.operation} to source table "
            f"{cfg.source_table} ({cfg.key_column}={cfg.key_value})"
        )
        apply_source_change(source, cfg.source_table, cfg.operation, cfg.key_column, row)

        expected = expected_target_row(row, cfg.operation)
        compare_keys = list(row.keys())

        def replicated() -> bool:
            observed = _fetch_target_row(
                target, cfg.target_table, cfg.key_column, cfg.key_value
            )
            return rows_match(expected, observed, compare_keys)

        ok = poll_until(replicated, timeout=cfg.timeout, interval=cfg.interval)
        print("Replication PASS" if ok else "Replication FAIL")
        return ok
    finally:
        source.close()
        target.close()


def _parse_set_values(pairs: Iterable[str]) -> Dict[str, Any]:
    """Parse ``key=value`` CLI pairs into a dict of column values."""
    values: Dict[str, Any] = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise ValueError(f"--set expects key=value, got {pair!r}")
        key, value = pair.split("=", 1)
        values[key.strip()] = value
    return values


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser for the verifier CLI."""
    parser = argparse.ArgumentParser(
        prog="tools.verify_replication",
        description="Verify SQL Server -> MySQL replication end to end.",
    )

    # Source (SQL Server) connection.
    parser.add_argument("--mssql-host", default="localhost")
    parser.add_argument("--mssql-port", type=int, default=1433)
    parser.add_argument("--mssql-db", default="inventory")
    parser.add_argument("--mssql-user", default="sa")
    parser.add_argument("--mssql-password", default="Password!")
    parser.add_argument(
        "--mssql-driver",
        default="ODBC Driver 17 for SQL Server",
        help="ODBC driver name registered with the system",
    )

    # Target (MySQL) connection.
    parser.add_argument("--mysql-host", default="localhost")
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-db", default="inventory")
    parser.add_argument("--mysql-user", default="root")
    parser.add_argument("--mysql-password", default="")

    # What to change.
    parser.add_argument("--source-table", default="dbo.products")
    parser.add_argument("--target-table", default="products")
    parser.add_argument("--key-column", default="id")
    parser.add_argument("--key-value", default="9001")
    parser.add_argument(
        "--operation",
        choices=OPERATIONS,
        default=OP_INSERT,
        help="Change to apply to the source row",
    )
    parser.add_argument(
        "--set",
        dest="set_values",
        action="append",
        default=[],
        metavar="COL=VALUE",
        help="Column values for insert/update (repeatable)",
    )

    # Polling behaviour.
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--connect-timeout", type=float, default=10.0)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point. Returns a process exit code (0 = PASS)."""
    parser = build_parser()
    cfg = parser.parse_args(argv)
    ok = verify(cfg)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
