# Debezium Change Data Capture (SQL Server → MySQL)

A Docker-based **Change Data Capture (CDC)** pipeline that streams row-level
changes from **SQL Server** to **MySQL** in near real-time using
[Debezium](https://debezium.io/), [Kafka Connect](https://kafka.apache.org/documentation/#connect),
and JDBC sink connectors.

Inserts, updates, and deletes on the source database are captured from the
transaction log, published to Kafka topics, and applied to the target database —
no dual-writes, no polling.

## How it works

```
SQL Server  ──▶  Debezium source connector  ──▶  Kafka topics  ──▶  JDBC sink connector  ──▶  MySQL
 (source)          (reads the txn log)                                (applies changes)      (target)
```

The JDBC driver JARs under `jar/` are mounted into the Kafka Connect container
(see `docker-compose.yaml`) so the connectors can talk to both databases.

## Prerequisites

- Docker & Docker Compose

## 1. Start the stack

```bash
docker compose up --build
```

Kafka Connect's REST API comes up on `http://localhost:8083`.

## 2. Register the connectors

```bash
# SQL Server sink connector
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @jdbc-sql-server-sink.json

# MySQL source connector
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @mysql-source.json
```

## 3. Seed the source database

```bash
/opt/mssql-tools/bin/sqlcmd -U sa -P 'Password!' \
  -i /tmp/debezium-sqlserver-init/inventory.sql
```

## Managing connectors

```bash
# List connectors
curl -s http://localhost:8083/connectors/

# Delete a connector
curl -i -X DELETE http://localhost:8083/connectors/sql-server-sink
curl -i -X DELETE http://localhost:8083/connectors/mysql-connector
```

## Tooling

A small Python package under `tools/` wraps the pipeline in a friendlier
interface. Install the dependencies first:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> `pyodbc` needs system ODBC drivers (unixODBC + the Microsoft ODBC Driver for
> SQL Server). It is imported lazily, so the CLI and the unit tests work even
> if it is not installed.

### Connector CLI

`tools/cli.py` is an `argparse` wrapper around a small Kafka Connect REST
client (`tools/connectors.py`, default base URL `http://localhost:8083`):

```bash
# Register the connectors from their JSON config files
python -m tools.cli register mysql-source.json
python -m tools.cli register jdbc-sql-server-sink.json

# List, inspect, restart and delete
python -m tools.cli list
python -m tools.cli status mysql-connector
python -m tools.cli restart mysql-connector
python -m tools.cli delete mysql-connector

# Point at a non-default worker
python -m tools.cli --base-url http://connect:8083 list
```

`make register` runs the two `register` commands for you.

### Replication verifier

`tools/verify_replication.py` proves the pipeline works end to end: it applies
an insert/update/delete to the SQL Server **source**, then polls the MySQL
**target** until the change appears (or a timeout elapses) and prints
`PASS`/`FAIL` (exit code `0`/`1`).

```bash
python -m tools.verify_replication \
  --operation insert \
  --source-table dbo.products --target-table products \
  --key-column id --key-value 9001 \
  --set name=widget --set weight=1.5 \
  --timeout 30 --interval 1
```

Connection parameters for both databases have sane local defaults and can be
overridden with flags (`--mssql-host`, `--mysql-host`, `--mysql-user`, ...).
The polling and comparison core is factored into pure functions
(`rows_match`, `poll_until`, `expected_target_row`) so it is unit-testable
without a live stack. `make verify` runs it with defaults.

## Tests

Logic-only unit tests live in `tests/` and require **no** live infrastructure
— the database drivers are imported lazily and the Kafka Connect client is
exercised against a mocked `requests` session:

```bash
pip install -r requirements.txt   # or just: pip install requests pytest
pytest -q                         # or: make test
```

The suite covers the row-comparison logic, the timeout-aware poll loop, and
the REST client's URL building and error handling.

## Project layout

```
.
├── docker-compose.yaml            # Kafka, Connect, source & target databases
├── Dockerfile
├── jdbc-sql-server-sink.json      # SQL Server sink connector config
├── mysql-source.json              # MySQL source connector config
├── jar/                           # JDBC drivers mounted into Kafka Connect
├── debezium-sqlserver-init/
│   └── inventory.sql              # sample schema/seed data
├── tools/                         # Python CLI + replication verifier
│   ├── connectors.py              # Kafka Connect REST client
│   ├── cli.py                     # argparse CLI wrapper
│   └── verify_replication.py      # end-to-end replication verifier
├── tests/                         # logic-only unit tests (no infra)
├── requirements.txt
└── Makefile                       # up / down / register / verify / test
```

> **Note:** credentials in the connector configs and SQL scripts are for local
> development only. Supply real secrets via environment variables or a secrets
> manager — never commit them.

## License

MIT
