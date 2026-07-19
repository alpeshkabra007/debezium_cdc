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

## Project layout

```
.
├── docker-compose.yaml            # Kafka, Connect, source & target databases
├── Dockerfile
├── jdbc-sql-server-sink.json      # SQL Server sink connector config
├── mysql-source.json              # MySQL source connector config
├── jar/                           # JDBC drivers mounted into Kafka Connect
└── debezium-sqlserver-init/
    └── inventory.sql              # sample schema/seed data
```

> **Note:** credentials in the connector configs and SQL scripts are for local
> development only. Supply real secrets via environment variables or a secrets
> manager — never commit them.

## License

MIT
