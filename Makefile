# Convenience targets for the Debezium CDC demo and its Python tooling.

PYTHON ?= python3
CONNECT_URL ?= http://localhost:8083

.PHONY: help up down register verify test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

up: ## Build and start the full stack (Kafka, Connect, databases)
	docker compose up --build

down: ## Stop the stack and remove containers
	docker compose down

register: ## Register both connectors via the CLI
	$(PYTHON) -m tools.cli --base-url $(CONNECT_URL) register mysql-source.json
	$(PYTHON) -m tools.cli --base-url $(CONNECT_URL) register jdbc-sql-server-sink.json

verify: ## Run the end-to-end replication verifier
	$(PYTHON) -m tools.verify_replication

test: ## Run the logic-only unit tests
	$(PYTHON) -m pytest -q
