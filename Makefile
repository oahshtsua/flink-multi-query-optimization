COMPOSE    := docker compose -f infra/docker-compose.yml --env-file infra/defaults.env $(if $(wildcard .env),--env-file .env)
SQL_CLIENT := $(COMPOSE) exec sql-gateway bin/sql-client.sh gateway -e localhost:8083

.DEFAULT_GOAL := help
.PHONY: help up down ps logs sql topics smoke

help: ## List make targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-8s %s\n", $$1, $$2}'

up: ## Start Kafka, Flink cluster and SQL gateway (waits until healthy)
	$(COMPOSE) up -d --build --wait

down: ## Stop everything and delete all data
	$(COMPOSE) down -v --remove-orphans

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Follow logs (S=<service> for a single service)
	$(COMPOSE) logs -f $(S)

sql: ## Open an interactive Flink SQL client connected to the gateway
	$(SQL_CLIENT)

topics: ## List Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

smoke: up ## End-to-end check via the REST APIs: Flink SQL writes to Kafka and reads it back
	$(COMPOSE) run --rm python infra/smoke.py
