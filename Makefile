SHELL := /bin/bash
MVN := ./mvnw

.PHONY: help up down build test verify clean logs ps services stop-services

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

up: ## Start Postgres + Kafka + Kafdrop
	docker compose up -d --wait

down: ## Stop and remove infra containers
	docker compose down

logs: ## Tail infra logs
	docker compose logs -f

ps: ## Show infra container status
	docker compose ps

build: ## Compile all modules (no tests)
	$(MVN) -q clean package -DskipTests

test: ## Run all tests (Testcontainers needs Docker)
	$(MVN) -q verify

verify: ## Alias for test
	$(MVN) -q verify

services: build ## Build and run all 4 services (requires `make up` first; logs in /tmp/fuseflow-*.log)
	@for m in fuseflow-api-gateway fuseflow-definition-service fuseflow-workflow-engine fuseflow-worker-registry; do \
		ls $$m/target/$$m-*.jar >/dev/null 2>&1 || { echo "ERROR: missing $$m jar — run 'make build' first"; exit 1; }; \
	done
	@echo "Starting services in background — logs in /tmp/fuseflow-*.log"
	@nohup java -jar fuseflow-api-gateway/target/fuseflow-api-gateway-*.jar > /tmp/fuseflow-gateway.log 2>&1 &
	@nohup java -jar fuseflow-definition-service/target/fuseflow-definition-service-*.jar > /tmp/fuseflow-definition.log 2>&1 &
	@nohup java -jar fuseflow-workflow-engine/target/fuseflow-workflow-engine-*.jar > /tmp/fuseflow-engine.log 2>&1 &
	@nohup java -jar fuseflow-worker-registry/target/fuseflow-worker-registry-*.jar > /tmp/fuseflow-registry.log 2>&1 &

stop-services: ## Stop locally running services
	@for p in gateway definition engine registry; do \
		pid=$$(lsof -ti tcp:$$(case $$p in gateway) echo 8080;; definition) echo 8081;; engine) echo 8082;; registry) echo 8083;; esac) 2>/dev/null); \
		if [ -n "$$pid" ]; then kill $$pid && echo "stopped $$p ($$pid)"; fi; \
	done
