SHELL := /bin/bash
MVN := ./mvnw

.PHONY: help up down build test verify clean logs ps services stop-services workers stop-workers workers-fleet stop-fleet-workers

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

services: build ## Build and run the 4 platform services (requires `make up` first; logs in /tmp/fuseflow-*.log)
	@for m in fuseflow-api-gateway fuseflow-definition-service fuseflow-workflow-engine fuseflow-worker-registry; do \
		ls $$m/target/$$m-*.jar >/dev/null 2>&1 || { echo "ERROR: missing $$m jar — run 'make build' first"; exit 1; }; \
	done
	@echo "Starting services in background — logs in /tmp/fuseflow-*.log"
	@nohup java -jar fuseflow-api-gateway/target/fuseflow-api-gateway-*.jar > /tmp/fuseflow-gateway.log 2>&1 &
	@nohup java -jar fuseflow-definition-service/target/fuseflow-definition-service-*.jar > /tmp/fuseflow-definition.log 2>&1 &
	@nohup java -jar fuseflow-workflow-engine/target/fuseflow-workflow-engine-*.jar > /tmp/fuseflow-engine.log 2>&1 &
	@nohup java -jar fuseflow-worker-registry/target/fuseflow-worker-registry-*.jar > /tmp/fuseflow-registry.log 2>&1 &

workers: build ## Build and run the sample SDK workers (port 8090; requires engine + registry up)
	@ls fuseflow-sample-workers/target/fuseflow-sample-workers-*.jar >/dev/null 2>&1 || { echo "ERROR: missing sample-workers jar — run 'make build' first"; exit 1; }
	@echo "Starting sample workers — log in /tmp/fuseflow-workers.log"
	@nohup java -jar fuseflow-sample-workers/target/fuseflow-sample-workers-*.jar > /tmp/fuseflow-workers.log 2>&1 &

stop-workers: ## Stop the locally running sample workers
	@pid=$$(lsof -ti tcp:8090 2>/dev/null); \
	if [ -n "$$pid" ]; then kill $$pid && echo "stopped sample-workers ($$pid)"; fi

workers-fleet: build ## Build + launch a heterogeneous pool fleet (3 io + 3 media workers, ports 8100-8105)
	@scripts/start-fleet-workers.sh 3 3 8

stop-fleet-workers: ## Stop the fleet workers (ports 8100+)
	@for port in $$(seq 8100 8120); do \
		pid=$$(lsof -ti tcp:$$port 2>/dev/null); \
		if [ -n "$$pid" ]; then kill $$pid && echo "stopped fleet worker on $$port ($$pid)"; fi; \
	done

stop-services: ## Stop locally running services
	@for p in gateway:8080 definition:8081 engine:8082 registry:8083; do \
		name=$${p%%:*}; port=$${p##*:}; \
		pid=$$(lsof -ti tcp:$$port 2>/dev/null); \
		if [ -n "$$pid" ]; then kill $$pid && echo "stopped $$name ($$pid)"; fi; \
	done
