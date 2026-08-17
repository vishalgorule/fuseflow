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

services: build ## Build and run the platform services with engine HA (requires `make up` first; logs in /tmp/fuseflow-*.log)
	@for m in fuseflow-api-gateway fuseflow-definition-service fuseflow-workflow-engine fuseflow-worker-registry; do \
		ls $$m/target/$$m-*.jar >/dev/null 2>&1 || { echo "ERROR: missing $$m jar — run 'make build' first"; exit 1; }; \
	done
	@scripts/ensure-engine-ha-partitions.sh 4
	@echo "Starting services in background — logs in /tmp/fuseflow-*.log"
	@# Engine HA (Phase 5): two instances split the 8 shards (0-3 on 8082, 4-7 on 8084) and join
	@# the same fuseflow-engine group on activity-results — 4 partitions (concurrency 2 × 2
	@# instances) so the range assignor gives each instance a fair share. Each instance gets its
	@# OWN worker-events group so every worker registration/offline event refreshes its routing
	@# table (a shared group would only refresh one instance — see ensure-engine-ha-partitions.sh).
	@# daemon-java.sh double-forks + setsids each JVM so it survives shell/process-group teardown
	@# (plain nohup is not enough — see the fleet-workers launcher).
	@./scripts/daemon-java.sh fuseflow-api-gateway/target/fuseflow-api-gateway-*.jar /tmp/fuseflow-gateway.log
	@./scripts/daemon-java.sh fuseflow-definition-service/target/fuseflow-definition-service-*.jar /tmp/fuseflow-definition.log
	@./scripts/daemon-java.sh fuseflow-workflow-engine/target/fuseflow-workflow-engine-*.jar /tmp/fuseflow-engine.log FUSEFLOW_ENGINE_OWNED_SHARDS=0-3 FUSEFLOW_ENGINE_LISTENER_CONCURRENCY=2 FUSEFLOW_KAFKA_TOPICS_PARTITIONS=4 FUSEFLOW_ENGINE_WORKER_EVENTS_GROUP=fuseflow-engine-events-a
	@./scripts/daemon-java.sh fuseflow-workflow-engine/target/fuseflow-workflow-engine-*.jar /tmp/fuseflow-engine-2.log SERVER_PORT=8084 FUSEFLOW_ENGINE_OWNED_SHARDS=4-7 FUSEFLOW_ENGINE_LISTENER_CONCURRENCY=2 FUSEFLOW_KAFKA_TOPICS_PARTITIONS=4 FUSEFLOW_ENGINE_WORKER_EVENTS_GROUP=fuseflow-engine-events-b
	@./scripts/daemon-java.sh fuseflow-worker-registry/target/fuseflow-worker-registry-*.jar /tmp/fuseflow-registry.log

workers: build ## Build and run the sample SDK workers (port 8090; requires engine + registry up)
	@ls fuseflow-sample-workers/target/fuseflow-sample-workers-*.jar >/dev/null 2>&1 || { echo "ERROR: missing sample-workers jar — run 'make build' first"; exit 1; }
	@echo "Starting sample workers — log in /tmp/fuseflow-workers.log"
	@./scripts/daemon-java.sh fuseflow-sample-workers/target/fuseflow-sample-workers-*.jar /tmp/fuseflow-workers.log

stop-workers: ## Stop the locally running sample workers
	@pid=$$(lsof -ti tcp:8090 2>/dev/null); \
	if [ -n "$$pid" ]; then kill $$pid && echo "stopped sample-workers ($$pid)"; fi

workers-fleet: build ## Build + launch the io pool fleet (8 io + 0 media workers, concurrency 8, ports 8100-8107)
	@scripts/start-fleet-workers.sh 8 0 8

stop-fleet-workers: ## Stop the fleet workers (ports 8100+)
	@for port in $$(seq 8100 8120); do \
		pid=$$(lsof -ti tcp:$$port 2>/dev/null); \
		if [ -n "$$pid" ]; then kill $$pid && echo "stopped fleet worker on $$port ($$pid)"; fi; \
	done

stop-services: ## Stop locally running services
	@for p in gateway:8080 definition:8081 engine:8082 engine-2:8084 registry:8083; do \
		name=$${p%%:*}; port=$${p##*:}; \
		pid=$$(lsof -ti tcp:$$port 2>/dev/null); \
		if [ -n "$$pid" ]; then kill $$pid && echo "stopped $$name ($$pid)"; fi; \
	done
