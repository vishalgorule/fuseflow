#!/usr/bin/env bash
# Launches the platform services detached (logs in /tmp/fuseflow-*.log). Engine HA (Phase 5)
# is the default: two engine instances split the 8 shards (0-3 on 8082, 4-7 on 8084) and share
# the fuseflow-engine group on activity-results, so killing one engine mid-run loses no work.
# Each JVM is launched via daemon-java.sh (double-fork + setsid) so the services survive the
# calling shell being torn down — the same launcher the fleet-workers script uses.
#   scripts/start-services.sh      (returns immediately; logs in /tmp/fuseflow-*.log)
#   make stop-services             (stop them)
set -euo pipefail
cd "$(dirname "$0")/.."

for m in fuseflow-api-gateway fuseflow-definition-service fuseflow-workflow-engine fuseflow-worker-registry; do
    ls "$m"/target/"$m"-*.jar >/dev/null 2>&1 || { echo "ERROR: missing $m jar — run 'make build' first"; exit 1; }
done

# Both engine instances must be able to consume results: activity-results needs concurrency ×
# instances = 4 partitions so the range assignor gives each instance a fair share.
./scripts/ensure-engine-ha-partitions.sh 4

./scripts/daemon-java.sh fuseflow-api-gateway/target/fuseflow-api-gateway-*.jar /tmp/fuseflow-gateway.log
./scripts/daemon-java.sh fuseflow-definition-service/target/fuseflow-definition-service-*.jar /tmp/fuseflow-definition.log
./scripts/daemon-java.sh fuseflow-workflow-engine/target/fuseflow-workflow-engine-*.jar /tmp/fuseflow-engine.log \
    FUSEFLOW_ENGINE_OWNED_SHARDS=0-3 FUSEFLOW_ENGINE_LISTENER_CONCURRENCY=2 FUSEFLOW_KAFKA_TOPICS_PARTITIONS=4 \
    FUSEFLOW_ENGINE_WORKER_EVENTS_GROUP=fuseflow-engine-events-a
./scripts/daemon-java.sh fuseflow-workflow-engine/target/fuseflow-workflow-engine-*.jar /tmp/fuseflow-engine-2.log \
    SERVER_PORT=8084 FUSEFLOW_ENGINE_OWNED_SHARDS=4-7 FUSEFLOW_ENGINE_LISTENER_CONCURRENCY=2 FUSEFLOW_KAFKA_TOPICS_PARTITIONS=4 \
    FUSEFLOW_ENGINE_WORKER_EVENTS_GROUP=fuseflow-engine-events-b
./scripts/daemon-java.sh fuseflow-worker-registry/target/fuseflow-worker-registry-*.jar /tmp/fuseflow-registry.log

echo "launched (gateway 8080, definition 8081, engine 8082 + HA replica 8084, registry 8083) — logs in /tmp/fuseflow-*.log"
