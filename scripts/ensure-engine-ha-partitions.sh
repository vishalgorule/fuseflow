#!/usr/bin/env bash
# Engine HA prerequisite (Phase 5): the two engine instances share the `fuseflow-engine`
# consumer group on `activity-results`, so that topic needs enough partitions for both
# instances to consume results. The engines run with listener concurrency 2, so the target is
# concurrency × instances = 4 partitions — with 4 partitions and 4 consumer threads each
# instance holds 2 and the range assignor balances them (2 partitions would give both to one
# instance). The engine creates it on first boot with `fuseflow.kafka.topics.partitions`, and
# Kafka partitions can only grow — a topic created by a pre-HA run stays smaller, which would
# leave one engine instance with no partitions to consume. Run after `make up`, before
# starting the services.
#
# Usage: ensure-engine-ha-partitions.sh [target-partitions]
#   default target: 4 (listener concurrency 2 × 2 engine instances)
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET=${1:-4}

# Topic may not exist yet (the engine creates it on boot with FUSEFLOW_KAFKA_TOPICS_PARTITIONS
# set in the start scripts) — nothing to do in that case.
count=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --describe --topic activity-results 2>/dev/null \
    | grep -o 'PartitionCount: [0-9]*' | grep -o '[0-9]*' | head -1 || true)

if [ -z "$count" ]; then
    echo "activity-results not present yet — the engine will create it with $TARGET partition(s)"
elif [ "$count" -lt "$TARGET" ]; then
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --alter --topic activity-results \
        --partitions "$TARGET" >/dev/null 2>&1 || true
    echo "raised activity-results partitions $count -> $TARGET (engine HA)"
else
    echo "activity-results already has $count partition(s) — good for engine HA"
fi
