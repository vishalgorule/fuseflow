#!/usr/bin/env bash
# Phase 5: launch a heterogeneous worker fleet on pool-based routing.
#
#   N "io"    workers  -> pool `io`    (downloadImage, uploadImage only)
#   N "media" workers  -> pool `media` (resizeImage, watermarkImage, compressImage only)
#
# Each pool gets its own consumer group (the pool name) and its own dispatch queue
# (fuseflow-pool.<pool>), auto-provisioned by the engine with partitions = min(declared
# concurrency, cap). Instances of the same pool share the group for parallelism.
#
# Usage: start-fleet-workers.sh [io-count] [media-count] [concurrency]
#   default: 3 3 8   (6 workers on ports 8100-8105, 8-partition pool queues)
set -euo pipefail
cd "$(dirname "$0")/.."

IO=${1:-3}
MEDIA=${2:-3}
THREADS=${3:-8}

JAR=$(ls fuseflow-sample-workers/target/fuseflow-sample-workers-*.jar 2>/dev/null | head -1)
if [ -z "$JAR" ]; then
    echo "ERROR: sample-workers jar missing — run 'make build' first" >&2
    exit 1
fi

# Stop any previously launched fleet workers (ports 8100+).
for port in $(seq 8100 $((8100 + IO + MEDIA))); do
    pid=$(lsof -ti tcp:$port 2>/dev/null || true)
    if [ -n "$pid" ]; then
        kill "$pid" 2>/dev/null || true
    fi
done
sleep 3

# Reset the pool consumer groups to latest: with `auto.offset.reset=earliest`, a brand-new
# group would otherwise replay the whole dispatch history from previous demo sessions.
reset_group() {
    docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 --reset-offsets --to-latest \
        --group "$1" --all-topics --execute >/dev/null 2>&1 || true
}
reset_group io
reset_group media

launch() {
    local port=$1 name=$2 log=$3
    shift 3
    ./scripts/daemon-java.sh "$JAR" "$log" "$@"
    echo "$name (port $port) launched -> $log"
}

PORT=8100
echo "=== launching $IO io worker(s) + $MEDIA media worker(s), concurrency=$THREADS ==="
for i in $(seq 1 "$IO"); do
    launch "$PORT" "io-$i" "/tmp/fuseflow-workers-io-$i.log" \
        "SERVER_PORT=$PORT" \
        "FUSEFLOW_WORKER_ID=33330000-0000-0000-0000-0000000000$(printf %02d "$i")" \
        "FUSEFLOW_WORKER_POOL=io" \
        "FUSEFLOW_WORKER_CONCURRENCY=$THREADS" \
        "FUSEFLOW_SAMPLE_ENABLE_DOWNLOAD=true" \
        "FUSEFLOW_SAMPLE_ENABLE_UPLOAD=true" \
        "FUSEFLOW_SAMPLE_ENABLE_RESIZE=false" \
        "FUSEFLOW_SAMPLE_ENABLE_WATERMARK=false" \
        "FUSEFLOW_SAMPLE_ENABLE_COMPRESS=false"
    PORT=$((PORT + 1))
done
for i in $(seq 1 "$MEDIA"); do
    launch "$PORT" "media-$i" "/tmp/fuseflow-workers-media-$i.log" \
        "SERVER_PORT=$PORT" \
        "FUSEFLOW_WORKER_ID=44440000-0000-0000-0000-0000000000$(printf %02d "$i")" \
        "FUSEFLOW_WORKER_POOL=media" \
        "FUSEFLOW_WORKER_CONCURRENCY=$THREADS" \
        "FUSEFLOW_SAMPLE_ENABLE_DOWNLOAD=false" \
        "FUSEFLOW_SAMPLE_ENABLE_UPLOAD=false" \
        "FUSEFLOW_SAMPLE_ENABLE_RESIZE=true" \
        "FUSEFLOW_SAMPLE_ENABLE_WATERMARK=true" \
        "FUSEFLOW_SAMPLE_ENABLE_COMPRESS=true"
    PORT=$((PORT + 1))
done

echo
echo "Waiting for workers to become healthy..."
for attempt in $(seq 1 60); do
    total=0; up=0
    for port in $(seq 8100 $((8099 + IO + MEDIA))); do
        total=$((total + 1))
        if curl -s --max-time 2 "localhost:$port/actuator/health" >/dev/null 2>&1; then
            up=$((up + 1))
        fi
    done
    [ "$up" -eq "$total" ] && break
    sleep 2
done

echo
echo "=== fleet worker health ==="
for port in $(seq 8100 $((8099 + IO + MEDIA))); do
    state=$(curl -s --max-time 2 "localhost:$port/actuator/health" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo DOWN)
    echo "  port $port: $state"
done

echo
echo "=== registry view ==="
curl -s --max-time 3 localhost:8083/api/v1/workers 2>/dev/null | python3 -c "
import sys, json
workers = json.load(sys.stdin)
for w in sorted(workers, key=lambda w: (w.get('poolName',''), w.get('id',''))):
    print('  pool=%-8s status=%-7s activities=%s' % (w.get('poolName'), w.get('status'), w.get('activities')))
" 2>/dev/null || echo "  (registry not reachable — is the stack up?)"
echo
echo "Fleet live. Run: scripts/demo-scale.sh --fleet 10 20 32"
