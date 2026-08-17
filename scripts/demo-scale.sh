#!/usr/bin/env bash
# FuseFlow scale demo (Phase 5 — pool-based routing + engine HA).
#
#   Single mode:  demo-scale.sh <workflow-id> [count] [concurrency]
#       Runs `count` executions of one workflow definition (default 1 / 16).
#
#   Fleet mode:   demo-scale.sh --fleet <fleet-size> [per-workflow] [concurrency]
#       Registers `fleet-size` distinct workflow definitions across 5 DAG shapes (chain /
#       diamond / fanout / parallel / layered) built from the 5 real sample activities,
#       preflights worker coverage against the registry, runs `per-workflow` executions of
#       each (round-robin), and prints a per-workflow + fleet summary with honest throughput
#       based on the real task counts.
#
#   Engine HA:    every run preflights the HA stack — both engine instances (8082 + 8084),
#       the activity-results partition count (>= 4 for balanced consumption), and engine
#       state before/after the run. Pass --failover[=PORT] to additionally kill one engine
#       instance after the batch is started, let the survivor finish the run, then restart
#       the killed instance so the stack is left in full HA (default target: 8084).
#
# Requires the live stack (make up && make services — engine HA by default) + workers
# (make workers or scripts/start-fleet-workers.sh).
set -euo pipefail
cd "$(dirname "$0")/.."

DEF_URL=localhost:8081
ENG_URL=localhost:8082
ENG_HA_URL=localhost:8084
REG_URL=localhost:8083

# --failover[=PORT] is optional and position-independent; strip it before positional parsing.
FAILOVER=""
ARGS=()
for a in "$@"; do
    case "$a" in
        --failover) FAILOVER=8084 ;;
        --failover=*) FAILOVER=${a#--failover=} ;;
        *) ARGS+=("$a") ;;
    esac
done
set -- "${ARGS[@]}"

if [ "${1:-}" = "--fleet" ]; then
    MODE=fleet
    FLEET_SIZE=${2:-10}
    PER_WORKFLOW=${3:-10}
    CONCURRENCY=${4:-16}
else
    MODE=single
    WF_ID=${1:-}
    COUNT=${2:-1}
    CONCURRENCY=${3:-16}
    if [ -z "$WF_ID" ]; then
        echo "usage: demo-scale.sh <workflow-id> [count] [concurrency] [--failover[=PORT]]" >&2
        echo "       demo-scale.sh --fleet <fleet-size> [per-workflow] [concurrency] [--failover[=PORT]]" >&2
        exit 1
    fi
fi

WORK_DIR=$(mktemp -d /tmp/fuseflow-scale.XXXXXX)
IDS_FILE="$WORK_DIR/ids.txt"
RESP_DIR="$WORK_DIR/responses"
mkdir -p "$RESP_DIR"

# ------------------------------------------------------------------ fleet definition setup

declare -a WF_IDS=()
declare -a WF_NAMES=()
declare -a WF_TASKS=()
declare -a SHAPES=(chain diamond fanout parallel layered)

register_shape() {
    local i=$1 shape=$2
    local name="fuse-scale-$i-$shape"
    local body
    case "$shape" in
        chain)    body='[{"id":"download","activity":"downloadImage"},{"id":"resize","activity":"resizeImage","dependsOn":["download"]},{"id":"upload","activity":"uploadImage","dependsOn":["resize"]}]';;
        diamond)  body='[{"id":"download","activity":"downloadImage"},{"id":"resize","activity":"resizeImage","dependsOn":["download"]},{"id":"watermark","activity":"watermarkImage","dependsOn":["resize"]},{"id":"compress","activity":"compressImage","dependsOn":["resize"]},{"id":"upload","activity":"uploadImage","dependsOn":["watermark","compress"]}]';;
        fanout)   body='[{"id":"download","activity":"downloadImage"},{"id":"resize","activity":"resizeImage","dependsOn":["download"]},{"id":"watermark","activity":"watermarkImage","dependsOn":["download"]},{"id":"compress","activity":"compressImage","dependsOn":["download"]},{"id":"upload","activity":"uploadImage","dependsOn":["resize","watermark","compress"]}]';;
        parallel) body='[{"id":"download","activity":"downloadImage"},{"id":"resize","activity":"resizeImage","dependsOn":["download"]},{"id":"watermark","activity":"watermarkImage"},{"id":"compress","activity":"compressImage","dependsOn":["watermark"]},{"id":"upload","activity":"uploadImage","dependsOn":["resize","compress"]}]';;
        layered)  body='[{"id":"download","activity":"downloadImage"},{"id":"resize","activity":"resizeImage","dependsOn":["download"]},{"id":"watermark","activity":"watermarkImage","dependsOn":["resize"]},{"id":"compress","activity":"compressImage","dependsOn":["watermark"]},{"id":"upload","activity":"uploadImage","dependsOn":["compress"]}]';;
    esac

    # Idempotent upsert by name (Phase 1 model): reuse an existing definition with the same name.
    local existing
    existing=$(curl -s "$DEF_URL/api/v1/workflows" | python3 -c "
import sys, json
target = '$name'
for w in json.load(sys.stdin):
    if w.get('name') == target:
        print(w['id']); break
")
    if [ -n "$existing" ]; then
        WF_IDS+=("$existing")
    else
        local created
        created=$(curl -s -X POST "$DEF_URL/api/v1/workflows" -H 'Content-Type: application/json' \
            -d "{\"name\":\"$name\",\"description\":\"scale-$shape\",\"tasks\":$body}" \
            | python3 -c "import sys, json; print(json.load(sys.stdin).get('id',''))")
        WF_IDS+=("$created")
    fi
    WF_NAMES+=("$name")
    # Report the real task count of the registered/reused definition (a previous session may
    # have left a different DAG under the same name).
    WF_TASKS+=($(curl -s "$DEF_URL/api/v1/workflows/${WF_IDS[-1]}" | python3 -c "
import sys, json
print(len(json.load(sys.stdin).get('tasks', [])))" 2>/dev/null || echo 5))
}

if [ "$MODE" = "fleet" ]; then
    echo "=== registering $FLEET_SIZE fleet definition(s) (5 shapes, idempotent) ==="
    for i in $(seq 0 $((FLEET_SIZE - 1))); do
        register_shape "$i" "${SHAPES[$((i % 5))]}"
    done
else
    WF_NAMES+=("single")
    WF_IDS+=("$WF_ID")
    TASKS=$(curl -s "$DEF_URL/api/v1/workflows/$WF_ID" | python3 -c "
import sys, json
try:
    print(len(json.load(sys.stdin).get('tasks', [])))
except Exception:
    print(5)
")
    WF_TASKS+=(${TASKS:-5})
fi

# ---------------------------------------------------------------- preflight capability check

echo "=== preflight: worker coverage per activity ==="
ALL_ACTIVITIES="downloadImage resizeImage watermarkImage compressImage uploadImage"
for activity in $ALL_ACTIVITIES; do
    count=$(curl -s "$REG_URL/api/v1/workers/activities/$activity" | python3 -c "import sys, json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)
    if [ "$count" -eq 0 ]; then
        echo "  WARN: no worker advertises '$activity' — its tasks will stay SCHEDULED (ActivityUnroutable)"
    else
        echo "  OK: $activity -> $count worker(s)"
    fi
done

# ------------------------------------------------------------------- engine HA preflight

eng_status() {  # UP | DOWN for a port
    curl -s --max-time 2 "localhost:$1/actuator/health" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo DOWN
}
eng_get() {  # GET <path> from the first live engine (8082 primary, 8084 HA fallback)
    local path=$1
    for p in 8082 8084; do
        local body
        body=$(curl -s --max-time 3 "localhost:$p$path" 2>/dev/null || true)
        [ -n "$body" ] && { printf '%s' "$body"; return 0; }
    done
    return 1
}

ENG_A=$(eng_status 8082)
ENG_B=$(eng_status 8084)
echo
echo "=== engine HA preflight ==="
echo "  engine A (8082): $ENG_A   engine B (8084): $ENG_B"
partitions=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --describe --topic activity-results 2>/dev/null \
    | grep -o 'PartitionCount: [0-9]*' | grep -o '[0-9]*' | head -1 || true)
echo "  activity-results partitions: ${partitions:-?} (>= 4 needed for balanced HA consumption)"
if [ "$ENG_A" != "UP" ] || [ "$ENG_B" != "UP" ]; then
    echo "  WARN: engine HA is degraded (only one instance up) — the run will not exercise failover"
    FAILOVER=""
fi
if [ -n "$FAILOVER" ] && [ "$FAILOVER" != "8082" ] && [ "$FAILOVER" != "8084" ]; then
    echo "  WARN: unknown --failover target '$FAILOVER' (use 8082 or 8084) — defaulting to 8084"
    FAILOVER=8084
fi
if [ -n "$FAILOVER" ]; then
    echo "  --failover armed: engine on port $FAILOVER will be killed after the batch starts"
fi

# --------------------------------------------------------------- snapshot worker log baselines

log_marker() {
    # Track both the single `make workers` worker (/tmp/fuseflow-workers.log) and the fleet
    # workers (/tmp/fuseflow-workers-*.log) so the per-worker delta covers whichever is live.
    # Only log files still held open by a live process are included — a stale log from a
    # previous session's dead worker (e.g. media workers after the config went 8 io / 0 media)
    # would otherwise show up as a spurious 0-delta row in the summary.
    local logs=()
    if [ -f /tmp/fuseflow-workers.log ] && lsof /tmp/fuseflow-workers.log >/dev/null 2>&1; then
        logs+=(/tmp/fuseflow-workers.log)
    fi
    if ls /tmp/fuseflow-workers-*.log >/dev/null 2>&1; then
        for f in /tmp/fuseflow-workers-*.log; do
            if lsof "$f" >/dev/null 2>&1; then
                logs+=("$f")
            fi
        done
    fi
    if [ "${#logs[@]}" -gt 0 ]; then
        for f in "${logs[@]}"; do
            # grep -c prints "0" AND exits 1 when there are no matches, so `|| echo 0` would
            # emit a second line and misalign baseline.txt with logs.txt → the summary delta
            # arithmetic then reads the wrong/blank baseline. `|| true` keeps the single count.
            grep -c 'Executing ' "$f" 2>/dev/null || true
        done > "$WORK_DIR/baseline.txt"
        printf '%s\n' "${logs[@]}" > "$WORK_DIR/logs.txt"
    fi
}
log_marker

# ------------------------------------------------------------------------------- start executions

STARTED=0
echo
echo "=== starting executions (mode=$MODE, concurrency=$CONCURRENCY) ==="
# Pace the start: keep at most $CONCURRENCY executions in flight so the worker fleet's pool
# queue never outruns its drain rate (a task that sits in the queue past the engine's start
# timeout gets retried and eventually fails). Concurrency here is a rate cap, not parallelism.
IN_FLIGHT_FILE="$WORK_DIR/inflight.txt"
: > "$IN_FLIGHT_FILE"

# Fetch execution statuses in parallel (16-way). Replaces the old serial eng_get loop —
# polling 200 executions one curl at a time was the dominant cost of a fleet run's poll phase.
# one_status is exported so xargs subshells can call it without nested-quote gymnastics.
one_status() {  # $1=idx $2=id -> "<idx> <id> <status>" (first live engine)
    local idx=$1 id=$2 s="?" body
    for p in 8082 8084; do
        body=$(curl -s --max-time 3 "localhost:$p/api/v1/executions/$id" 2>/dev/null || true)
        if [ -n "$body" ]; then
            s=$(printf '%s' "$body" | grep -o '"status":"[A-Z]*"' | head -1 | cut -d'"' -f4)
            [ -n "$s" ] && break
        fi
    done
    printf '%s %s %s\n' "$idx" "$id" "${s:-?}"
}
export -f one_status

fetch_statuses() {  # reads "<idx> <id>" lines on stdin, prints "<idx> <id> <status>"
    xargs -P 16 -n 2 bash -c 'one_status "$1" "$2"' _
}

in_flight() {  # count started executions that have not reached a terminal state yet
    awk '{print "0 " $0}' "$IN_FLIGHT_FILE" \
        | xargs -P 16 -n 2 bash -c 'one_status "$1" "$2"' _ \
        | awk '$3 != "COMPLETED" && $3 != "FAILED" {n++} END {print n+0}'
}

pace() {  # block until fewer than CONCURRENCY executions are in flight
    while [ "$(in_flight)" -ge "$CONCURRENCY" ]; do
        sleep 2
    done
}

if [ "$MODE" = "fleet" ]; then
    for round in $(seq 1 "$PER_WORKFLOW"); do
        # Pace ONCE per round (FLEET_SIZE starts), not before every single start: in_flight()
        # re-polls the whole growing id set, so calling it 200× (once per execution) turns into
        # ~40k HTTP calls and dominates wall time. Per-round pacing still caps in-flight at
        # CONCURRENCY + FLEET_SIZE, which is enough to protect the pool queue.
        pace
        for i in $(seq 0 $((FLEET_SIZE - 1))); do
            f="$RESP_DIR/wf$i-$round.json"
            curl -s -X POST "$ENG_URL/api/v1/executions" -H 'Content-Type: application/json' \
                -d "{\"workflowId\": \"${WF_IDS[$i]}\", \"input\": {\"batch\": $round}}" > "$f"
            id=$(python3 -c "import sys, json; print(json.load(open('$f')).get('id',''))" 2>/dev/null || true)
            [ -n "$id" ] && echo "$id" >> "$IN_FLIGHT_FILE"
            STARTED=$((STARTED + 1))
        done
    done
else
    for i in $(seq 1 "$COUNT"); do
        pace
        f="$RESP_DIR/i$i.json"
        curl -s -X POST "$ENG_URL/api/v1/executions" -H 'Content-Type: application/json' \
            -d "{\"workflowId\": \"$WF_ID\", \"input\": {\"run\": $i}}" > "$f"
        id=$(python3 -c "import sys, json; print(json.load(open('$f')).get('id',''))" 2>/dev/null || true)
        [ -n "$id" ] && echo "$id" >> "$IN_FLIGHT_FILE"
        STARTED=$((STARTED + 1))
    done
fi
echo "started $STARTED execution(s)"

# ------------------------------------------------------------- engine HA failover (--failover)

if [ -n "$FAILOVER" ]; then
    pid=$(lsof -ti tcp:"$FAILOVER" 2>/dev/null || true)
    if [ -n "$pid" ]; then
        echo "=== --failover: killing engine on port $FAILOVER (pid $pid) mid-run ==="
        kill "$pid"
        # Give the consumer group a moment to rebalance before polling starts.
        sleep 5
        echo "  killed; the survivor finishes the batch (polls fall back to any live engine)"
    else
        echo "WARN: no process on port $FAILOVER — continuing without failover"
        FAILOVER=""
    fi
fi

# collect ids (single mode: bare id; fleet mode: "<wf-index> <id>" so per-workflow results work)
: > "$IDS_FILE"
if [ "$MODE" = "fleet" ]; then
    for i in $(seq 0 $((FLEET_SIZE - 1))); do
        for round in $(seq 1 "$PER_WORKFLOW"); do
            id=$(python3 -c "import sys, json; print(json.load(open('$RESP_DIR/wf$i-$round.json')).get('id',''))" 2>/dev/null || true)
            [ -n "$id" ] && echo "$i $id" >> "$IDS_FILE"
        done
    done
else
    # Poll loop reads "<idx> <id>" pairs; single mode has no per-workflow index, so use 0.
    for f in "$RESP_DIR"/*.json; do
        id=$(python3 -c "import sys, json; print(json.load(open('$f')).get('id',''))" 2>/dev/null || true)
        [ -n "$id" ] && echo "0 $id" >> "$IDS_FILE"
    done
fi
STARTED=$(wc -l < "$IDS_FILE" | tr -d ' ')

# ------------------------------------------------------------------------------------- polling

POLL_DEADLINE=${POLL_DEADLINE:-600}
deadline=$((SECONDS + POLL_DEADLINE))
COMPLETED=0
FAILED=0
declare -a FAILED_IDS=()
if [ "$MODE" = "fleet" ]; then
    declare -a WF_COMPLETED=()
    declare -a WF_FAILED=()
    for i in $(seq 0 $((FLEET_SIZE - 1))); do
        WF_COMPLETED+=("0")
        WF_FAILED+=("0")
    done
fi

while :; do
    RUNNING=0
    COMPLETED=0
    FAILED=0
    FAILED_IDS=()
    if [ "$MODE" = "fleet" ]; then
        for i in $(seq 0 $((FLEET_SIZE - 1))); do WF_COMPLETED[$i]=0; WF_FAILED[$i]=0; done
    fi
    while read -r idx id status; do
        case "$status" in
            COMPLETED)
                COMPLETED=$((COMPLETED + 1))
                [ "$MODE" = "fleet" ] && WF_COMPLETED[$idx]=$((WF_COMPLETED[$idx] + 1));;
            FAILED)
                FAILED=$((FAILED + 1)); FAILED_IDS+=("$id")
                [ "$MODE" = "fleet" ] && WF_FAILED[$idx]=$((WF_FAILED[$idx] + 1));;
            *) RUNNING=$((RUNNING + 1));;
        esac
    done < <(fetch_statuses < "$IDS_FILE")
    if [ "$RUNNING" -eq 0 ]; then
        break
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "WARN: poll deadline (${POLL_DEADLINE}s) reached with $RUNNING execution(s) still running" >&2
        break
    fi
    sleep 2
done

# -------------------------------------------------------------------------------------- summary

ELAPSED=$SECONDS
TOTAL_TASKS=0
if [ "$MODE" = "fleet" ]; then
    for i in $(seq 0 $((FLEET_SIZE - 1))); do
        TOTAL_TASKS=$((TOTAL_TASKS + WF_TASKS[$i] * PER_WORKFLOW))
    done
else
    TOTAL_TASKS=$((WF_TASKS[0] * COUNT))
fi

echo
echo "=== summary ==="
if [ "$MODE" = "fleet" ]; then
    printf "  %-28s %8s %10s %10s %10s\n" "workflow" "started" "completed" "failed" "tasks"
    for i in $(seq 0 $((FLEET_SIZE - 1))); do
        printf "  %-28s %8s %10s %10s %10s\n" "${WF_NAMES[$i]}" "$PER_WORKFLOW" "${WF_COMPLETED[$i]}" "${WF_FAILED[$i]}" "$((WF_TASKS[$i] * PER_WORKFLOW))"
    done
fi
echo "  started=$STARTED completed=$COMPLETED failed=$FAILED elapsed=${ELAPSED}s"

if [ "$ELAPSED" -gt 0 ] && [ "$TOTAL_TASKS" -gt 0 ]; then
    echo "  throughput: $((STARTED / ELAPSED)) exec/sec | $((TOTAL_TASKS / ELAPSED)) activities/sec"
fi

# worker-side view: this run's delta only (baseline snapshot taken before starting)
if [ -f "$WORK_DIR/logs.txt" ]; then
    echo
    echo "  per-worker activity delta this run:"
    i=0
    while read -r logfile; do
        baseline=$(sed -n "$((i + 1))p" "$WORK_DIR/baseline.txt")
        now=$(grep -c 'Executing ' "$logfile" 2>/dev/null || true)
        delta=$((now - baseline))
        echo "    $(basename "$logfile" .log): $delta"
        i=$((i + 1))
    done < "$WORK_DIR/logs.txt"
fi

# ------------------------------------------------------------------ engine HA post-run

if [ -n "$FAILOVER" ]; then
    # Restore the stack to full HA: relaunch the killed instance with the same env the start
    # scripts use (shard split, listener concurrency, partitions, per-instance worker-events group).
    JAR=$(ls fuseflow-workflow-engine/target/fuseflow-workflow-engine-*.jar 2>/dev/null | head -1 || true)
    if [ -n "$JAR" ]; then
        if [ "$FAILOVER" = "8082" ]; then
            ./scripts/daemon-java.sh "$JAR" /tmp/fuseflow-engine.log \
                FUSEFLOW_ENGINE_OWNED_SHARDS=0-3 FUSEFLOW_ENGINE_LISTENER_CONCURRENCY=2 \
                FUSEFLOW_KAFKA_TOPICS_PARTITIONS=4 FUSEFLOW_ENGINE_WORKER_EVENTS_GROUP=fuseflow-engine-events-a
        else
            ./scripts/daemon-java.sh "$JAR" /tmp/fuseflow-engine-2.log \
                SERVER_PORT=8084 FUSEFLOW_ENGINE_OWNED_SHARDS=4-7 FUSEFLOW_ENGINE_LISTENER_CONCURRENCY=2 \
                FUSEFLOW_KAFKA_TOPICS_PARTITIONS=4 FUSEFLOW_ENGINE_WORKER_EVENTS_GROUP=fuseflow-engine-events-b
        fi
        for i in $(seq 1 30); do
            [ "$(eng_status "$FAILOVER")" = "UP" ] && break
            sleep 2
        done
        if [ "$(eng_status "$FAILOVER")" = "UP" ]; then
            echo "  failover: killed port $FAILOVER mid-run, survivor completed the batch, engine restarted (full HA restored)"
        else
            echo "  WARN: failover engine on port $FAILOVER did not come back UP after restart" >&2
        fi
    else
        echo "  WARN: engine jar missing — cannot restart the killed instance (run 'make build')" >&2
    fi
fi
echo "  engines after run: A=$(eng_status 8082) B=$(eng_status 8084)"

rm -rf "$WORK_DIR"

if [ "$FAILED" -gt 0 ]; then
    echo "ERROR: $FAILED execution(s) failed: ${FAILED_IDS[*]}" >&2
    exit 1
fi
if [ "$COMPLETED" -lt "$STARTED" ]; then
    echo "ERROR: $((STARTED - COMPLETED)) execution(s) did not complete (routing/timing issue?)" >&2
    exit 1
fi
echo "OK"
