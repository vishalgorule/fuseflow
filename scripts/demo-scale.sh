#!/usr/bin/env bash
# FuseFlow scale demo (Phase 5 — pool-based routing).
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
# Requires the live stack (make up && make services) + workers (make workers or
# scripts/start-fleet-workers.sh).
set -euo pipefail

DEF_URL=localhost:8081
ENG_URL=localhost:8082
REG_URL=localhost:8083

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
        echo "usage: demo-scale.sh <workflow-id> [count] [concurrency]" >&2
        echo "       demo-scale.sh --fleet <fleet-size> [per-workflow] [concurrency]" >&2
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

# --------------------------------------------------------------- snapshot worker log baselines

log_marker() {
    if ls /tmp/fuseflow-workers-*.log >/dev/null 2>&1; then
        for f in /tmp/fuseflow-workers-*.log; do
            grep -c 'Executing ' "$f" 2>/dev/null || echo 0
        done > "$WORK_DIR/baseline.txt"
        ls /tmp/fuseflow-workers-*.log > "$WORK_DIR/logs.txt"
    fi
}
log_marker

# ------------------------------------------------------------------------------- start executions

STARTED=0
echo
echo "=== starting executions (mode=$MODE, concurrency=$CONCURRENCY) ==="
if [ "$MODE" = "fleet" ]; then
    for round in $(seq 1 "$PER_WORKFLOW"); do
        for i in $(seq 0 $((FLEET_SIZE - 1))); do
            f="$RESP_DIR/wf$i-$round.json"
            curl -s -X POST "$ENG_URL/api/v1/executions" -H 'Content-Type: application/json' \
                -d "{\"workflowId\": \"${WF_IDS[$i]}\", \"input\": {\"batch\": $round}}" > "$f"
            STARTED=$((STARTED + 1))
        done
    done
else
    for i in $(seq 1 "$COUNT"); do
        f="$RESP_DIR/i$i.json"
        curl -s -X POST "$ENG_URL/api/v1/executions" -H 'Content-Type: application/json' \
            -d "{\"workflowId\": \"$WF_ID\", \"input\": {\"run\": $i}}" > "$f"
        STARTED=$((STARTED + 1))
    done
fi
echo "started $STARTED execution(s)"

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
    for f in "$RESP_DIR"/*.json; do
        id=$(python3 -c "import sys, json; print(json.load(open('$f')).get('id',''))" 2>/dev/null || true)
        [ -n "$id" ] && echo "$id" >> "$IDS_FILE"
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
    while read -r idx id; do
        status=$(curl -s "$ENG_URL/api/v1/executions/$id" | python3 -c "import sys, json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "?")
        case "$status" in
            COMPLETED)
                COMPLETED=$((COMPLETED + 1))
                [ "$MODE" = "fleet" ] && WF_COMPLETED[$idx]=$((WF_COMPLETED[$idx] + 1));;
            FAILED)
                FAILED=$((FAILED + 1)); FAILED_IDS+=("$id")
                [ "$MODE" = "fleet" ] && WF_FAILED[$idx]=$((WF_FAILED[$idx] + 1));;
            *) RUNNING=$((RUNNING + 1));;
        esac
    done < "$IDS_FILE"
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
        now=$(grep -c 'Executing ' "$logfile" 2>/dev/null || echo 0)
        delta=$((now - baseline))
        echo "    $(basename "$logfile" .log): $delta"
        i=$((i + 1))
    done < "$WORK_DIR/logs.txt"
fi

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
