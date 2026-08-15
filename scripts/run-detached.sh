#!/usr/bin/env bash
# Launches a Spring Boot jar detached with optional env overrides, logging to a file.
# Usage: run-detached.sh <jar> <log-file> [ENV=value ...]
# Prints the PID of the launched process.
set -euo pipefail

JAR=$1
LOG=$2
shift 2

if [ ! -f "$JAR" ]; then
    echo "ERROR: jar not found: $JAR" >&2
    exit 1
fi

ENV_ARGS=()
for kv in "$@"; do
    ENV_ARGS+=("$kv")
done

nohup env "${ENV_ARGS[@]}" java -jar "$JAR" > "$LOG" 2>&1 &
PID=$!
echo "$PID"
