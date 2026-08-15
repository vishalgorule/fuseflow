#!/usr/bin/env bash
# Launches the 4 platform services detached (logs in /tmp/fuseflow-*.log) and waits, keeping
# them alive for the lifetime of this process. Run it in the background:
#   scripts/start-services.sh &    (or via a background terminal session)
set -euo pipefail
cd "$(dirname "$0")/.."

for m in fuseflow-api-gateway fuseflow-definition-service fuseflow-workflow-engine fuseflow-worker-registry; do
    ls "$m"/target/"$m"-*.jar >/dev/null 2>&1 || { echo "ERROR: missing $m jar — run 'make build' first"; exit 1; }
done

nohup java -jar fuseflow-api-gateway/target/fuseflow-api-gateway-*.jar > /tmp/fuseflow-gateway.log 2>&1 &
nohup java -jar fuseflow-definition-service/target/fuseflow-definition-service-*.jar > /tmp/fuseflow-definition.log 2>&1 &
nohup java -jar fuseflow-workflow-engine/target/fuseflow-workflow-engine-*.jar > /tmp/fuseflow-engine.log 2>&1 &
nohup java -jar fuseflow-worker-registry/target/fuseflow-worker-registry-*.jar > /tmp/fuseflow-registry.log 2>&1 &

echo "launched; waiting (Ctrl-C or kill to stop the services)"

# Keep this process alive so the detached children are never reaped by a returning shell.
wait
