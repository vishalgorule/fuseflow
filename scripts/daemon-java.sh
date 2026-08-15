#!/usr/bin/env bash
# Detached Java launcher: double-forks + setsids the JVM so it survives the calling shell
# (nohup alone is not enough when the parent shell is torn down with its process group).
#
# Usage: daemon-java.sh <jar> <log-file> [ENV=value ...]
#   optional ENV=value pairs are applied to the JVM environment.
set -euo pipefail

JAR=$1
LOG=$2
shift 2

if [ ! -f "$JAR" ]; then
    echo "ERROR: jar not found: $JAR" >&2
    exit 1
fi

export FUSEFLOW_DAEMON_JAR="$JAR"
export FUSEFLOW_DAEMON_LOG="$LOG"
export FUSEFLOW_DAEMON_ENV="$*"

python3 - <<'PYEOF'
import os
import sys

jar = os.environ["FUSEFLOW_DAEMON_JAR"]
log_path = os.environ["FUSEFLOW_DAEMON_LOG"]

# Apply env overrides (space-separated KEY=value pairs) to the child environment.
for pair in filter(None, os.environ.get("FUSEFLOW_DAEMON_ENV", "").split(" ")):
    key, _, value = pair.partition("=")
    if key:
        os.environ[key] = value

# First fork: detach from the calling shell's process group.
pid = os.fork()
if pid > 0:
    sys.exit(0)

# New session: immune to group-wide teardown of the original session.
os.setsid()

# Second fork: the JVM is not a session leader (no controlling terminal issues).
pid2 = os.fork()
if pid2 > 0:
    sys.exit(0)

with open(log_path, "a") as log:
    os.dup2(log.fileno(), 1)
    os.dup2(log.fileno(), 2)
    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)

os.execvp("java", ["java", "-jar", jar])
PYEOF
