#!/usr/bin/env bash
# Stops the History Server, workers and master started by start_cluster.sh.
. "$(dirname "$0")/_common.sh"

stop_pid() {
  local f="$1" name="$2"
  if is_running "$f"; then
    local pid; pid=$(cat "$f")
    echo "Stopping $name (pid $pid)"
    kill "$pid" 2>/dev/null
    local i=0
    while kill -0 "$pid" 2>/dev/null && [ $i -lt 15 ]; do sleep 1; i=$((i+1)); done
    kill -0 "$pid" 2>/dev/null && { echo "  force killing $name"; kill -9 "$pid" 2>/dev/null; }
  fi
  rm -f "$f"
}

stop_pid "$RUN_DIR/history.pid" "History Server"
for f in "$RUN_DIR"/worker-*.pid; do
  [ -e "$f" ] && stop_pid "$f" "$(basename "$f" .pid)"
done
stop_pid "$RUN_DIR/master.pid" "Master"
echo "Cluster stopped. Event logs are kept in spark-events/ for next time."
