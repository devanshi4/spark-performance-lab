#!/usr/bin/env bash
# Shows which cluster processes are running.
. "$(dirname "$0")/_common.sh"
for f in "$RUN_DIR"/master.pid "$RUN_DIR"/worker-*.pid "$RUN_DIR"/history.pid; do
  [ -e "$f" ] || continue
  if is_running "$f"; then echo "RUNNING  $(basename "$f" .pid) (pid $(cat "$f"))"
  else echo "STOPPED  $(basename "$f" .pid) (stale pid file)"; fi
done
[ -e "$RUN_DIR/master.pid" ] || echo "Cluster not started. Run ./scripts/start_cluster.sh"
