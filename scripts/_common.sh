#!/usr/bin/env bash
# Shared setup sourced by every script in this folder.
# Works with macOS's default bash 3.2 (no bash-4-only features).

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$PROJECT_ROOT/.venv"
RUN_DIR="$PROJECT_ROOT/run"          # PID files
LOG_DIR="$PROJECT_ROOT/logs"         # master / worker / history server logs
WORK_DIR="$PROJECT_ROOT/work"        # worker scratch dirs; executor stdout/stderr live here
EVENT_DIR="$PROJECT_ROOT/spark-events"

# Load KEY=VALUE pairs from conf/cluster.env
set -a
# shellcheck disable=SC1091
. "$PROJECT_ROOT/conf/cluster.env"
set +a

# --- Java -------------------------------------------------------------------
java_major() {
  # Prints the major version of the java binary given as $1 (e.g. 17, 21, 8)
  local v
  v=$("$1" -version 2>&1 | grep -i 'version' | head -1 | sed -E 's/.*version "([^"]+)".*/\1/')
  case "$v" in
    1.*) echo "$v" | cut -d. -f2 ;;     # 1.8.0_x -> 8
    *)   echo "$v" | cut -d. -f1 ;;     # 17.0.9 -> 17
  esac
}

pick_java_home() {
  # Respect an existing JAVA_HOME if it points to Java 17 or 21.
  if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    case "$(java_major "$JAVA_HOME/bin/java")" in 17|21) return 0 ;; esac
  fi
  # macOS: ask the system for a registered JDK 17 or 21
  if [ -x /usr/libexec/java_home ]; then
    for v in 17 21; do
      local h
      h=$(/usr/libexec/java_home -v "$v" 2>/dev/null) || continue
      if [ -n "$h" ] && [ "$(java_major "$h/bin/java")" = "$v" ]; then
        export JAVA_HOME="$h"; return 0
      fi
    done
  fi
  # Homebrew keg-only installs (Apple Silicon and Intel locations)
  for h in /opt/homebrew/opt/openjdk@17 /opt/homebrew/opt/openjdk@21 \
           /usr/local/opt/openjdk@17 /usr/local/opt/openjdk@21; do
    if [ -x "$h/bin/java" ]; then export JAVA_HOME="$h"; return 0; fi
  done
  # Fall back to whatever `java` is on PATH
  if command -v java >/dev/null 2>&1; then
    case "$(java_major "$(command -v java)")" in
      17|21) export JAVA_HOME="$(cd "$(dirname "$(readlink -f "$(command -v java)" 2>/dev/null || command -v java)")/.." && pwd)"; return 0 ;;
    esac
  fi
  return 1
}

# --- Spark ------------------------------------------------------------------
activate_venv() {
  if [ ! -x "$VENV/bin/python" ]; then
    echo "ERROR: virtual environment not found. Run ./scripts/setup.sh first." >&2
    exit 1
  fi
  # shellcheck disable=SC1091
  . "$VENV/bin/activate"
  # pip-installed PySpark ships the full Spark runtime (jars + bin/spark-class).
  SPARK_HOME="$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
  export SPARK_HOME
  export PYSPARK_PYTHON="$VENV/bin/python"
  export PYSPARK_DRIVER_PYTHON="$VENV/bin/python"
  # Bind everything to loopback: avoids macOS hostname / firewall problems.
  export SPARK_LOCAL_IP=127.0.0.1
  export SPARK_LOG_DIR="$LOG_DIR"
  # Heap cap for master / worker / History Server JVMs (from conf/cluster.env)
  export SPARK_DAEMON_MEMORY="${SPARK_DAEMON_MEMORY:-512m}"
}

wait_for_url() {
  # wait_for_url <url> <name> : poll until the HTTP endpoint answers (max ~40s)
  local i=0
  while [ $i -lt 40 ]; do
    if curl -s -o /dev/null "$1"; then echo "  $2 is up: $1"; return 0; fi
    sleep 1; i=$((i+1))
  done
  echo "  WARNING: $2 did not come up at $1 -- check logs in $LOG_DIR" >&2
  return 1
}

is_running() {
  # is_running <pidfile>
  [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null
}
