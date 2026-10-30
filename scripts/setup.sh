#!/usr/bin/env bash
# One-time setup: create .venv and install PySpark + helpers.
set -e
. "$(dirname "$0")/_common.sh"

PY=""
for c in python3.12 python3.11 python3.13 python3; do
  if command -v "$c" >/dev/null 2>&1; then PY="$c"; break; fi
done
[ -z "$PY" ] && { echo "Python 3 not found. brew install python@3.12"; exit 1; }

echo "Creating virtual environment with $PY ..."
"$PY" -m venv "$VENV"
"$VENV/bin/pip" install --upgrade pip >/dev/null
"$VENV/bin/pip" install -r "$PROJECT_ROOT/requirements.txt"

mkdir -p "$RUN_DIR" "$LOG_DIR" "$WORK_DIR" "$EVENT_DIR" "$PROJECT_ROOT/data" "$PROJECT_ROOT/output"
echo
"$(dirname "$0")/check_env.sh"
