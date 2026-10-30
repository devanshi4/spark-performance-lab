#!/usr/bin/env bash
# Runs any lab module with the right Python, JAVA_HOME and working directory.
# Usage examples:
#   ./scripts/run.sh data_generation.generate_exp01_data
#   ./scripts/run.sh spark_jobs.before.exp01_skewed_join
#   ./scripts/run.sh spark_jobs.before.exp01_skewed_join --hold
set -e
. "$(dirname "$0")/_common.sh"
activate_venv
pick_java_home || { echo "Java 17 or 21 not found. Run ./scripts/check_env.sh"; exit 1; }
export JAVA_HOME
cd "$PROJECT_ROOT"
module="$1"; shift
exec python -m "$module" "$@"
