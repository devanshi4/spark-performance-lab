#!/usr/bin/env bash
# Checks that Python and Java versions are compatible with Spark 4.0.
#   Spark 4.0 requires Java 17 or 21 (Java 8 and 11 are no longer supported).
#   PySpark 4.0 requires Python 3.9+. This lab is tested with 3.11-3.13.
. "$(dirname "$0")/_common.sh"

ok=1
echo "== Operating system"
uname -sm

echo
echo "== Python"
PY=""
for c in python3.12 python3.11 python3.13 python3; do
  if command -v "$c" >/dev/null 2>&1; then PY="$c"; break; fi
done
if [ -z "$PY" ]; then
  echo "  NOT FOUND. Install with: brew install python@3.12"; ok=0
else
  pyver=$("$PY" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])')
  echo "  $PY -> $pyver"
  minor=$(echo "$pyver" | cut -d. -f2)
  if [ "$(echo "$pyver" | cut -d. -f1)" != "3" ] || [ "$minor" -lt 9 ]; then
    echo "  PROBLEM: PySpark 4.0 needs Python 3.9 or newer."; ok=0
  elif [ "$minor" -gt 13 ]; then
    echo "  WARNING: Python $pyver is newer than what PySpark 4.0 was tested with. Prefer 3.12."
  else
    echo "  OK"
  fi
fi

echo
echo "== Java"
if pick_java_home; then
  echo "  JAVA_HOME=$JAVA_HOME"
  echo "  major version: $(java_major "$JAVA_HOME/bin/java")  -> OK (Spark 4.0 supports 17 and 21)"
else
  found=""
  command -v java >/dev/null 2>&1 && found=$(java_major "$(command -v java)")
  if [ -n "$found" ]; then
    echo "  Found Java $found on PATH, but Spark 4.0 needs Java 17 or 21."
  else
    # On macOS /usr/bin/java always exists, but it is only a placeholder until a JDK is installed.
    echo "  No Java installed (on macOS, /usr/bin/java is only a placeholder until a JDK is installed)."
  fi
  echo "  Install Java 17, either:"
  echo "    a) Homebrew:  brew install openjdk@17"
  echo "                  sudo ln -sfn \$(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk"
  echo "    b) Installer: https://adoptium.net/temurin/releases/?version=17&os=mac  (choose the .pkg for your CPU: aarch64 = Apple Silicon)"
  ok=0
fi

echo
echo "== Hardware"
if [ "$(uname -s)" = "Darwin" ]; then
  echo "  CPU cores (logical): $(sysctl -n hw.ncpu)"
  echo "  RAM: $(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 )) GB"
else
  echo "  CPU cores: $(nproc)"
  echo "  RAM: $(free -g | awk '/Mem:/ {print $2}') GB"
fi
echo "  Lab cluster: $NUM_WORKERS executors x $EXECUTOR_CORES cores x $EXECUTOR_MEMORY heap, driver $DRIVER_MEMORY (see conf/cluster.env)"

echo
if [ -x "$VENV/bin/python" ]; then
  echo "== PySpark in .venv: $("$VENV/bin/python" -c 'import pyspark; print(pyspark.__version__)' 2>/dev/null || echo 'not installed')"
else
  echo "== .venv not created yet -> run ./scripts/setup.sh"
fi

echo
[ $ok = 1 ] && echo "Environment looks good." || { echo "Fix the problems above before continuing."; exit 1; }
