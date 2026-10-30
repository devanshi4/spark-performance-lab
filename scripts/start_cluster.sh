#!/usr/bin/env bash
# Starts: 1 Spark master, NUM_WORKERS workers, and the History Server.
# Each process is a separate JVM running in the background. PIDs go to run/, logs to logs/.
set -e
. "$(dirname "$0")/_common.sh"
activate_venv
pick_java_home || { echo "Java 17 or 21 not found. Run ./scripts/check_env.sh"; exit 1; }
export JAVA_HOME

mkdir -p "$RUN_DIR" "$LOG_DIR" "$WORK_DIR" "$EVENT_DIR"
CLASS="$SPARK_HOME/bin/spark-class"
MASTER_URL="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"

echo "Using JAVA_HOME=$JAVA_HOME"
echo "Using SPARK_HOME=$SPARK_HOME"
echo

# ---- Master: keeps the list of workers and hands their cores/memory to applications
if is_running "$RUN_DIR/master.pid"; then
  echo "Master already running (pid $(cat "$RUN_DIR/master.pid"))"
else
  echo "Starting master ..."
  nohup "$CLASS" org.apache.spark.deploy.master.Master \
    --host "$SPARK_MASTER_HOST" --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_MASTER_WEBUI_PORT" \
    > "$LOG_DIR/master.log" 2>&1 &
  echo $! > "$RUN_DIR/master.pid"
fi
wait_for_url "http://$SPARK_MASTER_HOST:$SPARK_MASTER_WEBUI_PORT" "Master UI"

# ---- Workers: each one launches executor JVMs for applications
i=1
while [ $i -le "$NUM_WORKERS" ]; do
  pidf="$RUN_DIR/worker-$i.pid"
  port=$((WORKER_WEBUI_BASE_PORT + i - 1))
  if is_running "$pidf"; then
    echo "Worker $i already running (pid $(cat "$pidf"))"
  else
    echo "Starting worker $i ($WORKER_CORES cores, $WORKER_MEMORY) ..."
    mkdir -p "$WORK_DIR/worker-$i"
    nohup "$CLASS" org.apache.spark.deploy.worker.Worker \
      --host "$SPARK_MASTER_HOST" --cores "$WORKER_CORES" --memory "$WORKER_MEMORY" \
      --webui-port "$port" --work-dir "$WORK_DIR/worker-$i" "$MASTER_URL" \
      > "$LOG_DIR/worker-$i.log" 2>&1 &
    echo $! > "$pidf"
  fi
  wait_for_url "http://$SPARK_MASTER_HOST:$port" "Worker $i UI"
  i=$((i+1))
done

# ---- History Server: reads event logs of finished applications from spark-events/
if is_running "$RUN_DIR/history.pid"; then
  echo "History Server already running (pid $(cat "$RUN_DIR/history.pid"))"
else
  echo "Starting History Server ..."
  SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://$EVENT_DIR \
    -Dspark.history.ui.port=$HISTORY_UI_PORT \
    -Dspark.history.fs.update.interval=5s" \
  nohup "$CLASS" org.apache.spark.deploy.history.HistoryServer \
    > "$LOG_DIR/history.log" 2>&1 &
  echo $! > "$RUN_DIR/history.pid"
fi
wait_for_url "http://$SPARK_MASTER_HOST:$HISTORY_UI_PORT" "History Server"

echo
echo "Cluster is ready."
echo "  Master UI (workers, running apps):  http://$SPARK_MASTER_HOST:$SPARK_MASTER_WEBUI_PORT"
echo "  Worker UIs (executor logs):         http://$SPARK_MASTER_HOST:$WORKER_WEBUI_BASE_PORT ..."
echo "  Live application UI (while a job runs): http://$SPARK_MASTER_HOST:4040"
echo "  History Server (finished apps):      http://$SPARK_MASTER_HOST:$HISTORY_UI_PORT"
