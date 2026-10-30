"""Creates a SparkSession connected to the local standalone cluster."""
from __future__ import annotations
import socket
import sys

from pyspark.sql import SparkSession

from conf import settings


def _master_is_up() -> bool:
    try:
        with socket.create_connection((settings.MASTER_HOST, int(settings.CLUSTER["SPARK_MASTER_PORT"])), timeout=2):
            return True
    except OSError:
        return False


def build_session(app_name: str, extra_conf: dict | None = None) -> SparkSession:
    """Start a Spark application on the standalone cluster.

    What happens when this runs:
      1. Python starts a JVM on your machine. That JVM is the DRIVER: it plans the work.
      2. The driver registers with the master at spark://127.0.0.1:7077 and asks for cores/memory.
      3. The master tells each worker to start an EXECUTOR JVM (2 cores, 1 GB heap each by default).
      4. Executors connect back to the driver and wait for tasks.
    Nothing reads data yet. Data is only read when an action (count, write, collect) runs.
    """
    if not _master_is_up():
        sys.exit(f"Spark master not reachable at {settings.MASTER_URL}. Run ./scripts/start_cluster.sh first.")

    settings.EVENT_LOG_DIR.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.MASTER_URL)
        # --- resources this application asks the cluster for
        .config("spark.driver.memory", settings.DRIVER_MEMORY)
        .config("spark.executor.memory", settings.EXECUTOR_MEMORY)
        .config("spark.executor.cores", settings.EXECUTOR_CORES)
        .config("spark.cores.max", settings.TOTAL_CORES)
        # --- networking: keep everything on loopback
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # --- event log: every UI event is written to spark-events/ so the History Server
        #     can show this application after it finishes
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{settings.EVENT_LOG_DIR}")
        # --- same Python on driver and executors
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        # --- keep more UI history than default so nothing disappears from the UI
        .config("spark.ui.retainedTasks", "200000")
        .config("spark.sql.ui.retainedExecutions", "200")
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, str(value))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
