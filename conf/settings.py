"""Central configuration for the lab.

Reads conf/cluster.env so the bash scripts and the Python jobs always agree on
ports, core counts and memory sizes.
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"            # generated input datasets (Parquet)
OUTPUT_DIR = PROJECT_ROOT / "output"        # results written by the jobs
EVENT_LOG_DIR = PROJECT_ROOT / "spark-events"  # read by the History Server
RESULTS_DIR = PROJECT_ROOT / "results"      # raw metrics JSON captured from Spark's REST API
REPORTS_DIR = PROJECT_ROOT / "reports"      # human-readable comparison reports


def _load_env(path: Path) -> dict:
    values = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


CLUSTER = _load_env(PROJECT_ROOT / "conf" / "cluster.env")

MASTER_HOST = CLUSTER["SPARK_MASTER_HOST"]
MASTER_URL = f"spark://{MASTER_HOST}:{CLUSTER['SPARK_MASTER_PORT']}"
MASTER_UI_URL = f"http://{MASTER_HOST}:{CLUSTER['SPARK_MASTER_WEBUI_PORT']}"
HISTORY_UI_URL = f"http://{MASTER_HOST}:{CLUSTER['HISTORY_UI_PORT']}"

NUM_WORKERS = int(CLUSTER["NUM_WORKERS"])
WORKER_CORES = int(CLUSTER["WORKER_CORES"])
EXECUTOR_CORES = int(CLUSTER["EXECUTOR_CORES"])
EXECUTOR_MEMORY = CLUSTER["EXECUTOR_MEMORY"]
DRIVER_MEMORY = CLUSTER["DRIVER_MEMORY"]

# Total task slots available to one application.
TOTAL_CORES = NUM_WORKERS * WORKER_CORES
