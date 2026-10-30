"""Runs one experiment variant: start app -> run workload -> capture real metrics -> stop.

Every job file in spark_jobs/before and spark_jobs/optimized calls run_experiment().
"""
from __future__ import annotations

import argparse
import json
import time

from conf import settings
from spark_jobs.common import metrics
from spark_jobs.common.session import build_session


def run_experiment(experiment: str, variant: str, workload, extra_conf: dict | None = None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--hold", action="store_true",
                        help="keep the application alive after the workload so you can browse the LIVE UI on :4040")
    args = parser.parse_args()

    spark = build_session(f"{experiment}__{variant}", extra_conf)
    sc = spark.sparkContext
    print("=" * 78)
    print(f"Experiment : {experiment}")
    print(f"Variant    : {variant}")
    print(f"App ID     : {sc.applicationId}")
    print(f"Live UI    : {sc.uiWebUrl}   (only while this app runs)")
    print(f"History    : {settings.HISTORY_UI_URL}/history/{sc.applicationId}/jobs/   (after it finishes)")
    print("=" * 78)

    start = time.time()
    workload(spark)                    # <-- the actual Spark work happens here
    elapsed = time.time() - start
    print(f"\nWorkload finished in {elapsed:.1f} s (wall clock, measured in the driver)")

    result = metrics.collect(spark, experiment, variant, elapsed)
    out_dir = settings.RESULTS_DIR / experiment
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{variant}__{sc.applicationId}.json"
    out_file.write_text(json.dumps(result, indent=2))
    print(f"Metrics saved to {out_file.relative_to(settings.PROJECT_ROOT)}")

    if args.hold:
        input(f"\nApplication still running. Browse {sc.uiWebUrl} now. Press Enter to stop it... ")
    spark.stop()
    print(f"Done. Open {settings.HISTORY_UI_URL} to review application {result['app_id']}.")
