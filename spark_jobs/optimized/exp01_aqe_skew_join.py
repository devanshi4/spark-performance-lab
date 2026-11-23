"""Experiment 1 -- OPTIMIZED version A: let Adaptive Query Execution split the skewed partition.

Same business query and same data as spark_jobs/before/exp01_skewed_join.py.
The ONLY difference is configuration: AQE is enabled.

How AQE skew-join handling works (what Spark actually does):
  1. Spark runs the two "map" stages that shuffle-write transactions and merchants.
  2. Before starting the join stage, the driver looks at the real size of every shuffle
     partition (each map task reported how many bytes it wrote for each reduce partition).
  3. A partition is marked SKEWED when BOTH are true:
        size > skewedPartitionFactor (5)  x  median partition size
        size > skewedPartitionThresholdInBytes (256 MB)
  4. A skewed partition on the transactions side is cut into several pieces of about
     advisoryPartitionSizeInBytes (64 MB). Each piece becomes its own task.
     The matching merchants partition is READ AGAIN by every one of those tasks
     (it is duplicated, not split), so every piece still finds its merchant rows.
  5. In the SQL tab the join shows as "SortMergeJoin(skew=true)".

AQE will NOT split skew when:
  - the skewed partition is below the 256 MB threshold (common on small test data), or
  - splitting would force Spark to add an extra shuffle (for example when the next
    operator needs the data still grouped by the join key). This can be overridden with
    spark.sql.adaptive.forceOptimizeSkewedJoin=true, which adds that extra shuffle.

Run:  ./scripts/run.sh spark_jobs.optimized.exp01_aqe_skew_join
"""
from spark_jobs.before.exp01_skewed_join import workload as before_workload
from conf import settings
from spark_jobs.common.runner import run_experiment

CONF = {
    "spark.sql.shuffle.partitions": "16",   # same as the baseline, for a fair comparison
    "spark.sql.adaptive.enabled": "true",
    # The values below are Spark's defaults, written out so they're visible
    # and easy to change.
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
    # AQE can also MERGE small partitions. That is a different optimization (Experiment 3),
    # so it is switched off here: the only AQE feature measured is the skew-join split.
    "spark.sql.adaptive.coalescePartitions.enabled": "false",
}


def workload(spark):
    # Exactly the same DataFrame code as the BEFORE version. Only the output folder differs.
    before_workload(spark, out=settings.OUTPUT_DIR / "exp01" / "optimized_aqe")


if __name__ == "__main__":
    run_experiment("exp01_data_skew", "optimized_aqe_skew_join", workload, CONF)
