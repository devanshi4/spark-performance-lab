"""Experiment 1 -- OPTIMIZED version C: shrink the hot key BEFORE the join.

Same business question, same data, AQE OFF (so the effect comes only from the code change).

Observation that leads to this fix
----------------------------------
The report only needs counts and sums per (category, country, day).
Category and country depend only on merchant_id, so transactions can first be reduced to
one row per (merchant_id, day), and only then join to merchants.

Why this removes the skew
-------------------------
A groupBy in Spark runs in two steps:
  - Partial aggregation inside each MAP task, before the shuffle. Each map task keeps a
    hash table of (merchant_id, day) -> running count/sum and emits one row per group.
  - Final aggregation after the shuffle, combining the partial rows.
Merchant 1 has 21M transactions but only ~90 distinct days. After partial aggregation each
map task sends at most ~90 rows for merchant 1, instead of millions.
A join cannot do this: it has to keep every individual row. That is why skew hurts
joins (and non-combinable aggregations such as collect_list) much more than sum/count.

Note on averages: an average of averages is wrong, so sum and count are carried through
the pipeline and compute avg = sum / count at the end.

Run:  ./scripts/run.sh spark_jobs.optimized.exp01_aggregate_before_join
"""
from pyspark.sql import functions as F

from conf import settings
from spark_jobs.common.runner import run_experiment

CONF = {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.shuffle.partitions": "16",   # same as the baseline, for a fair comparison
}


def workload(spark):
    data = settings.DATA_DIR / "exp01"
    transactions = spark.read.parquet(str(data / "transactions"))
    merchants = spark.read.parquet(str(data / "merchants"))

    # (1) Reduce 30M transactions to one row per (merchant_id, day).
    #     Merchant 1 collapses from 21M rows to ~90 rows.
    #     Other merchants have ~12 transactions spread over 90 days, so they stay ~1 row each.
    per_merchant_day = (
        transactions
        .withColumn("txn_date", F.to_date("txn_ts"))
        .groupBy("merchant_id", "txn_date")
        .agg(F.count("*").alias("txn_count"), F.sum("amount").alias("total_amount"))
    )

    # (2) Join the much smaller, no-longer-skewed result to merchants.
    enriched = per_merchant_day.join(merchants, on="merchant_id", how="inner")

    # (3) Roll up to the report level. Sum the counts and sums; compute avg at the end.
    report = (
        enriched
        .groupBy("category", "country", "txn_date")
        .agg(F.sum("txn_count").alias("txn_count"),
             F.sum("total_amount").alias("total_amount"))
        .withColumn("avg_amount", F.col("total_amount") / F.col("txn_count"))
    )
    report.explain(mode="formatted")
    report.write.mode("overwrite").parquet(str(settings.OUTPUT_DIR / "exp01" / "optimized_agg_first"))


if __name__ == "__main__":
    run_experiment("exp01_data_skew", "optimized_aggregate_before_join", workload, CONF)
