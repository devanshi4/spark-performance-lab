"""Experiment 1 -- BEFORE version (intentionally inefficient).

Business scenario
-----------------
A payments company processes card transactions for ~1 million merchants.
The finance team wants a daily report: revenue per merchant CATEGORY and COUNTRY per day
(for example: "electronics, DE, 2024-06-03 -> 41,203 transactions, 2.1M total").

The transactions table only has merchant_id. Category and country live in the merchant
master table. So the job must:
    1. JOIN transactions to merchants on merchant_id
    2. GROUP BY category, country, day

Inputs (create first with:  ./scripts/run.sh data_generation.generate_exp01_data)
    data/exp01/transactions   30M rows, ~740 MB Parquet
    data/exp01/merchants      1M rows,  ~75 MB Parquet

Run:  ./scripts/run.sh spark_jobs.before.exp01_skewed_join
      ./scripts/run.sh spark_jobs.before.exp01_skewed_join --hold   (keeps the live UI on :4040)
"""
from pyspark.sql import functions as F

from conf import settings
from spark_jobs.common.runner import run_experiment

# Spark settings for this run.
CONF = {
    # Adaptive Query Execution (AQE) lets Spark change the plan WHILE the query runs, using
    # statistics it measures after each shuffle. It is ON by default since Spark 3.2.
    # It is switched OFF here so you see the plan Spark makes with only the information it
    # has before execution starts. Many older production jobs still run this way.
    "spark.sql.adaptive.enabled": "false",
    # Number of partitions Spark creates on the receiving side of every shuffle
    # (join, groupBy). Spark's default is 200. We use 16 (= 4 cores x 4) in EVERY version of
    # this experiment so that "too many tiny partitions" does not hide the skew problem.
    # Experiment 3 is about choosing this number.
    "spark.sql.shuffle.partitions": "16",
}


def workload(spark, out=None):
    data = settings.DATA_DIR / "exp01"

    # (1) Describe the inputs. Spark reads only Parquet metadata here (schema, file list,
    #     file sizes). No transaction rows are read yet: Spark is lazy.
    transactions = spark.read.parquet(str(data / "transactions"))
    merchants = spark.read.parquet(str(data / "merchants"))

    # (2) Describe the join. Nothing runs yet.
    #     Spark must bring every transaction and its merchant row together on ONE executor
    #     core. How it does that is the "join strategy". Look for the strategy name in the
    #     plan printed below.
    enriched = transactions.join(merchants, on="merchant_id", how="inner")

    # (3) Describe the aggregation. Nothing runs yet.
    report = (
        enriched
        .withColumn("txn_date", F.to_date("txn_ts"))
        .groupBy("category", "country", "txn_date")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
        )
    )

    # (4) Print the physical plan Spark has chosen. explain() does NOT execute anything.
    report.explain(mode="formatted")

    # (5) The ACTION. Only now does the driver convert the plan into jobs, stages and tasks
    #     and send the tasks to the executors. Data is read from Parquet, shuffled, joined,
    #     aggregated, and the final result is written as Parquet files.
    out = out or settings.OUTPUT_DIR / "exp01" / "before"
    report.write.mode("overwrite").parquet(str(out))


if __name__ == "__main__":
    run_experiment("exp01_data_skew", "before_sortmerge_no_aqe", workload, CONF)
