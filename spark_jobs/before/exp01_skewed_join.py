"""Experiment 1, baseline: a skewed join run the slow way on purpose.

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
    # AQE (on by default since Spark 3.2) re-plans using runtime statistics. I turn it off
    # here to see the plan Spark picks from pre-execution information alone, which is how
    # many older production jobs still run.
    "spark.sql.adaptive.enabled": "false",
    # 16 shuffle partitions (4 cores x 4) in every version of this experiment, so too many
    # tiny partitions can't hide the skew. Choosing this number is experiment 3.
    "spark.sql.shuffle.partitions": "16",
}


def workload(spark, out=None):
    data = settings.DATA_DIR / "exp01"

    transactions = spark.read.parquet(str(data / "transactions"))
    merchants = spark.read.parquet(str(data / "merchants"))

    # One merchant owns most transactions, so with a sort-merge join all of its rows hash
    # to the same shuffle partition and a single task does most of the work.
    enriched = transactions.join(merchants, on="merchant_id", how="inner")

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

    # Print the physical plan to confirm the join strategy before running.
    report.explain(mode="formatted")

    out = out or settings.OUTPUT_DIR / "exp01" / "before"
    report.write.mode("overwrite").parquet(str(out))


if __name__ == "__main__":
    run_experiment("exp01_data_skew", "before_sortmerge_no_aqe", workload, CONF)
