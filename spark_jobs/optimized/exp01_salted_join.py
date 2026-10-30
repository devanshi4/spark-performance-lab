"""Experiment 1 -- OPTIMIZED version B: manual salting of the hot key.

Same business query and same data as the BEFORE version. AQE stays OFF so that the effect
you see comes only from salting (in real jobs you would normally keep AQE on as well).

Idea, in concrete terms
-----------------------
Spark sends a row to shuffle partition  hash(join key) % 16  (16 = spark.sql.shuffle.partitions).
All 21M rows of merchant 1 have the same key, so they all land in the same partition,
and one task must process them all.

Salting changes the join key from  merchant_id  to  (merchant_id, salt).
  - For the HOT merchant, each transaction gets a salt between 0 and SALT_BUCKETS-1.
    Now its rows are spread over all 16 shuffle partitions instead of one.
  - For every other merchant, salt = 0 (nothing changes for them).
  - The merchants table must match every possible (merchant_id, salt) pair, so the hot
    merchant's single row is copied SALT_BUCKETS times (salt 0..N-1). Other merchants keep
    one row with salt 0.
The join result is identical; only the way rows are spread across tasks changes.

Run:  ./scripts/run.sh spark_jobs.optimized.exp01_salted_join
"""
from pyspark.sql import functions as F

from conf import settings
from spark_jobs.common.runner import run_experiment

CONF = {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.shuffle.partitions": "16",   # same as BEFORE, for a fair comparison
}

# How many pieces to cut each hot key into. With 64 salt values hashed into 16 partitions,
# every partition receives roughly 4 pieces of merchant 1, so all 16 partitions end up
# with a similar amount of data. Rule of thumb: use several times more salt values than
# shuffle partitions, so hash collisions between salt values even out.
SALT_BUCKETS = 64

# A merchant is "hot" if it owns more than this share of all transactions.
HOT_KEY_SHARE = 0.01


def workload(spark):
    data = settings.DATA_DIR / "exp01"
    transactions = spark.read.parquet(str(data / "transactions"))
    merchants = spark.read.parquet(str(data / "merchants"))

    # (1) Find the hot keys from the data itself. This is a separate, cheap Spark job:
    #     a groupBy+count does a partial count inside each map task BEFORE the shuffle,
    #     so only ~1 row per merchant per task is shuffled, even for merchant 1.
    total = transactions.count()
    hot_ids = [r["merchant_id"] for r in
               transactions.groupBy("merchant_id").count()
               .where(F.col("count") > total * HOT_KEY_SHARE)
               .collect()]                                  # small list sent to the driver
    print(f"Hot merchant ids: {hot_ids}")
    is_hot = F.col("merchant_id").isin(hot_ids)

    # (2) Transactions side: hot rows get a salt 0..63 derived from txn_id (deterministic,
    #     spreads rows evenly). Non-hot rows get salt 0.
    salted_txn = transactions.withColumn(
        "salt",
        F.when(is_hot, F.pmod(F.xxhash64("txn_id"), F.lit(SALT_BUCKETS))).otherwise(F.lit(0)).cast("int"),
    )

    # (3) Merchants side: hot merchants are exploded into 64 rows (salt 0..63).
    #     Non-hot merchants keep one row with salt 0. This adds only 63 extra rows.
    salted_merchants = merchants.withColumn(
        "salt",
        F.explode(F.when(is_hot, F.sequence(F.lit(0), F.lit(SALT_BUCKETS - 1)))
                  .otherwise(F.array(F.lit(0)))),
    )

    # (4) Join on BOTH columns. Shuffle partition is now hash(merchant_id, salt) % 16.
    enriched = salted_txn.join(salted_merchants, on=["merchant_id", "salt"], how="inner")

    # (5) The aggregation is unchanged. Salt is not part of the output.
    report = (
        enriched
        .withColumn("txn_date", F.to_date("txn_ts"))
        .groupBy("category", "country", "txn_date")
        .agg(F.count("*").alias("txn_count"),
             F.sum("amount").alias("total_amount"),
             F.avg("amount").alias("avg_amount"))
    )
    report.explain(mode="formatted")
    report.write.mode("overwrite").parquet(str(settings.OUTPUT_DIR / "exp01" / "optimized_salted"))


if __name__ == "__main__":
    run_experiment("exp01_data_skew", "optimized_salted_join", workload, CONF)
