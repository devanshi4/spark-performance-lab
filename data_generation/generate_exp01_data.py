"""Generates the datasets for Experiment 1 (data skew).

  data/exp01/transactions/  -- card transactions, Parquet
  data/exp01/merchants/     -- merchant master data, Parquet

The skew is deliberate: one merchant (merchant_id = 1, "MegaMart Online") receives 70% of rows
(--hot-fraction). All other ~1M merchants share the rest evenly (~12 rows each).

Run:  ./scripts/run.sh data_generation.generate_exp01_data            (default 30M rows)
      ./scripts/run.sh data_generation.generate_exp01_data --rows 10000000   (smaller laptop)
"""
import argparse
import time

from pyspark.sql import functions as F

from conf import settings
from spark_jobs.common.session import build_session

NUM_MERCHANTS = 1_000_000
HOT_MERCHANT_ID = 1


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=30_000_000, help="number of transactions")
    p.add_argument("--hot-fraction", type=float, default=0.7, help="share of rows that go to merchant 1")
    args = p.parse_args()

    spark = build_session("datagen_exp01")
    out = settings.DATA_DIR / "exp01"
    t0 = time.time()

    # spark.range(n) creates a DataFrame with one column `id` = 0..n-1.
    # It is split into 32 partitions -> 32 tasks generate rows in parallel on the executors.
    base = spark.range(0, args.rows, numPartitions=32)

    # A deterministic "random" number in [0, 1) per row, derived from the row id.
    # Using a hash (instead of rand()) means every run produces exactly the same data.
    u = (F.pmod(F.xxhash64("id", F.lit(7)), F.lit(1_000_000)) / F.lit(1_000_000.0))

    transactions = base.select(
        F.col("id").alias("txn_id"),
        # The skew: rows with u < hot_fraction belong to merchant 1, the rest are spread
        # evenly over merchants 2 .. NUM_MERCHANTS.
        F.when(u < args.hot_fraction, F.lit(HOT_MERCHANT_ID))
         .otherwise(F.pmod(F.xxhash64("id", F.lit(11)), F.lit(NUM_MERCHANTS - 1)) + 2)
         .cast("int").alias("merchant_id"),
        F.pmod(F.xxhash64("id", F.lit(13)), F.lit(5_000_000)).alias("customer_id"),
        F.round(F.pmod(F.xxhash64("id", F.lit(17)), F.lit(50_000)) / 100.0 + 1.0, 2).alias("amount"),
        # spread transactions over 90 days
        F.timestamp_seconds(F.lit(1_717_200_000) + F.pmod(F.xxhash64("id", F.lit(19)), F.lit(90 * 86_400)))
         .alias("txn_ts"),
        F.element_at(F.array(*[F.lit(x) for x in ["card", "apple_pay", "google_pay", "bank_transfer"]]),
                     (F.pmod(F.xxhash64("id", F.lit(23)), F.lit(4)) + 1).cast("int")).alias("payment_method"),
    )
    transactions.write.mode("overwrite").parquet(str(out / "transactions"))
    print(f"transactions written ({args.rows:,} rows) in {time.time() - t0:.0f}s")

    categories = ["grocery", "electronics", "fashion", "travel", "restaurants", "fuel", "health", "digital"]
    countries = ["US", "GB", "DE", "FR", "IN", "BR", "JP", "AU", "CA", "MX"]
    merchants = spark.range(1, NUM_MERCHANTS + 1, numPartitions=8).select(
        F.col("id").cast("int").alias("merchant_id"),
        F.when(F.col("id") == HOT_MERCHANT_ID, F.lit("MegaMart Online"))
         .otherwise(F.concat(F.lit("Merchant-"), F.col("id").cast("string"))).alias("merchant_name"),
        F.element_at(F.array(*[F.lit(c) for c in categories]),
                     (F.pmod(F.xxhash64("id", F.lit(3)), F.lit(len(categories))) + 1).cast("int")).alias("category"),
        F.element_at(F.array(*[F.lit(c) for c in countries]),
                     (F.pmod(F.xxhash64("id", F.lit(5)), F.lit(len(countries))) + 1).cast("int")).alias("country"),
        # an opaque legal-entity reference (does not compress well, like real IDs)
        F.sha2(F.col("id").cast("string"), 256).alias("legal_entity_ref"),
        F.date_add(F.lit("2015-01-01"), F.pmod(F.xxhash64("id", F.lit(9)), F.lit(3000)).cast("int")).alias("onboarded_on"),
    )
    merchants.write.mode("overwrite").parquet(str(out / "merchants"))

    # Report the real skew so you know what the data looks like.
    counts = (spark.read.parquet(str(out / "transactions"))
              .groupBy("merchant_id").count().orderBy(F.desc("count")).limit(3).collect())
    print("Top merchants by transaction count:")
    for r in counts:
        print(f"  merchant_id={r['merchant_id']:>8}  transactions={r['count']:,}")
    print(f"Done in {time.time() - t0:.0f}s. Data in {out}")
    spark.stop()


if __name__ == "__main__":
    main()
