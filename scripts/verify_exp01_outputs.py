"""Checks that every Experiment 1 variant produced the same report.

Counts must match exactly. Sums/averages of DOUBLE values may differ in the last digits,
because floating-point addition gives slightly different results when rows are added in a
different order (and each variant adds them in a different order). So amounts are compared
with a relative tolerance of 1e-9.

Run:  ./scripts/run.sh scripts.verify_exp01_outputs
"""
from pyspark.sql import functions as F

from conf import settings
from spark_jobs.common.session import build_session

KEYS = ["category", "country", "txn_date"]

spark = build_session("verify_exp01_outputs")
base = settings.OUTPUT_DIR / "exp01"
variants = sorted(p.name for p in base.iterdir() if p.is_dir())
ref_name = "before" if "before" in variants else variants[0]
ref = spark.read.parquet(str(base / ref_name)).alias("r")
print(f"Reference: {ref_name} ({ref.count():,} rows)")
for v in variants:
    if v == ref_name:
        continue
    other = spark.read.parquet(str(base / v)).alias("o")
    j = ref.join(other, on=KEYS, how="full_outer")
    rel = lambda c: F.abs(F.col(f"r.{c}") - F.col(f"o.{c}")) / F.greatest(F.abs(F.col(f"r.{c}")), F.lit(1e-9))
    stats = j.agg(
        F.sum(F.when(F.col("r.txn_count").isNull() | F.col("o.txn_count").isNull(), 1).otherwise(0)).alias("missing"),
        F.sum(F.when(F.col("r.txn_count") != F.col("o.txn_count"), 1).otherwise(0)).alias("count_mismatch"),
        F.max(rel("total_amount")).alias("max_rel_diff_total"),
        F.max(rel("avg_amount")).alias("max_rel_diff_avg"),
    ).first()
    ok = stats.missing == 0 and stats.count_mismatch == 0 and stats.max_rel_diff_total < 1e-9 and stats.max_rel_diff_avg < 1e-9
    print(f"{v:<22} {'MATCH' if ok else 'DIFFERENT'}  missing_rows={stats.missing}  count_mismatches={stats.count_mismatch}"
          f"  max_relative_diff(total)={stats.max_rel_diff_total:.1e}  (avg)={stats.max_rel_diff_avg:.1e}")
spark.stop()
