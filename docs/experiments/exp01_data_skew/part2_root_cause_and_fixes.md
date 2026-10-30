# Experiment 1 · Part 2 — Root cause, fixes, comparison

> Open this only after finishing Part 1 and writing your own hypothesis.

---

## 1. Root cause: one key → one partition → one task

1. In the transactions stage, every task computes `hash(merchant_id) mod 16` for each row and
   writes the row to that shuffle partition.
2. Merchant 1 has **70% of all rows (21M of 30M)**. All of its rows produce the same hash, so they all
   go to the **same** shuffle partition. The other 15 partitions get only about 0.6M rows each.
3. In the join stage, the task for that partition must fetch ~300 MB of shuffle data, **sort
   21M+ rows** by merchant_id (SortMergeJoin needs both sides sorted), then merge them.
4. Its sort buffer is larger than its share of execution memory (roughly 110–217 MB in a 1 GB executor
   running 2 tasks), so it **spills** sorted runs to disk and merges them back. That makes it even slower.
5. The other 15 tasks finish in under a second. 3 of the 4 cores then sit idle, waiting for one task.
   The stage (and the whole job) takes as long as that single task.

**How the metrics prove it (not just suggest it):**

| Observation | Meaning |
|---|---|
| Max shuffle-read **records** ≫ median records | Data is unevenly distributed. The slow task got more data. This is not a slow machine. |
| Spill only in that one task | Spill is a *consequence* of the oversized partition, not a separate memory problem |
| Normal GC in that task compared with its duration | Not a GC problem |
| Event Timeline: one long bar, other cores idle | Lost parallelism |
| `groupBy("merchant_id").count()` shows one merchant with 21M rows | Confirms the skewed key |

**Why the final groupBy stage was not skewed:** the aggregation keys are
`(category, country, txn_date)`. Before shuffling, each join task already does a **partial
aggregation** (the first `HashAggregate` in the plan). It turns millions of rows into one row per
group. Only those small partial results are shuffled. Skew hurts operations that must keep
**every row** (joins, window functions, `collect_list`) much more than combinable ones (sum, count, min, max).

---

## 2. Fix A — AQE skew join (configuration only)

`./scripts/run.sh spark_jobs.optimized.exp01_aqe_skew_join`

Same code, `spark.sql.adaptive.enabled=true`. After the map stages finish, the driver knows the
real size of each shuffle partition. A partition is split when it is
**> 5 × median AND > 256 MB**. The big transactions partition is cut into pieces of about 64 MB, and the
matching merchants partition is read again by each piece's task.

Look for: in the SQL tab, `SortMergeJoin(skew=true)` and `AQEShuffleRead` showing *skewed*. In the Stages tab,
more tasks in the join stage. With AQE, each query stage becomes its own job, so you see more jobs and "skipped" stages. That is expected.

**When AQE does not help:**
- The skewed partition is under 256 MB. Check the max Shuffle Read per task. Lower
  `skewedPartitionThresholdInBytes` if the skew still matters at that size.
- Splitting would require an extra shuffle. For example, if the next step groups by the join key itself,
  the join output must stay grouped by that key. AQE then skips the split.
  `spark.sql.adaptive.forceOptimizeSkewedJoin=true` forces it and adds that shuffle.
- The skew is inside a groupBy/window, not a join: AQE's skew handling is for joins.
- Each split piece still contains only one key. For some join types (for example full outer join), the
  side that would have to be duplicated cannot be split.

## 3. Fix B — Salting (code change)

`./scripts/run.sh spark_jobs.optimized.exp01_salted_join`

Add a `salt` column (0..63) to the hot merchant's transactions and copy the hot merchant's row
64 times on the merchants side. Join on `(merchant_id, salt)`. The hot rows now spread over
all 16 partitions.

**Costs you will see in the metrics:** extra jobs to find the hot keys (a `count` and a
`groupBy count`, which are extra scans of the input), duplicated rows on the small side, and more complex code.

**When salting is useful:**
- AQE is not available or not applicable (older Spark, conditions above not met, RDD code,
  skewed window/aggregation with non-combinable functions).
- A few known hot keys that are stable over time (you can keep them in a config table and skip detection).
- Neither side of the join is small enough to broadcast.

**When salting is unnecessary or harmful:**
- AQE already splits the partition (check for `skew=true`).
- The other side is small enough to broadcast. A broadcast join has no shuffle on the big side, so it has no skew (Experiment 4).
- The operation is a combinable aggregation (sum/count). Partial aggregation already removes the skew.
- The "skew" is small (max < ~3× median), or the job time is dominated by something else.
- You salt *every* key: this multiplies the other side by the number of salt values for no benefit.

## 4. Fix C — Reduce before you join (query design)

`./scripts/run.sh spark_jobs.optimized.exp01_aggregate_before_join`

The report only needs sums and counts, and category/country depend only on merchant_id. So
aggregate transactions to `(merchant_id, day)` first. Merchant 1 collapses from 21M rows to ~90, which removes the
skew before the join exists. It also shuffles less data, because partial aggregation runs before the shuffle.
Carry `sum` and `count`, and compute `avg = sum / count` at the end.
Averaging averages would give a wrong result.

This is often the best fix in practice, because it removes work instead of redistributing it.
It is only possible when the business logic allows it.

## 5. Compare

```bash
./scripts/run.sh scripts.compare_results exp01_data_skew   # -> reports/exp01_data_skew_comparison.md
```

```bash
./scripts/run.sh scripts.verify_exp01_outputs              # all versions must produce the same report
```

Read the report and check against your own UI observations:
- Did the max/median task-duration ratio of the join stage drop? By how much?
- Did spill disappear or shrink? Where?
- Did total runtime drop for **every** fix? If not, which metrics explain why (extra jobs,
  extra input read, more shuffle)?
- Single runs vary by ±10–20%. Run a variant twice before trusting a small difference.

## 6. A note on what this experiment does NOT show

On a 4-core laptop, a 3-minute straggler costs 3 minutes. On a 400-core cluster processing
terabytes, the same pattern means 399 idle cores, and often an executor OOM, because the
oversized partition no longer fits even with spill. Skew handling matters far more at scale
than the seconds you save here.
