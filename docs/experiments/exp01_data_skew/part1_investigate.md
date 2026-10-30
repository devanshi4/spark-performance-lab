# Experiment 1 · Part 1 — Investigate (no answers in this file)

Do **not** open `part2_root_cause_and_fixes.md` or the files in `spark_jobs/optimized/` yet.
Write down your own observations first.

---

## Step 1. The business scenario

A payments company processes card transactions for about 1 million merchants.
Finance wants a daily report: **for every merchant category, country and day, the number of
transactions, total amount and average amount.**

- `data/exp01/transactions`: 30 million rows. Columns: `txn_id, merchant_id, customer_id, amount, txn_ts, payment_method`.
- `data/exp01/merchants`: 1 million rows. Columns: `merchant_id, merchant_name, category, country, legal_entity_ref, onboarded_on`.

One merchant, "MegaMart Online" (merchant_id = 1), is a very large online retailer.

## Step 2. What the job must do

Transactions only contain `merchant_id`; category and country are in `merchants`. So:
1. Join `transactions` to `merchants` on `merchant_id`.
2. Group by `category, country, txn_date` and compute count, sum and average.
3. Write the report as Parquet.

## Step 3. The code

Open `spark_jobs/before/exp01_skewed_join.py` and read the comments. Key points:

| Code | What Spark does at that moment |
|---|---|
| `CONF = {"spark.sql.adaptive.enabled": "false", ...}` | Settings passed to the application when it starts. AQE off; 16 shuffle partitions. |
| `spark.read.parquet(...)` | Reads only file metadata. No rows are processed. |
| `transactions.join(merchants, on="merchant_id")` | Records a join in the plan. Nothing runs. |
| `.groupBy(...).agg(...)` | Records an aggregation. Nothing runs. |
| `report.explain(mode="formatted")` | Prints the physical plan. Nothing runs. |
| `report.write...parquet(...)` | **Action.** Jobs, stages and tasks are created and executed now. |

## Step 4. Run it

```bash
./scripts/run.sh data_generation.generate_exp01_data      # once; ~1 minute
```

```bash
./scripts/run.sh spark_jobs.before.exp01_skewed_join --hold
```

`--hold` keeps the application alive at the end, so the live UI at http://127.0.0.1:4040 stays up
until you press Enter. Even without `--hold`, the History Server at http://127.0.0.1:18080 will
show the run once it finishes.

**While it runs**, watch the console progress bar, for example
`[Stage 4:=====>  (15 + 1) / 16]`, which means 15 tasks finished, 1 running, 16 in total.
Note which stage pauses, and with how many tasks still running.

Also scroll up in the console and find the printed **physical plan**. Note the join
operator's name and the `Exchange` lines.

## Step 5. Open the Spark UI and record what you see

Open the application (http://127.0.0.1:4040 with `--hold`, or http://127.0.0.1:18080 → click
the App ID that starts with the one printed in your console).

Fill this in (copy it into a notes file, or reply with it in the chat):

### 5a. Jobs tab
1. How many jobs are there? Which one took most of the time?
2. Click that job. How many stages does its DAG Visualization show?

### 5b. Stages tab
3. List each stage with: number of tasks, duration, Input, Shuffle Read, Shuffle Write.
4. Which stage is the slowest? Does it have spill columns?

### 5c. Slowest stage → detail page
5. Expand **Event Timeline**. Describe what you see near the end of the stage: how many tasks are
   still running, and what the other cores are doing.
6. In **Summary Metrics**, write down Min / Median / Max for:
   - Duration
   - Shuffle Read Size / Records
   - Spill (Memory) and Spill (Disk)
   - Peak Execution Memory (tick *Show Additional Metrics → Select All* first)
   - GC Time
7. In the **Tasks** table, sort by Duration (descending). For the slowest task, write its
   Index, Executor ID, Duration, Shuffle Read Records, and Spill. Then do the same for a typical task.

### 5d. SQL / DataFrame tab
8. Click the query that wrote the Parquet output. Find the join operator. What is its name?
9. Find the `Sort` operator(s) directly below the join. What do they show for
   **spill size** and **peak memory** (total, min, med, max)? Which stage and task do the max values point to?
10. Find the `Exchange` operators. What does each show for **shuffle bytes written** / **data size**?

### 5e. Executors tab
11. Do both executors show similar Task Time, Shuffle Read and GC Time? Is any value red?

### 5f. Storage and Environment tabs
12. Is anything in Storage? Why or why not?
13. In Environment, confirm `spark.sql.adaptive.enabled` and `spark.sql.shuffle.partitions`.

## Step 6. Your hypothesis

Answer in your own words before going further:
- **A.** Was the work spread evenly across tasks in the slowest stage? What numbers show it?
- **B.** Did spill happen in every task or only in some? What does that tell you?
- **C.** What property of the *data* could produce the pattern you saw? How could you check that with one line of PySpark?
- **D.** Why did the `groupBy` stage after the join *not* show the same pattern? (Compare its Summary Metrics.)

When you have answers, share them in the chat and we will interpret them together.
