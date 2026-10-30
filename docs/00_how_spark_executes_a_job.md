# How Spark executes a job (read this first)

This page explains what physically happens on your Mac when you run a lab job.
Every term here appears in the Spark UI, so it is worth understanding them precisely.

---

## 1. The processes that run

When the cluster is started (`./scripts/start_cluster.sh`) and a job is running, these
separate Java processes (JVMs) exist on your machine:

| Process | How many | What it does | Where you see it |
|---|---|---|---|
| **Master** | 1 | Keeps a list of workers and how many cores/memory each has free. Gives resources to applications. Does **not** process data. | http://127.0.0.1:8080 |
| **Worker** | 2 | Each worker offers 2 cores and 3 GB to the master. When an application starts, the worker launches an **executor** process for it. Does **not** process data itself. | http://127.0.0.1:8081, :8082 |
| **Driver** | 1 per application | Started by your Python script. Runs your DataFrame code, builds the plan, splits it into tasks, sends tasks to executors, tracks progress, serves the live UI. | http://127.0.0.1:4040 (while running) |
| **Executor** | 2 per application (1 per worker) | Runs tasks. Each executor has 2 cores, so it runs up to 2 tasks at the same time. Holds a 2 GB Java heap. Reads input, shuffles data, writes output. | Executors tab |
| **History Server** | 1 | Reads event log files from `spark-events/` and shows the UI of applications that already finished. | http://127.0.0.1:18080 |

So for one application: **2 executors × 2 cores = 4 task slots**. At most 4 tasks run at the same moment.

---

## 2. From your code to tasks

Take this code from Experiment 1:

```python
transactions = spark.read.parquet("data/exp01/transactions")   # line A
merchants    = spark.read.parquet("data/exp01/merchants")      # line B
enriched     = transactions.join(merchants, on="merchant_id")  # line C
report       = enriched.groupBy("category", "country", "txn_date").agg(...)  # line D
report.write.parquet("output/...")                             # line E
```

### Lines A to D: transformations (nothing is processed)
- Line A and B: the driver reads only **metadata**: the list of files, the schema, the file sizes.
  To learn the schema, Spark may run a tiny job that opens the footer of the Parquet files.
  No transaction rows are processed.
- Lines C and D: the driver only records "join these" and "group this". It builds a
  **logical plan**. No data moves.

This is called **lazy evaluation**: transformations only describe the work.

### Line E: an action (now the work happens)
`write`, `count`, `collect`, `show` are **actions**. When the driver reaches an action:

1. **Optimize.** The Catalyst optimizer turns the logical plan into a **physical plan**:
   it decides which columns to read, which filters to push into the file reader,
   and which **join strategy** to use. `df.explain()` prints this plan.
2. **Create a job.** One action creates one (sometimes more than one) **job**.
3. **Cut the job into stages.** The driver cuts the plan wherever data must move between
   executors. That cut point is a **shuffle** (shown as `Exchange` in the plan).
   Everything between two shuffles runs as one **stage**.
4. **Cut each stage into tasks.** A stage runs one **task per partition**.
   A partition is one slice of the data. 16 partitions = 16 tasks.
5. **Schedule tasks.** The driver sends tasks to free cores. With 4 cores and 16 tasks,
   the first 4 run; when one finishes, the next starts on that core.
6. **A stage finishes only when its last task finishes.** The next stage that depends on it
   cannot start before that.

---

## 3. What a partition is, concretely

A partition is a set of rows that one task processes. There are two kinds:

**Input partitions** (when reading files). Spark groups Parquet files/blocks into partitions of
up to `spark.sql.files.maxPartitionBytes` (default 128 MB). For the 740 MB transactions
dataset you will see around 7 input partitions, so the reading stage has around 7 tasks.

**Shuffle partitions** (after a join or groupBy). The number is
`spark.sql.shuffle.partitions` (default 200; this lab often sets it explicitly).
Spark decides which shuffle partition a row goes to with:

```
partition_number = hash(key columns) mod number_of_shuffle_partitions
```

For a join on `merchant_id` with 16 shuffle partitions, each row goes to partition
`hash(merchant_id) mod 16`. **Every row with the same merchant_id goes to the same partition.**
That is what makes the join possible: the transactions and the merchant row for the same
merchant meet in the same task.

---

## 4. What a shuffle is, concretely

A shuffle has two halves, in two different stages:

**Shuffle write (end of the earlier stage, the "map side").**
Each task computes, for every row, the target partition number. It writes the rows into a
local file on the executor's disk, sorted by target partition, plus an index file that says
where each partition's bytes start. Shown in the UI as **Shuffle Write**.

**Shuffle read (start of the next stage, the "reduce side").**
Task number *k* of the next stage fetches partition *k* from **every** map output file,
from its own executor's disk (local read) or over the network from the other executor
(remote read). Shown in the UI as **Shuffle Read**.

A shuffle costs: serialization, disk writes, disk reads, network transfer, and a barrier
(the next stage waits for all map tasks to finish).

---

## 5. Where memory goes inside an executor

Each executor JVM in this lab has a 2 GB heap. Spark divides it roughly like this
(Spark's "unified memory manager"):

```
2048 MB heap
 - 300 MB reserved for Spark internals
 = 1748 MB usable
   x 0.6 (spark.memory.fraction)  -> ~1049 MB "Spark memory", shared by:
        execution memory: sort buffers, hash tables for joins/aggregations, shuffle buffers
        storage memory:   cached DataFrames (df.cache())
   the other 40% (~699 MB) -> "user memory": Java objects, UDF data, Spark metadata
```

The ~1 GB of execution memory is **shared by the tasks running at the same time** on that
executor (2 here). A task can use at most 1/N of it (N = running tasks), so roughly 500 MB to 1 GB.

When a task needs more memory than it can get (for example, it must sort more rows than fit):
- It writes what it has to a temporary file on disk (**spill**), frees the memory, and continues.
- At the end it merges the spilled files.
- The UI reports **Spill (Memory)**: the in-memory size of the data that was spilled, and
  **Spill (Disk)**: the size of that same data after serialization and compression, on disk.
  They describe the same data, so do not add them together.

Spilling is not a failure; the task still succeeds, but it does extra disk work and is slower.
If the memory needed cannot be spilled (some structures cannot), the task fails with
`OutOfMemoryError`. Experiment 5 covers that.

---

## 6. Summary: when data is read, shuffled and written in Experiment 1

| Moment | What happens | UI column |
|---|---|---|
| `spark.read.parquet` | Footer/metadata only (tiny job) | small job, 1 task |
| Stage reading transactions | Each task reads ~1/7 of the files, keeps 3 needed columns, computes `hash(merchant_id) mod 16`, writes shuffle files | Input, Shuffle Write |
| Stage reading merchants | Same for the merchants table | Input, Shuffle Write |
| Join stage | Task *k* fetches partition *k* of both tables, sorts both by merchant_id, merges matching rows, partially aggregates, writes shuffle files for the groupBy | Shuffle Read, Spill, Shuffle Write |
| Final stage | Task *k* fetches partition *k* of the groupBy shuffle, finishes the sums/counts, writes Parquet output | Shuffle Read, Output |
