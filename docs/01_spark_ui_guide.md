# Spark UI guide: where to click, and how to read what you see

Every metric on these pages is measured by Spark while the job runs. This guide applies to
all five experiments. Each experiment's walkthrough tells you which of these pages to open.

> **Section 3 ("Combining metrics")** describes the typical pattern of each performance problem.
> For each experiment, try to form your own explanation first, then read that section to check it.

---

## 0. Which UI to open

| URL | When | What it shows |
|---|---|---|
| http://127.0.0.1:4040 | Only **while** an application runs. Use `--hold` on any job to keep the application alive after the work finishes. | Live application UI. A second running app uses :4041. |
| http://127.0.0.1:18080 | After the application has finished (refreshes every 5 s) | **History Server**: the same application UI, rebuilt from `spark-events/`. Click the App ID. |
| http://127.0.0.1:8080 | Any time | Master: workers, cores/memory in use, running and completed applications. |
| http://127.0.0.1:8081 / 8082 | Any time | Worker: the executors it launched, with links to each executor's **stdout/stderr logs**. |

The application UI has these tabs across the top:
**Jobs · Stages · Storage · Environment · Executors · SQL / DataFrame**

---

## 1. Page by page

### 1.1 Jobs tab
**Where:** top menu → *Jobs*.

**What you see:** one row per job: Job Id, Description (the line of your code that triggered
it, such as `parquet at NativeMethodAccessorImpl.java:0`), Submitted, Duration,
Stages: Succeeded/Total, Tasks: Succeeded/Total. Above the table, **Event Timeline** (click to expand)
shows when executors were added and when each job ran.

**How a Data Engineer uses it:**
- Find the job whose duration accounts for most of the runtime. That is where to dig.
- Unexpected extra jobs mean extra passes over the data. Common causes: schema inference,
  `count()` calls, `collect()`, broadcast preparation, and with AQE, one job per query stage.
- Click a job to open its **DAG Visualization**: one box per stage, with arrows for shuffle
  dependencies. "Skipped" stages are stages whose output already existed (reused shuffle
  files), so they did not run again.

### 1.2 Stages tab
**Where:** top menu → *Stages*, or click a stage inside a job.

**The list** shows per stage: Duration, Tasks: Succeeded/Total, **Input**, **Output**,
**Shuffle Read**, **Shuffle Write**. Columns for spill only appear when spill happened.

**How to use it:** sort by Duration. For the longest stage, check:
- How many tasks it had (= partitions).
- Whether it reads input (a scan stage) or shuffle data (a stage after a join/groupBy).

### 1.3 Stage detail page (the most important page for performance work)
**Where:** Stages tab → click the stage **Description** link.

Sections, from top to bottom:

1. **Summary line**: Total Time Across All Tasks, Locality Level Summary, Input Size / Records,
   Shuffle Read, Shuffle Write, Spill (Memory), Spill (Disk).
2. **DAG Visualization**: the operators inside this stage.
3. **Show Additional Metrics**: a checkbox list. Tick **Select All** to add columns such as Scheduler
   Delay, Task Deserialization Time, Shuffle Read Blocked Time, **Peak Execution Memory**.
4. **Event Timeline**: click to expand. One horizontal bar per task, grouped by executor, coloured
   by phase (scheduler delay, deserialization, shuffle read, **executor computing time**,
   shuffle write, result serialization). It shows whether tasks ran in parallel and whether one bar is far longer than the rest.
5. **Summary Metrics for N Completed Tasks**: a table with columns **Min, 25th percentile,
   Median, 75th percentile, Max** for Duration, GC Time, Input Size / Records,
   Shuffle Read Size / Records, Shuffle Write Size / Records, Spill (Memory), Spill (Disk),
   Peak Execution Memory. **Compare Max with Median in every row.** This table is the
   fastest way to see whether work was evenly distributed.
6. **Aggregated Metrics by Executor**: the same metrics summed per executor.
7. **Tasks (N)**: one row per task. Click a column header to sort (for example by Duration,
   descending). Columns include Index (= partition number), Executor ID, Status,
   Attempt, Duration, GC Time, Shuffle Read Size / Records, Spill, Errors.

### 1.4 SQL / DataFrame tab
**Where:** top menu → *SQL / DataFrame* → click a query description.

You get a graph of the physical plan. Each box is an operator: `Scan parquet`,
`Filter`, `Exchange` (= shuffle), `Sort`, `SortMergeJoin`, `BroadcastHashJoin`,
`HashAggregate`, `AQEShuffleRead`, and so on. Blue `WholeStageCodegen (n)` boxes group operators that
Spark compiled into one Java function.

Each operator shows its own metrics, often in the format:

```
spill size total (min, med, max (stageId: taskId))
280.4 MiB (0.0 B, 0.0 B, 280.4 MiB (stage 4.0: task 37))
```

The format tells you which **operator** produced the problem and in which task. Under the
graph, **Details** shows the text plan. With AQE enabled it shows a **Final Plan** (what
actually ran) and an **Initial Plan** (what Spark planned before it had statistics).

### 1.5 Executors tab
**Where:** top menu → *Executors*.

One row per executor (plus `driver`): Status, Cores, Active/Failed/Complete Tasks,
**Task Time (GC Time)**, Input, Shuffle Read, Shuffle Write, **Logs** (stdout / stderr),
Thread Dump. Spark colours GC time red when it is above 10% of task time.

**Use it to answer:** Did all executors do a similar amount of work? Did any executor fail or
get removed? (In the History Server, removed executors stay listed as "Dead", and the
removal reason is shown.) Is GC time a large share of task time?

### 1.6 Storage tab
Shows DataFrames/RDDs that were **cached** (`df.cache()` / `persist()`): how many
partitions are cached, how much is in memory and how much is on disk. **Empty means nothing was cached.**
Experiments 1–4 do not cache, so this tab is empty for them; that is expected.

### 1.7 Environment tab
Shows every Spark setting the application actually ran with. Check it to confirm that a
config you set (for example `spark.sql.adaptive.enabled`) really took effect.

---

## 2. Metric reference

For each metric: **Meaning** · **What it tells you** · **Concerning pattern** ·
**Compare against** · **Investigate next** · **Possible actions**.

### Task Duration (Summary Metrics → Duration)
- **Meaning:** wall-clock time from when the executor started the task until it finished.
- **Tells you:** how evenly work was split across tasks. A stage cannot finish before its slowest task.
- **Concerning:** Max more than ~3–5× the Median (a "straggler"); or all tasks under ~100 ms
  (overhead dominates); or one or two very long tasks while other cores sit idle in the Event Timeline.
- **Compare against:** Median and 75th percentile in the same row; the Shuffle Read Records row
  (did the slow task also get more data?); the GC Time row (was it slow because of GC?).
- **Investigate next:** sort the Tasks table by Duration, open the slowest task and look at its
  records, spill and GC time.
- **Actions:** depend on the cause (see section 3). More data in that task points to skew. The same data but slower
  points to the machine or GC. Uniformly long tasks mean partitions are too big.

### Shuffle Read Size / Records
- **Meaning:** bytes and rows a task fetched from the previous stage's shuffle output.
- **Tells you:** how much data each partition received after hash-partitioning.
- **Concerning:** Max records ≫ Median records (for example 20M versus 600K): data is not evenly
  distributed across partitions.
- **Compare against:** task Duration (does the biggest-input task also take the longest?), the
  number of distinct keys, and the shuffle partition count.
- **Investigate next:** find the key responsible. Run
  `df.groupBy(key).count().orderBy(desc("count")).show()`.
- **Actions:** fix the uneven distribution (AQE skew join, salting, pre-aggregation, broadcast).

### Shuffle Write Size / Records
- **Meaning:** bytes and rows a task wrote to shuffle files for the next stage.
- **Tells you:** how much data crosses the shuffle boundary.
- **Concerning:** shuffle write is close to the input size even though the next step only needs a
  few columns or rows (you are moving data you do not need).
- **Compare against:** the stage's Input size; the shuffle write of a version that projects or
  aggregates earlier.
- **Actions:** select only needed columns before the shuffle; filter early; aggregate before
  joining; use broadcast joins to avoid shuffling the big table.

### Spill (Memory) and Spill (Disk)
- **Meaning:** a task needed more execution memory than it could get, so it wrote part of its data to local disk and read it back later.
  *Memory* is the in-memory size of the spilled data. *Disk* is the same data serialized and compressed on disk.
- **Tells you:** the task's working set (sort buffer or hash table) did not fit in its share of execution memory.
- **Concerning:** any spill on a large scale. **Pay attention to who spills:** a single task spilling while
  others do not points to an uneven partition. All tasks spilling evenly means partitions are too large for the memory available.
- **Compare against:** Peak Execution Memory; Shuffle Read per task; executor memory and cores
  (memory per running task).
- **Actions:** make partitions smaller or more even; reduce the data before the operator; avoid
  unnecessary sorts and wide rows; only then consider more memory per task.

### Peak Execution Memory
- **Meaning:** the largest amount of execution memory the task's operators (sort, hash
  aggregate, hash join) held at once.
- **Concerning:** close to the per-task share of execution memory (about 0.5–1 GB in this lab),
  especially together with spill.
- **Compare against:** across tasks (Median against Max), and against `spark.executor.memory`.

### GC Time
- **Meaning:** time the JVM spent in garbage collection while the task ran.
- **Concerning:** above ~10% of task duration (the Executors tab colours it red). Rising GC together with
  long tasks and no spill suggests too many Java objects (for example from Python UDFs or collecting large rows).
- **Compare against:** Duration of the same task; GC time across executors.
- **Actions:** reduce object creation (built-in functions instead of UDFs), smaller partitions,
  avoid caching unneeded data, size memory appropriately.

### Scheduler Delay / Task Deserialization Time (Additional Metrics)
- **Meaning:** time the task spent waiting to be launched and setting itself up.
- **Concerning:** comparable to or larger than "executor computing time". The tasks are too small;
  there are too many partitions.
- **Actions:** fewer, larger partitions (coalesce; AQE coalescing; lower `spark.sql.shuffle.partitions`).

### Shuffle Read Blocked Time / Fetch Wait Time
- **Meaning:** time a task waited for shuffle blocks to arrive from other executors.
- **Concerning:** a large share of task time: shuffle volume is high or an executor serving the data is slow.

### Number of tasks (Tasks: Succeeded/Total)
- **Meaning:** number of partitions in the stage.
- **Concerning:** fewer tasks than cores (idle cores); or thousands of tasks of a few milliseconds each.
- **Compare against:** the total cores (4 in this lab) and the median task duration.

### Failed tasks, Attempt, Errors column
- **Meaning:** a task attempt threw an exception or its executor died. Spark retries a task up
  to `spark.task.maxFailures` (default 4) times; then the stage and the job fail.
- **Investigate next:** the Errors column on the stage page, then the executor's stderr log
  (Executors tab → Logs, or the worker UI).

---

## 3. Combining metrics: telling problems apart

One metric alone rarely identifies a cause. Here are the combinations that do.

| Problem | Duration (max against median) | Shuffle Read per task (max against median) | Spill | GC | Other signs |
|---|---|---|---|---|---|
| **Data skew** | Max ≫ median, a few tasks only | **Max ≫ median.** The slow task received far more records | Often only in the slow task(s) | Normal for most tasks | Event Timeline: one long bar while other cores are idle. A single key dominates `groupBy(key).count()` |
| **Partitions too large / memory too small** | All tasks slow, max ≈ 1–3× median | Even | **In most tasks** | Often high | Peak execution memory high in every task |
| **Too many small partitions** | Median tiny (tens of ms) | Tiny, even | None | Low | Scheduler delay and deserialization comparable to compute. Thousands of tasks |
| **Too few partitions** | Long, even | Large, even | Possible | Possible | Fewer tasks than cores, so idle cores in the timeline |
| **Slow node / GC problem (not skew)** | One task slow | **Even**. The slow task had the same records as the others | No | **High in the slow task** | Slow tasks cluster on one executor |
| **Unnecessary shuffle** | Not necessarily slow | Large shuffle write/read | Possible | - | Extra `Exchange` in the plan. Large shuffle bytes compared with input |
| **Memory failure** | - | - | - | Very high before failure | Failed tasks, `OutOfMemoryError` in stderr, executor removed |

**How to tell skew from spill:** sort the Tasks table by Duration and look at the slowest
task. If it read **many more records** than the median task, the problem is skew. Spill is
then a **consequence** of skew, because one task tried to sort too much data. If the slowest task read
about the **same** number of records as the others, and every task spills, the problem is partition size
compared with memory. Skew is not the cause.

**The order to investigate in:**
Jobs (which job is slow) → Stages (which stage) → Stage detail Summary Metrics (is the work
even? spill? GC?) → Tasks table (which task, how much data) → SQL tab (which operator) →
Executors (is it machine-wide?) → Environment (were the configs what you intended?).
