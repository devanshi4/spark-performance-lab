# PySpark Performance Troubleshooting Lab

A hands-on lab for learning how Data Engineers find and fix Spark performance problems
using the **real** Spark UI. Every number you look at comes from an actual Spark execution
on a local multi-executor cluster. There are no screenshots and no simulated metrics.

| # | Experiment | Status |
|---|---|---|
| 1 | Data skew | ✅ ready |
| 2 | Memory spill | planned |
| 3 | Partition count (too few / too many / appropriate) | planned |
| 4 | Join strategy (SortMergeJoin vs BroadcastHashJoin) | planned |
| 5 | Out-of-memory and task failures (isolated) | planned |

Experiments are added one at a time, after the previous one has been investigated.

---

## 1. Requirements and compatibility

| Component | Version | Why |
|---|---|---|
| macOS | Apple Silicon or Intel | Linux also works |
| **Java** | **17 or 21** | Spark 4.0 dropped Java 8 and 11. Java 22+ is not supported by Spark 4.0. |
| **Python** | **3.9 – 3.13** (3.12 recommended) | PySpark 4.0 needs 3.9+. Driver and executors must use the same minor version. The lab runs both from `.venv`, so they match. |
| PySpark | 4.0.5 (pinned in `requirements.txt`) | The pip package contains the full Spark runtime: jars, `spark-class`, History Server. No separate Spark download is needed. |
| RAM | 8 GB minimum, 16 GB recommended | Default cluster uses ~6 GB for executors + 2 GB for the driver |
| Disk | ~3 GB free | Generated data, shuffle files, event logs |

### Install Java 17 on a Mac (if `check_env.sh` says it is missing)
```bash
brew install openjdk@17
```

```bash
sudo ln -sfn "$(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk" /Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

The scripts find Java automatically (`JAVA_HOME`, `/usr/libexec/java_home`, or Homebrew paths).

---

## 2. Setup (one time)

```bash
cd spark-perf-lab
```

```bash
chmod +x scripts/*.sh
```

```bash
./scripts/check_env.sh        # shows Python/Java versions and whether they are compatible
```

```bash
./scripts/setup.sh            # creates .venv and installs PySpark
```

## 3. Start and stop the local cluster

```bash
./scripts/start_cluster.sh
```

This starts 4 background Java processes:

| Process | URL | Purpose |
|---|---|---|
| Master | http://127.0.0.1:8080 | Manages workers and allocates their resources |
| Worker 1, Worker 2 | http://127.0.0.1:8081, :8082 | Launch executors; executor logs (stdout/stderr) are linked here |
| History Server | http://127.0.0.1:18080 | UI of finished applications (reads `spark-events/`) |

While an application runs, its live UI is at **http://127.0.0.1:4040**.

```bash
./scripts/status.sh           # what is running
```

```bash
./scripts/stop_cluster.sh     # stops everything; event logs stay in spark-events/
```

**Cluster shape** (edit `conf/cluster.env`): 2 workers × 2 cores × 3 GB. Each application
gets 2 executors with 2 cores and a 2 GB heap each, so **4 tasks run in parallel**. Using a real
standalone cluster (instead of `local[*]`) means you see separate executor processes in the
Executors tab, real shuffle traffic between executors, and real executor logs.

## 4. Running things

Always run modules through `scripts/run.sh`. It activates `.venv`, sets `JAVA_HOME`, and runs
from the project root:

```bash
./scripts/run.sh <python.module.path> [--hold]
```

`--hold` keeps the application alive after the work finishes, so you can browse the live UI
at :4040. Press Enter to end it. Without `--hold`, use the History Server.

Each job run:
1. prints the App ID and UI links,
2. prints the physical plan (`explain`),
3. runs the workload,
4. reads the application's metrics from Spark's REST API (the same data the UI shows) and saves them to
   `results/<experiment>/<variant>__<app-id>.json`.

## 5. Experiment 1: Data skew

Follow `docs/experiments/exp01_data_skew/part1_investigate.md`. In short:

```bash
./scripts/run.sh data_generation.generate_exp01_data                 # ~1 min, ~800 MB
```

```bash
./scripts/run.sh spark_jobs.before.exp01_skewed_join --hold          # investigate in the UI
```

Then, after the investigation (Part 2):

```bash
./scripts/run.sh spark_jobs.optimized.exp01_aqe_skew_join
```

```bash
./scripts/run.sh spark_jobs.optimized.exp01_salted_join
```

```bash
./scripts/run.sh spark_jobs.optimized.exp01_aggregate_before_join
```

```bash
./scripts/run.sh scripts.verify_exp01_outputs                        # all variants give the same answer
```

```bash
./scripts/run.sh scripts.compare_results exp01_data_skew             # -> reports/exp01_data_skew_comparison.md
```

Smaller machine? Generate less data, for example `--rows 15000000`. Note that AQE only splits a skewed
partition above 256 MB, so with much smaller data Fix A may not trigger. That is a real
limitation worth observing.

## 6. Project layout

```
conf/                    cluster.env (cluster shape, ports), settings.py (paths, shared config)
data_generation/         synthetic dataset generators (run on the cluster, write Parquet to data/)
spark_jobs/
  common/                session.py (SparkSession for the cluster), metrics.py (REST API collector),
                         runner.py (run → collect metrics → save)
  before/                intentionally inefficient implementations
  optimized/             improved implementations
docs/
  00_how_spark_executes_a_job.md   driver, executors, cores, partitions, tasks, shuffle, memory
  01_spark_ui_guide.md             where to click; what every metric means; diagnosing by combining metrics
  experiments/expNN_*/             part1_investigate.md (no answers), part2_root_cause_and_fixes.md
results/                 raw metrics JSON per run (created when you run jobs)
reports/                 comparison reports (generated by scripts/compare_results.py)
scripts/                 setup, start/stop/status, run, compare, verify
data/ output/ spark-events/ logs/ work/ run/   created at runtime (git-ignored)
```

`work/worker-N/app-.../<executor-id>/stderr` holds each executor's log on disk.
The worker UI links to the same files.

## 7. Troubleshooting

| Symptom | Fix |
|---|---|
| `Spark master not reachable` | `./scripts/start_cluster.sh` |
| Job waits forever with "Initial job has not accepted any resources" | Another application holds the cores. Check http://127.0.0.1:8080, and stop any app left running with `--hold`. |
| Port 8080 / 4040 / 18080 already in use | Change the ports in `conf/cluster.env`, or stop the other program |
| `UnsupportedClassVersionError` or a Java error at startup | Wrong Java. Run `./scripts/check_env.sh` |
| macOS firewall prompt for "java" | Allow it. Everything binds to 127.0.0.1 only. |
| History Server doesn't show the app yet | Wait ~5 s and refresh. Incomplete apps are listed under "Show incomplete applications". |
| Out of disk | Delete `output/`, `work/`, and old `spark-events/` files |

## 8. Honesty rules for results

- Runtime differences on a 4-core laptop are small, and they vary by ±10–20% between runs. Run twice
  before trusting a difference.
- An optimization that removes skew or spill does not always reduce total runtime. The
  comparison report shows the metrics that explain why.
- Only metrics Spark actually reports are saved. Nothing is estimated.
