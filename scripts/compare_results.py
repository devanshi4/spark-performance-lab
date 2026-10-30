"""Builds a Markdown comparison report from the metrics JSON files in results/<experiment>/.

Uses the most recent run of each variant. Every number comes from Spark's REST API
(captured by spark_jobs/common/metrics.py), nothing is computed from assumptions.

Run:  ./scripts/run.sh scripts.compare_results exp01_data_skew
Out:  reports/exp01_data_skew_comparison.md
"""
import json
import sys
from pathlib import Path

from conf import settings


def human_bytes(n):
    if n is None:
        return "-"
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(n) < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def ms(n):
    return "-" if n is None else (f"{n/1000:.1f} s" if n >= 1000 else f"{n:.0f} ms")


def latest_runs(exp_dir: Path):
    runs = {}
    for f in exp_dir.glob("*.json"):
        d = json.loads(f.read_text())
        if d["variant"] not in runs or d["captured_at"] > runs[d["variant"]]["captured_at"]:
            runs[d["variant"]] = d
    # "before*" variants first, then the rest alphabetically
    return [runs[k] for k in sorted(runs, key=lambda v: (not v.startswith("before"), v))]


def heaviest_shuffle_stage(d):
    """The stage that read the most shuffle data. In a join query this is the join stage."""
    cands = [s for s in d["stages"] if s.get("status") == "COMPLETE" and s.get("shuffle_read_bytes")]
    return max(cands, key=lambda s: s["shuffle_read_bytes"], default=None)


def final_plan_lines(plan: str):
    """With AQE the plan text has '== Final Plan ==' and '== Initial Plan =='. Keep only the final one."""
    lines = (plan or "").splitlines()
    if any("== Final Plan ==" in l for l in lines):
        start = next(i for i, l in enumerate(lines) if "== Final Plan ==" in l)
        end = next((i for i, l in enumerate(lines) if "== Initial Plan ==" in l), len(lines))
        return lines[start:end]
    return lines


def main():
    exp = sys.argv[1]
    runs = latest_runs(settings.RESULTS_DIR / exp)
    if not runs:
        sys.exit(f"No results in results/{exp}. Run the experiment first.")

    names = [r["variant"] for r in runs]
    L = [f"# {exp} - before/after comparison", "",
         "All values were captured from Spark's monitoring REST API at the end of each run",
         "(the same data the Spark UI shows). Most recent run of each variant is used.", "",
         "Single runs on one machine have noise of roughly +/-10-20%. Treat small differences as noise.", ""]

    def row(label, fn):
        L.append(f"| {label} | " + " | ".join(fn(r) for r in runs) + " |")

    L.append("## Application-level metrics\n")
    L.append("| Metric | " + " | ".join(f"`{n}`" for n in names) + " |")
    L.append("|---|" + "---|" * len(runs))
    row("Application ID", lambda r: f"`{r['app_id']}`")
    row("Wall-clock runtime of workload", lambda r: f"{r['wall_clock_runtime_s']:.1f} s")
    row("Jobs", lambda r: str(r["num_jobs"]))
    row("Stages run (skipped)", lambda r: f"{r['num_stages_run']} ({r['num_stages_skipped']})")
    row("Tasks", lambda r: str(r["num_tasks"]))
    row("Failed tasks", lambda r: str(r["failed_tasks"]))
    row("Input read", lambda r: human_bytes(r["input_bytes"]))
    row("Shuffle write", lambda r: human_bytes(r["shuffle_write_bytes"]))
    row("Shuffle read", lambda r: human_bytes(r["shuffle_read_bytes"]))
    row("Spill (memory)", lambda r: human_bytes(r["memory_spill_bytes"]))
    row("Spill (disk)", lambda r: human_bytes(r["disk_spill_bytes"]))
    row("Total GC time (all tasks)", lambda r: ms(r["gc_time_ms"]))
    row("Max task duration (any stage)", lambda r: ms(r["task_duration_ms_all_stages"]["max"]))
    row("Median task duration (all stages)", lambda r: ms(r["task_duration_ms_all_stages"]["median"]))

    L.append("\n## The heaviest shuffle stage in each run\n")
    L.append("The stage that read the most shuffle data (for a join query, the stage that performs the join).\n")
    L.append("| Metric | " + " | ".join(f"`{n}`" for n in names) + " |")
    L.append("|---|" + "---|" * len(runs))
    ws = [heaviest_shuffle_stage(r) for r in runs]

    def srow(label, fn):
        L.append(f"| {label} | " + " | ".join(fn(s) if s else "-" for s in ws) + " |")

    srow("Stage id", lambda s: str(s["stage_id"]))
    srow("Tasks in stage", lambda s: str(s["num_tasks"]))
    srow("Stage duration", lambda s: ms(s["stage_duration_ms"]))
    srow("Median task duration", lambda s: ms(s["task_duration_ms"]["median"]))
    srow("Max task duration", lambda s: ms(s["task_duration_ms"]["max"]))
    srow("Max / median duration", lambda s: f"{s['max_over_median_duration']}x")
    srow("Median records per task", lambda s: f"{s['records_per_task']['median']:,.0f}")
    srow("Max records per task", lambda s: f"{s['records_per_task']['max']:,.0f}")
    srow("Median shuffle read per task", lambda s: human_bytes(s.get("shuffle_read_bytes_per_task", {}).get("median")))
    srow("Max shuffle read per task", lambda s: human_bytes(s.get("shuffle_read_bytes_per_task", {}).get("max")))
    srow("Disk spill in stage", lambda s: human_bytes(s["disk_spill_bytes"]))
    srow("Max peak execution memory (one task)", lambda s: human_bytes(s["max_task_peak_execution_memory_bytes"]))

    L.append("\n## Per-stage detail\n")
    for r in runs:
        L.append(f"### `{r['variant']}`  (`{r['app_id']}`)\n")
        L.append("| Stage | Status | Tasks | Stage time | Task median | Task max | Max/median duration | Max/median records | Shuffle read | Shuffle write | Disk spill |")
        L.append("|---|---|---|---|---|---|---|---|---|---|---|")
        for s in r["stages"]:
            if s["status"] == "SKIPPED":
                L.append(f"| {s['stage_id']} | SKIPPED | | | | | | | | | |")
                continue
            rec = s["records_per_task"]
            ratio = f"{rec['max']/rec['median']:.1f}x" if rec["median"] else "-"
            L.append(f"| {s['stage_id']} | {s['status']} | {s['num_tasks']} | {ms(s['stage_duration_ms'])} | "
                     f"{ms(s['task_duration_ms']['median'])} | {ms(s['task_duration_ms']['max'])} | "
                     f"{s['max_over_median_duration']}x | {ratio} | {human_bytes(s['shuffle_read_bytes'])} | "
                     f"{human_bytes(s['shuffle_write_bytes'])} | {human_bytes(s['disk_spill_bytes'])} |")
        L.append("")

    L.append("## Join operators in the final physical plan\n")
    for r in runs:
        ops = set()
        for q in r["sql_queries"]:
            for line in final_plan_lines(q.get("plan")):
                t = line.strip(" +-:*")
                for op in ("SortMergeJoin(skew=true)", "SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin"):
                    if t.startswith(op):
                        ops.add(op)
                        break
        L.append(f"- `{r['variant']}`: {', '.join(sorted(ops)) or 'no join'}")

    out = settings.REPORTS_DIR / f"{exp}_comparison.md"
    out.parent.mkdir(exist_ok=True)
    out.write_text("\n".join(L) + "\n")
    print(f"Report written to {out.relative_to(settings.PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
