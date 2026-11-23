"""Collects REAL metrics for the current application from Spark's monitoring REST API.

The Spark UI reads its numbers from these same endpoints:
    /api/v1/applications/<app-id>/jobs
    /api/v1/applications/<app-id>/stages
    /api/v1/applications/<app-id>/stages/<stage-id>/<attempt>/taskList
    /api/v1/applications/<app-id>/allexecutors
    /api/v1/applications/<app-id>/sql
Nothing here is estimated or simulated: if Spark did not report a metric, it is not saved.
"""
import json
import statistics
import time
import urllib.request
from datetime import datetime


def _get(url: str):
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _pct(values, q):
    """Nearest-rank percentile (q in 0..1). Same idea as the Spark UI's summary table."""
    if not values:
        return None
    s = sorted(values)
    return s[min(len(s) - 1, max(0, round(q * (len(s) - 1))))]


def _wait_until_idle(base: str, timeout_s: int = 30):
    # The UI is updated by a background listener. Wait until no job/stage is still "RUNNING"
    # so the numbers are complete.
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        jobs = _get(f"{base}/jobs")
        if all(j["status"] != "RUNNING" for j in jobs):
            break
        time.sleep(1)
    time.sleep(2)


def collect(spark, experiment: str, variant: str, wall_clock_s: float) -> dict:
    sc = spark.sparkContext
    app_id = sc.applicationId
    base = f"{sc.uiWebUrl}/api/v1/applications/{app_id}"
    _wait_until_idle(base)

    jobs = _get(f"{base}/jobs")
    stages = _get(f"{base}/stages")
    executors = _get(f"{base}/allexecutors")

    all_durations = []
    stage_rows = []
    totals = dict(shuffle_read_bytes=0, shuffle_write_bytes=0, memory_spill_bytes=0,
                  disk_spill_bytes=0, input_bytes=0, failed_tasks=0, succeeded_tasks=0,
                  killed_tasks=0, gc_time_ms=0)

    for st in sorted(stages, key=lambda s: (s["stageId"], s["attemptId"])):
        if st["status"] == "SKIPPED":
            stage_rows.append({"stage_id": st["stageId"], "attempt": st["attemptId"],
                               "status": "SKIPPED", "name": st["name"]})
            continue
        tasks = _get(f"{base}/stages/{st['stageId']}/{st['attemptId']}/taskList?length=1000000")
        durations, records, spills, peak_mem, sread = [], [], [], [], []
        for t in tasks:
            status = t.get("status")
            if status == "FAILED":
                totals["failed_tasks"] += 1
            elif status == "KILLED":
                totals["killed_tasks"] += 1
            if status != "SUCCESS":
                continue
            totals["succeeded_tasks"] += 1
            m = t.get("taskMetrics", {})
            durations.append(t["duration"])
            sr = m.get("shuffleReadMetrics", {})
            inp = m.get("inputMetrics", {})
            records.append(sr.get("recordsRead", 0) + inp.get("recordsRead", 0))
            spills.append(m.get("diskBytesSpilled", 0))
            sread.append(sr.get("remoteBytesRead", 0) + sr.get("localBytesRead", 0))
            peak_mem.append(m.get("peakExecutionMemory", 0))
            totals["gc_time_ms"] += m.get("jvmGcTime", 0)
        all_durations.extend(durations)

        totals["shuffle_read_bytes"] += st.get("shuffleReadBytes", 0)
        totals["shuffle_write_bytes"] += st.get("shuffleWriteBytes", 0)
        totals["memory_spill_bytes"] += st.get("memoryBytesSpilled", 0)
        totals["disk_spill_bytes"] += st.get("diskBytesSpilled", 0)
        totals["input_bytes"] += st.get("inputBytes", 0)

        med = statistics.median(durations) if durations else None
        stage_rows.append({
            "stage_id": st["stageId"], "attempt": st["attemptId"], "status": st["status"],
            "name": st["name"],
            "num_tasks": len(tasks), "failed_tasks": st.get("numFailedTasks", 0),
            "stage_duration_ms": _stage_wall_ms(st),
            "task_duration_ms": {"min": min(durations) if durations else None, "median": med,
                                 "p75": _pct(durations, 0.75), "max": max(durations) if durations else None},
            "max_over_median_duration": round(max(durations) / med, 1) if durations and med else None,
            "records_per_task": {"min": min(records) if records else None,
                                 "median": statistics.median(records) if records else None,
                                 "max": max(records) if records else None},
            "shuffle_read_bytes_per_task": {"median": statistics.median(sread) if sread else None,
                                            "max": max(sread) if sread else None},
            "input_bytes": st.get("inputBytes", 0),
            "shuffle_read_bytes": st.get("shuffleReadBytes", 0),
            "shuffle_write_bytes": st.get("shuffleWriteBytes", 0),
            "memory_spill_bytes": st.get("memoryBytesSpilled", 0),
            "disk_spill_bytes": st.get("diskBytesSpilled", 0),
            "max_task_disk_spill_bytes": max(spills) if spills else 0,
            "max_task_peak_execution_memory_bytes": max(peak_mem) if peak_mem else 0,
        })

    sql = []
    try:
        for q in _get(f"{base}/sql?details=true&planDescription=true&length=1000"):
            sql.append({"id": q["id"], "description": q.get("description"), "status": q.get("status"),
                        "duration_ms": q.get("duration"), "plan": q.get("planDescription")})
    except Exception as exc:  # the SQL endpoint is best-effort
        sql.append({"error": str(exc)})

    return {
        "experiment": experiment,
        "variant": variant,
        "app_id": app_id,
        "app_name": sc.appName,
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "spark_version": spark.version,
        "wall_clock_runtime_s": round(wall_clock_s, 2),
        "num_jobs": len(jobs),
        "num_stages_run": sum(1 for s in stages if s["status"] not in ("SKIPPED",)),
        "num_stages_skipped": sum(1 for s in stages if s["status"] == "SKIPPED"),
        "num_tasks": totals["succeeded_tasks"] + totals["failed_tasks"] + totals["killed_tasks"],
        **totals,
        "task_duration_ms_all_stages": {"median": statistics.median(all_durations) if all_durations else None,
                                        "max": max(all_durations) if all_durations else None},
        "executors": [{"id": e["id"], "host_port": e["hostPort"], "is_active": e["isActive"],
                       "total_cores": e["totalCores"], "completed_tasks": e["completedTasks"],
                       "failed_tasks": e["failedTasks"], "total_gc_time_ms": e["totalGCTime"],
                       "total_duration_ms": e["totalDuration"],
                       "removed_reason": e.get("removeReason")} for e in executors],
        "stages": stage_rows,
        "sql_queries": sql,
        "relevant_conf": {k: v for k, v in sc.getConf().getAll()
                          if k.startswith(("spark.sql.adaptive", "spark.sql.shuffle", "spark.sql.autoBroadcast",
                                           "spark.executor.", "spark.memory", "spark.sql.files", "spark.task"))},
    }


def _stage_wall_ms(st):
    fmt = "%Y-%m-%dT%H:%M:%S.%f%Z"
    try:
        start = datetime.strptime(st["firstTaskLaunchedTime"].replace("GMT", "UTC"), fmt)
        end = datetime.strptime(st["completionTime"].replace("GMT", "UTC"), fmt)
        return int((end - start).total_seconds() * 1000)
    except (KeyError, ValueError, TypeError):
        return None
