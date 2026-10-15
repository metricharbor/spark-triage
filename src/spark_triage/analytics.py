from __future__ import annotations

import heapq
from collections import defaultdict, deque
from statistics import median
from typing import Any

from spark_triage.models import ApplicationSummary, StageSummary


def interval_union_ms(intervals: list[tuple[int, int]]) -> int:
    normalized = sorted((start, end) for start, end in intervals if end >= start)
    if not normalized:
        return 0
    total = 0
    current_start, current_end = normalized[0]
    for start, end in normalized[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
        else:
            total += current_end - current_start
            current_start, current_end = start, end
    return total + current_end - current_start


def max_concurrent(intervals: list[tuple[int, int]]) -> int:
    events: list[tuple[int, int]] = []
    for start, end in intervals:
        if end <= start:
            continue
        events.extend(((start, 1), (end, -1)))
    active = 0
    maximum = 0
    for _, delta in sorted(events, key=lambda item: (item[0], item[1])):
        active += delta
        maximum = max(maximum, active)
    return maximum


def _distribution(values: list[int]) -> dict[str, float | int | None]:
    if not values:
        return {"min": None, "p25": None, "median": None, "p75": None, "max": None}
    ordered = sorted(values)

    def percentile(fraction: float) -> float:
        position = (len(ordered) - 1) * fraction
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        weight = position - lower
        return ordered[lower] * (1 - weight) + ordered[upper] * weight

    return {
        "min": ordered[0],
        "p25": percentile(0.25),
        "median": float(median(ordered)),
        "p75": percentile(0.75),
        "max": ordered[-1],
    }


def _max_to_median(values: list[int]) -> float | None:
    if not values:
        return None
    center = float(median(values))
    return max(values) / max(1.0, center)


def _executor_metrics(application: ApplicationSummary) -> list[dict[str, Any]]:
    by_executor: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "task_count": 0,
            "failed_tasks": 0,
            "task_time_ms": 0,
            "input_bytes": 0,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 0,
            "spill_bytes": 0,
            "gc_time_ms": 0,
        }
    )
    for stage in application.stages.values():
        for task in stage.tasks:
            metrics = by_executor[task.executor_id]
            metrics["task_count"] += 1
            metrics["failed_tasks"] += int(not task.success)
            metrics["task_time_ms"] += task.executor_run_time_ms
            metrics["input_bytes"] += task.input_bytes
            metrics["shuffle_read_bytes"] += task.shuffle_read_bytes
            metrics["shuffle_write_bytes"] += task.shuffle_write_bytes
            metrics["spill_bytes"] += task.memory_spill_bytes + task.disk_spill_bytes
            metrics["gc_time_ms"] += task.jvm_gc_time_ms

    result = []
    for executor_id in sorted(set(application.executors) | set(by_executor)):
        executor = application.executors.get(executor_id)
        metrics = by_executor[executor_id]
        task_time = metrics["task_time_ms"]
        result.append(
            {
                "executor_id": executor_id,
                "host": executor.host if executor else "unknown",
                "cores": executor.total_cores if executor else 0,
                "added_at_ms": executor.added_at_ms if executor else None,
                "removed_at_ms": executor.removed_at_ms if executor else None,
                **metrics,
                "gc_ratio": metrics["gc_time_ms"] / task_time if task_time else 0.0,
            }
        )
    return result


def _stage_efficiency(stage: StageSummary) -> dict[str, Any]:
    durations = stage.task_durations_ms
    median_duration = float(median(durations)) if durations else 0.0
    maximum_duration = max(durations) if durations else 0
    duration = stage.duration_ms or 0
    return {
        "stage_id": stage.stage_id,
        "attempt_id": stage.attempt_id,
        "job_ids": sorted(stage.job_ids),
        "task_count": stage.task_count,
        "duration_ms": duration,
        "executor_run_time_ms": stage.executor_run_time_ms,
        "parallelism_ratio": stage.executor_run_time_ms / duration if duration else None,
        "task_skew": maximum_duration / median_duration if median_duration else None,
        "task_stage_skew": maximum_duration / duration if duration else None,
        "gc_percent": (
            100 * stage.jvm_gc_time_ms / stage.executor_run_time_ms
            if stage.executor_run_time_ms
            else 0.0
        ),
        "shuffle_fetch_percent": (
            100 * stage.fetch_wait_ms / stage.executor_run_time_ms
            if stage.executor_run_time_ms
            else 0.0
        ),
        "input_bytes": stage.input_bytes,
        "output_bytes": stage.output_bytes,
        "shuffle_read_bytes": stage.shuffle_read_bytes,
        "shuffle_write_bytes": stage.shuffle_write_bytes,
        "spill_bytes": stage.total_spill_bytes,
    }


def _critical_path_ms(application: ApplicationSummary) -> int:
    latest = _latest_stage_attempts(application)
    memo: dict[int, int] = {}
    visiting: set[int] = set()

    def path(stage_id: int) -> int:
        if stage_id in memo:
            return memo[stage_id]
        if stage_id in visiting:
            return 0
        visiting.add(stage_id)
        stage = latest.get(stage_id)
        if stage is None:
            visiting.remove(stage_id)
            return 0
        own = stage.duration_ms or max(stage.task_durations_ms, default=0)
        parent = max((path(parent_id) for parent_id in stage.parent_ids), default=0)
        visiting.remove(stage_id)
        memo[stage_id] = own + parent
        return memo[stage_id]

    return max((path(stage_id) for stage_id in latest), default=0)


def _latest_stage_attempts(application: ApplicationSummary) -> dict[int, StageSummary]:
    latest: dict[int, StageSummary] = {}
    for stage in application.stages.values():
        current = latest.get(stage.stage_id)
        if current is None or stage.attempt_id > current.attempt_id:
            latest[stage.stage_id] = stage
    return latest


def estimate_job_wallclock_ms(
    application: ApplicationSummary, job_id: int, total_cores: int
) -> int:
    """Estimate idealized job wall clock while preserving stage dependencies."""
    if total_cores < 1:
        raise ValueError("total_cores must be positive")
    job = application.jobs[job_id]
    latest = _latest_stage_attempts(application)
    stages = {stage_id: latest[stage_id] for stage_id in job.stage_ids if stage_id in latest}
    if not stages:
        return 0

    parents = {
        stage_id: {parent for parent in stage.parent_ids if parent in stages}
        for stage_id, stage in stages.items()
    }
    task_queues = {
        stage_id: deque(sorted((max(0, task.duration_ms) for task in stage.tasks), reverse=True))
        for stage_id, stage in stages.items()
    }
    completed: set[int] = set()
    activated: set[int] = set()
    remaining = {stage_id: len(tasks) for stage_id, tasks in task_queues.items()}
    running: list[tuple[int, int]] = []
    now = 0

    while len(completed) < len(stages):
        for stage_id in stages:
            if stage_id not in activated and parents[stage_id].issubset(completed):
                activated.add(stage_id)
                if remaining[stage_id] == 0:
                    completed.add(stage_id)

        available = total_cores - len(running)
        while available > 0:
            candidates = [
                stage_id
                for stage_id in sorted(activated)
                if stage_id not in completed and task_queues[stage_id]
            ]
            if not candidates:
                break
            stage_id = max(candidates, key=lambda item: task_queues[item][0])
            duration = task_queues[stage_id].popleft()
            heapq.heappush(running, (now + duration, stage_id))
            available -= 1

        if not running:
            if len(completed) < len(stages):
                raise ValueError(f"Stage dependency cycle or missing task data in job {job_id}")
            break

        now = running[0][0]
        finished_at_time: list[int] = []
        while running and running[0][0] == now:
            _, stage_id = heapq.heappop(running)
            finished_at_time.append(stage_id)
        for stage_id in finished_at_time:
            remaining[stage_id] -= 1
            if remaining[stage_id] == 0:
                completed.add(stage_id)
    return now


def build_analytics(application: ApplicationSummary) -> dict[str, Any]:
    job_intervals = [
        (job.submission_time_ms, job.completion_time_ms)
        for job in application.jobs.values()
        if job.submission_time_ms is not None and job.completion_time_ms is not None
    ]
    executor_intervals = [
        (executor.added_at_ms, executor.removed_at_ms or application.ended_at_ms)
        for executor in application.executors.values()
        if executor.added_at_ms is not None
        and (executor.removed_at_ms is not None or application.ended_at_ms is not None)
    ]
    executor_metrics = _executor_metrics(application)
    total_executor_task_time = sum(item["task_time_ms"] for item in executor_metrics)
    available_core_time = sum(
        max(0, end - start) * max(1, application.executors[executor_id].total_cores)
        for executor_id, start, end in (
            (
                executor.executor_id,
                executor.added_at_ms,
                executor.removed_at_ms or application.ended_at_ms,
            )
            for executor in application.executors.values()
            if executor.added_at_ms is not None
            and (executor.removed_at_ms is not None or application.ended_at_ms is not None)
        )
    )

    metric_names = (
        "task_time_ms",
        "input_bytes",
        "shuffle_read_bytes",
        "shuffle_write_bytes",
        "spill_bytes",
    )
    distributions = {
        metric: {
            "distribution": _distribution([item[metric] for item in executor_metrics]),
            "max_to_median": _max_to_median([item[metric] for item in executor_metrics]),
        }
        for metric in metric_names
    }

    job_groups: dict[str, list[int]] = defaultdict(list)
    for job in application.jobs.values():
        group = (
            f"sql:{job.sql_execution_id}"
            if job.sql_execution_id is not None
            else f"job:{job.job_id}"
        )
        job_groups[group].append(job.job_id)
    group_intervals = []
    group_details = []
    for group, job_ids in sorted(job_groups.items()):
        jobs = [application.jobs[job_id] for job_id in job_ids]
        starts = [job.submission_time_ms for job in jobs if job.submission_time_ms is not None]
        ends = [job.completion_time_ms for job in jobs if job.completion_time_ms is not None]
        start = min(starts) if starts else None
        end = max(ends) if ends else None
        if start is not None and end is not None:
            group_intervals.append((group, start, end))
        group_details.append(
            {
                "group": group,
                "job_ids": sorted(job_ids),
                "start_time_ms": start,
                "end_time_ms": end,
                "duration_ms": end - start if start is not None and end is not None else None,
            }
        )
    job_group_overlaps = []
    for index, (left, left_start, left_end) in enumerate(group_intervals):
        for right, right_start, right_end in group_intervals[index + 1 :]:
            overlap = min(left_end, right_end) - max(left_start, right_start)
            if overlap > 0:
                job_group_overlaps.append({"left": left, "right": right, "overlap_ms": overlap})

    stage_overlaps = []
    latest = _latest_stage_attempts(application)
    for job in application.jobs.values():
        stages = [latest[stage_id] for stage_id in job.stage_ids if stage_id in latest]
        for index, left in enumerate(stages):
            if left.submission_time_ms is None or left.completion_time_ms is None:
                continue
            for right in stages[index + 1 :]:
                if right.submission_time_ms is None or right.completion_time_ms is None:
                    continue
                overlap = min(left.completion_time_ms, right.completion_time_ms) - max(
                    left.submission_time_ms, right.submission_time_ms
                )
                if overlap > 0:
                    stage_overlaps.append(
                        {
                            "job_id": job.job_id,
                            "left_stage_id": left.stage_id,
                            "right_stage_id": right.stage_id,
                            "overlap_ms": overlap,
                        }
                    )

    observed_executors = max(1, max_concurrent(executor_intervals))
    cores_per_executor = max(
        [executor.total_cores for executor in application.executors.values()] or [1]
    )
    scaling_estimates = []
    for percentage in (25, 50, 75, 100, 125, 150, 200):
        executor_count = max(1, round(observed_executors * percentage / 100))
        total_cores = executor_count * max(1, cores_per_executor)
        estimates = []
        for job_id in application.jobs:
            try:
                estimates.append(estimate_job_wallclock_ms(application, job_id, total_cores))
            except ValueError:
                pass
        scaling_estimates.append(
            {
                "executor_count": executor_count,
                "cores_per_executor": cores_per_executor,
                "estimated_job_time_ms": sum(estimates),
                "model": "dependency-aware ideal task scheduler; job groups serialized",
            }
        )

    host_timeline: dict[str, dict[str, Any]] = {}
    for executor in application.executors.values():
        host = host_timeline.setdefault(
            executor.host,
            {"host": executor.host, "executor_ids": [], "start_time_ms": None, "end_time_ms": None},
        )
        host["executor_ids"].append(executor.executor_id)
        if executor.added_at_ms is not None:
            host["start_time_ms"] = (
                executor.added_at_ms
                if host["start_time_ms"] is None
                else min(host["start_time_ms"], executor.added_at_ms)
            )
        end = executor.removed_at_ms or application.ended_at_ms
        if end is not None:
            host["end_time_ms"] = (
                end if host["end_time_ms"] is None else max(host["end_time_ms"], end)
            )

    return {
        "efficiency": {
            "application_duration_ms": application.duration_ms,
            "job_active_time_ms": interval_union_ms(job_intervals),
            "driver_orchestration_time_ms": max(
                0, (application.duration_ms or 0) - interval_union_ms(job_intervals)
            ),
            "critical_path_ms": _critical_path_ms(application),
            "executor_task_time_ms": total_executor_task_time,
            "available_executor_core_time_ms": available_core_time,
            "cluster_utilization": (
                total_executor_task_time / available_core_time if available_core_time else None
            ),
            "max_concurrent_executors": observed_executors,
        },
        "stage_efficiency": [
            _stage_efficiency(stage)
            for stage in sorted(application.stages.values(), key=lambda item: item.key)
        ],
        "executor_metrics": executor_metrics,
        "executor_distributions": distributions,
        "job_groups": group_details,
        "job_group_overlaps": job_group_overlaps,
        "stage_overlaps": stage_overlaps,
        "host_timeline": [host_timeline[host] for host in sorted(host_timeline)],
        "scaling_estimates": scaling_estimates,
    }
