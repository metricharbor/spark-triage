from pathlib import Path

from spark_triage.analytics import (
    build_analytics,
    estimate_job_wallclock_ms,
    interval_union_ms,
    max_concurrent,
)
from spark_triage.models import ApplicationSummary, JobSummary, StageSummary, TaskSummary
from spark_triage.parser import parse_event_log

FIXTURE = Path(__file__).parent / "fixtures" / "spark_4_event_log.jsonl"


def task(task_id: int, duration: int) -> TaskSummary:
    return TaskSummary(
        task_id=task_id,
        index=task_id,
        attempt=0,
        executor_id="1",
        host="worker",
        duration_ms=duration,
        executor_run_time_ms=duration,
        executor_cpu_time_ns=duration * 1_000_000,
        jvm_gc_time_ms=0,
        input_bytes=0,
        input_records=0,
        output_bytes=0,
        output_records=0,
        shuffle_read_bytes=0,
        shuffle_read_records=0,
        shuffle_write_bytes=0,
        shuffle_write_records=0,
        fetch_wait_ms=0,
        memory_spill_bytes=0,
        disk_spill_bytes=0,
        result_size_bytes=0,
        success=True,
        end_reason="Success",
    )


def test_interval_math_matches_sparklens_overlap_scenarios() -> None:
    assert interval_union_ms([(0, 10), (5, 20), (30, 40)]) == 30
    assert max_concurrent([(0, 10), (5, 15), (10, 20)]) == 2
    assert max_concurrent([(i, i + 100_000) for i in range(100_000)]) == 100_000


def test_dependency_aware_parallel_stage_scheduler() -> None:
    app = ApplicationSummary()
    app.jobs[1] = JobSummary(job_id=1, stage_ids=[1, 2])
    app.stages[(1, 0)] = StageSummary(stage_id=1, tasks=[task(1, 10), task(2, 10)])
    app.stages[(2, 0)] = StageSummary(stage_id=2, parent_ids=[1], tasks=[task(3, 20), task(4, 20)])
    assert estimate_job_wallclock_ms(app, 1, total_cores=2) == 30

    app.stages[(2, 0)].parent_ids = []
    assert estimate_job_wallclock_ms(app, 1, total_cores=2) == 30


def test_report_contains_timelines_distributions_and_scaling_model() -> None:
    analytics = build_analytics(parse_event_log(FIXTURE))
    assert analytics["efficiency"]["max_concurrent_executors"] == 2
    assert analytics["executor_distributions"]["task_time_ms"]["distribution"]["max"] == 84_000
    assert [item["executor_count"] for item in analytics["scaling_estimates"]] == [
        1,
        1,
        2,
        2,
        2,
        3,
        4,
    ]
    assert analytics["host_timeline"][0]["host"] == "worker-a"


def test_job_and_stage_overlap_are_materialized() -> None:
    app = ApplicationSummary(started_at_ms=0, ended_at_ms=100)
    app.jobs[1] = JobSummary(
        job_id=1, stage_ids=[1, 2], submission_time_ms=0, completion_time_ms=80
    )
    app.jobs[2] = JobSummary(job_id=2, stage_ids=[3], submission_time_ms=20, completion_time_ms=100)
    app.stages[(1, 0)] = StageSummary(stage_id=1, submission_time_ms=0, completion_time_ms=50)
    app.stages[(2, 0)] = StageSummary(stage_id=2, submission_time_ms=25, completion_time_ms=75)
    app.stages[(3, 0)] = StageSummary(stage_id=3, submission_time_ms=20, completion_time_ms=100)
    analytics = build_analytics(app)
    assert analytics["job_group_overlaps"] == [
        {"left": "job:1", "right": "job:2", "overlap_ms": 60}
    ]
    assert analytics["stage_overlaps"] == [
        {"job_id": 1, "left_stage_id": 1, "right_stage_id": 2, "overlap_ms": 25}
    ]
