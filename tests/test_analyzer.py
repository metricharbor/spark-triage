from pathlib import Path

from spark_triage.analyzer import analyze
from spark_triage.models import ApplicationSummary, ExecutorSummary, JobSummary, StageSummary
from spark_triage.parser import parse_event_log

FIXTURE = Path(__file__).parent / "fixtures" / "spark_4_event_log.jsonl"


def test_analyze_combines_sparklens_and_dr_elephant_heuristics() -> None:
    report = analyze(parse_event_log(FIXTURE))
    rule_ids = {finding.rule_id for finding in report.findings}

    assert {
        "TASK_DURATION_SKEW",
        "INPUT_SIZE_SKEW",
        "EXCESSIVE_SPILL",
        "GC_PRESSURE",
        "SHUFFLE_FETCH_WAIT",
        "AQE_DISABLED_WITH_SKEW",
        "SERIALIZER_RECOMMENDATION",
    } <= rule_ids
    assert report.analytics["efficiency"]["critical_path_ms"] == 94_000
    assert report.analytics["stage_efficiency"][0]["task_skew"] == 7


def test_thresholds_are_configurable() -> None:
    report = analyze(
        parse_event_log(FIXTURE),
        skew_ratio=10,
        input_skew_ratio=30,
        spill_threshold_bytes=200 * 1024 * 1024,
        gc_ratio_threshold=0.9,
        fetch_wait_ratio_threshold=0.9,
    )
    rules = {finding.rule_id for finding in report.findings}
    assert "TASK_DURATION_SKEW" not in rules
    assert "INPUT_SIZE_SKEW" not in rules
    assert "EXCESSIVE_SPILL" not in rules


def test_failure_rate_and_long_stage_thresholds() -> None:
    app = ApplicationSummary(started_at_ms=0, ended_at_ms=4_000_000)
    app.jobs[1] = JobSummary(job_id=1, status="FAILED", result="JobFailed")
    app.stages[(1, 0)] = StageSummary(
        stage_id=1,
        status="FAILED",
        failure_reason="fetch failed",
        submission_time_ms=0,
        completion_time_ms=3_000_000,
    )
    rules = {finding.rule_id for finding in analyze(app).findings}
    assert {"JOB_FAILURE", "JOB_FAILURE_RATE", "STAGE_FAILURE", "LONG_RUNNING_STAGE"} <= rules


def test_executor_oom_is_classified_as_a_root_cause() -> None:
    app = ApplicationSummary()
    app.executors["7"] = ExecutorSummary(
        executor_id="7", host="worker-z", removed_reason="Container killed: OutOfMemoryError"
    )
    rules = {finding.rule_id for finding in analyze(app).findings}
    assert {"EXECUTOR_OUT_OF_MEMORY", "UNEXPECTED_EXECUTOR_REMOVAL"} <= rules
