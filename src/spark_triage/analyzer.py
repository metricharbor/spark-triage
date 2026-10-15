from __future__ import annotations

from collections import Counter
from statistics import median

from spark_triage.analytics import build_analytics
from spark_triage.models import ApplicationSummary, DiagnosticReport, Finding, StageSummary


def _stage_context(
    application: ApplicationSummary, stage: StageSummary
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    job_ids = tuple(sorted(stage.job_ids))
    sql_ids = tuple(
        sorted(
            {
                application.jobs[job_id].sql_execution_id
                for job_id in job_ids
                if job_id in application.jobs
                and application.jobs[job_id].sql_execution_id is not None
            }
        )
    )
    return job_ids, sql_ids


def _ratio(maximum: int, baseline: float) -> float:
    return maximum / max(1.0, baseline)


def _rate_severity(rate: float) -> str:
    if rate >= 0.5:
        return "critical"
    return "warning"


def analyze(
    application: ApplicationSummary,
    *,
    skew_ratio: float = 3.0,
    input_skew_ratio: float = 4.0,
    min_skew_delta_ms: int = 1_000,
    spill_threshold_bytes: int = 64 * 1024 * 1024,
    gc_ratio_threshold: float = 0.15,
    fetch_wait_ratio_threshold: float = 0.20,
) -> DiagnosticReport:
    if skew_ratio <= 1 or input_skew_ratio <= 1:
        raise ValueError("skew ratios must be greater than 1")
    if spill_threshold_bytes < 0:
        raise ValueError("spill_threshold_bytes cannot be negative")

    findings: list[Finding] = []
    if application.ended_at_ms is None:
        findings.append(
            Finding(
                rule_id="INCOMPLETE_APPLICATION",
                severity="warning",
                title="Application end event is missing",
                message="The event log may belong to a running or abruptly terminated application.",
                evidence={"run_id": application.run_id},
                recommendation=(
                    "Confirm the driver state and keep ingesting rolling log segments "
                    "before treating "
                    "aggregate metrics as final."
                ),
            )
        )

    for job in sorted(application.jobs.values(), key=lambda item: item.job_id):
        if job.status == "FAILED":
            sql_ids = (job.sql_execution_id,) if job.sql_execution_id is not None else ()
            findings.append(
                Finding(
                    rule_id="JOB_FAILURE",
                    severity="critical",
                    title="Spark job failed",
                    message=f"Job {job.job_id} ended with {job.result or 'an unknown failure'}.",
                    scope="job",
                    job_ids=(job.job_id,),
                    sql_execution_ids=sql_ids,
                    evidence={"job_result": job.result, "stage_ids": job.stage_ids},
                    recommendation=(
                        "Inspect failed task reasons and the linked SQL execution first."
                    ),
                )
            )

    for execution in sorted(
        application.sql_executions.values(), key=lambda item: item.execution_id
    ):
        if execution.error_message:
            findings.append(
                Finding(
                    rule_id="SQL_EXECUTION_FAILURE",
                    severity="critical",
                    title="SQL execution failed",
                    message=f"SQL execution {execution.execution_id} reported an error.",
                    scope="sql",
                    job_ids=tuple(sorted(execution.job_ids)),
                    sql_execution_ids=(execution.execution_id,),
                    evidence={"error_message": execution.error_message},
                    recommendation=(
                        "Use the query, job, and failed-stage evidence together; avoid tuning a "
                        "downstream symptom before resolving the SQL error."
                    ),
                )
            )
        plan = execution.physical_plan_description.lower()
        if "cartesianproduct" in plan or "cartesian product" in plan:
            findings.append(
                Finding(
                    rule_id="CARTESIAN_JOIN",
                    severity="critical",
                    title="Cartesian product detected",
                    message=(
                        f"SQL execution {execution.execution_id} contains a Cartesian product in "
                        "its physical plan."
                    ),
                    scope="sql",
                    job_ids=tuple(sorted(execution.job_ids)),
                    sql_execution_ids=(execution.execution_id,),
                    evidence={"plan_operator": "CartesianProduct"},
                    recommendation=(
                        "Add or repair the join condition, or explicitly document why the "
                        "cross join "
                        "is bounded and intentional."
                    ),
                )
            )

    for stage in sorted(application.stages.values(), key=lambda item: item.key):
        job_ids, sql_ids = _stage_context(application, stage)
        findings.extend(
            _analyze_stage(
                stage,
                job_ids=job_ids,
                sql_ids=sql_ids,
                skew_ratio=skew_ratio,
                input_skew_ratio=input_skew_ratio,
                min_skew_delta_ms=min_skew_delta_ms,
                spill_threshold_bytes=spill_threshold_bytes,
                gc_ratio_threshold=gc_ratio_threshold,
                fetch_wait_ratio_threshold=fetch_wait_ratio_threshold,
            )
        )

    analytics = build_analytics(application)
    findings.extend(_analyze_executors(application))
    findings.extend(_analyze_application_heuristics(application, analytics, findings))
    severity_rank = {"critical": 0, "warning": 1, "info": 2}
    findings.sort(
        key=lambda finding: (
            severity_rank.get(finding.severity, 99),
            finding.stage_id if finding.stage_id is not None else -1,
            finding.rule_id,
        )
    )
    return DiagnosticReport(application=application, findings=findings, analytics=analytics)


def _analyze_stage(
    stage: StageSummary,
    *,
    job_ids: tuple[int, ...],
    sql_ids: tuple[int, ...],
    skew_ratio: float,
    input_skew_ratio: float,
    min_skew_delta_ms: int,
    spill_threshold_bytes: int,
    gc_ratio_threshold: float,
    fetch_wait_ratio_threshold: float,
) -> list[Finding]:
    findings: list[Finding] = []
    common = {
        "scope": "stage",
        "stage_id": stage.stage_id,
        "job_ids": job_ids,
        "sql_execution_ids": sql_ids,
    }

    if stage.attempt_id > 0:
        findings.append(
            Finding(
                rule_id="STAGE_RETRY",
                severity="warning",
                title="Stage attempt was retried",
                message=f"Stage {stage.stage_id} is on attempt {stage.attempt_id}.",
                evidence={
                    "attempt_id": stage.attempt_id,
                    "failure_reason": stage.failure_reason,
                },
                recommendation=(
                    "Trace the preceding attempt and group failures by executor and reason."
                ),
                **common,
            )
        )

    if stage.status == "FAILED":
        findings.append(
            Finding(
                rule_id="STAGE_FAILURE",
                severity="critical",
                title="Spark stage failed",
                message=f"Stage {stage.stage_id} attempt {stage.attempt_id} failed.",
                evidence={"failure_reason": stage.failure_reason},
                recommendation=(
                    "Inspect failed task reasons and executor removals for this attempt."
                ),
                **common,
            )
        )

    durations = [task.duration_ms for task in stage.tasks if task.duration_ms >= 0]
    if len(durations) >= 4:
        median_duration = float(median(durations))
        slowest = max(durations)
        observed_ratio = _ratio(slowest, median_duration)
        if slowest - median_duration >= min_skew_delta_ms and observed_ratio >= skew_ratio:
            slow_task = max(stage.tasks, key=lambda task: task.duration_ms)
            findings.append(
                Finding(
                    rule_id="TASK_DURATION_SKEW",
                    severity="warning",
                    title="Task runtime skew detected",
                    message=(
                        f"The slowest task took {slowest} ms, {observed_ratio:.1f}x the "
                        f"stage median of {median_duration:.0f} ms."
                    ),
                    executor_ids=(slow_task.executor_id,),
                    evidence={
                        "task_count": len(durations),
                        "median_duration_ms": median_duration,
                        "max_duration_ms": slowest,
                        "ratio": round(observed_ratio, 3),
                        "slow_task_index": slow_task.index,
                    },
                    recommendation=(
                        "Compare the slow task's input and shuffle bytes with the median before "
                        "changing partition counts."
                    ),
                    **common,
                )
            )

    input_sizes = [task.input_bytes for task in stage.tasks]
    if len(input_sizes) >= 4:
        median_input = float(median(input_sizes))
        largest_input = max(input_sizes)
        observed_ratio = _ratio(largest_input, median_input)
        if largest_input >= 64 * 1024 * 1024 and observed_ratio >= input_skew_ratio:
            heavy_task = max(stage.tasks, key=lambda task: task.input_bytes)
            findings.append(
                Finding(
                    rule_id="INPUT_SIZE_SKEW",
                    severity="warning",
                    title="Input partition skew detected",
                    message=(
                        f"One task read {largest_input} bytes, {observed_ratio:.1f}x the "
                        f"median of {median_input:.0f} bytes."
                    ),
                    executor_ids=(heavy_task.executor_id,),
                    evidence={
                        "median_input_bytes": median_input,
                        "max_input_bytes": largest_input,
                        "ratio": round(observed_ratio, 3),
                        "task_index": heavy_task.index,
                    },
                    recommendation=(
                        "Inspect partition keys and file sizes; prefer targeted repartitioning, "
                        "salting, or AQE skew handling over a global partition increase."
                    ),
                    **common,
                )
            )

    if stage.total_spill_bytes > spill_threshold_bytes:
        findings.append(
            Finding(
                rule_id="EXCESSIVE_SPILL",
                severity="warning",
                title="Execution memory spilled",
                message=f"Stage spilled {stage.total_spill_bytes} bytes to memory or disk.",
                evidence={
                    "memory_spill_bytes": stage.memory_spill_bytes,
                    "disk_spill_bytes": stage.disk_spill_bytes,
                    "threshold_bytes": spill_threshold_bytes,
                },
                recommendation=(
                    "Correlate spill with skew and wide operators before increasing "
                    "executor memory; "
                    "partitioning or operator changes are often the primary fix."
                ),
                **common,
            )
        )

    if stage.executor_run_time_ms >= 10_000:
        gc_ratio = stage.jvm_gc_time_ms / stage.executor_run_time_ms
        if gc_ratio >= gc_ratio_threshold:
            findings.append(
                Finding(
                    rule_id="GC_PRESSURE",
                    severity="warning",
                    title="High JVM garbage-collection time",
                    message=f"GC consumed {gc_ratio:.1%} of task executor runtime.",
                    evidence={
                        "jvm_gc_time_ms": stage.jvm_gc_time_ms,
                        "executor_run_time_ms": stage.executor_run_time_ms,
                        "ratio": round(gc_ratio, 4),
                    },
                    recommendation=(
                        "Check object-heavy operators, serialization, partition size, and executor "
                        "memory overhead before changing the collector."
                    ),
                    **common,
                )
            )

        fetch_ratio = stage.fetch_wait_ms / stage.executor_run_time_ms
        if stage.fetch_wait_ms >= 10_000 and fetch_ratio >= fetch_wait_ratio_threshold:
            findings.append(
                Finding(
                    rule_id="SHUFFLE_FETCH_WAIT",
                    severity="warning",
                    title="Tasks are blocked on remote shuffle fetch",
                    message=f"Shuffle fetch wait consumed {fetch_ratio:.1%} of executor runtime.",
                    evidence={
                        "fetch_wait_ms": stage.fetch_wait_ms,
                        "executor_run_time_ms": stage.executor_run_time_ms,
                        "ratio": round(fetch_ratio, 4),
                        "shuffle_read_bytes": stage.shuffle_read_bytes,
                    },
                    recommendation=(
                        "Check network saturation, remote block distribution, executor loss, and "
                        "oversized shuffle partitions."
                    ),
                    **common,
                )
            )

    if stage.failed_tasks:
        reasons = Counter(task.end_reason for task in stage.tasks if not task.success)
        failed_executors = tuple(
            sorted({task.executor_id for task in stage.tasks if not task.success})
        )
        findings.append(
            Finding(
                rule_id="FAILED_TASK_ATTEMPTS",
                severity="critical",
                title="Task attempts failed",
                message=f"Stage recorded {stage.failed_tasks} failed task attempt(s).",
                executor_ids=failed_executors,
                evidence={"failure_reasons": dict(reasons)},
                recommendation=(
                    "Separate deterministic data or code failures from executor-local failures; "
                    "retries alone do not remove the root cause."
                ),
                **common,
            )
        )

        failure_rate = stage.failed_tasks / stage.task_count
        if failure_rate >= 0.10:
            findings.append(
                Finding(
                    rule_id="HIGH_TASK_FAILURE_RATE",
                    severity=_rate_severity(failure_rate),
                    title="High task-attempt failure rate",
                    message=(
                        f"{stage.failed_tasks} of {stage.task_count} task attempts failed "
                        f"({failure_rate:.1%})."
                    ),
                    evidence={
                        "failed_tasks": stage.failed_tasks,
                        "task_attempts": stage.task_count,
                        "failure_rate": round(failure_rate, 4),
                    },
                    recommendation="Group failures by reason, executor, and input partition.",
                    **common,
                )
            )

    if stage.duration_ms is not None and stage.duration_ms >= 15 * 60 * 1000:
        findings.append(
            Finding(
                rule_id="LONG_RUNNING_STAGE",
                severity="warning" if stage.duration_ms < 45 * 60 * 1000 else "critical",
                title="Long-running stage",
                message=f"Stage wall-clock duration was {stage.duration_ms} ms.",
                evidence={"duration_ms": stage.duration_ms, "threshold_ms": 900_000},
                recommendation=(
                    "Use critical-path, skew, spill, GC, and shuffle evidence to identify the "
                    "limiting resource before resizing the cluster."
                ),
                **common,
            )
        )
    return findings


def _analyze_executors(application: ApplicationSummary) -> list[Finding]:
    findings: list[Finding] = []
    expected_removal_terms = ("decommission", "driver stopped", "application finished")
    for executor in sorted(application.executors.values(), key=lambda item: item.executor_id):
        reason = (executor.removed_reason or "").lower()
        if reason and not any(term in reason for term in expected_removal_terms):
            findings.append(
                Finding(
                    rule_id="UNEXPECTED_EXECUTOR_REMOVAL",
                    severity="warning",
                    title="Executor was removed unexpectedly",
                    message=(
                        f"Executor {executor.executor_id} was removed: {executor.removed_reason}."
                    ),
                    scope="executor",
                    executor_ids=(executor.executor_id,),
                    evidence={
                        "host": executor.host,
                        "removed_reason": executor.removed_reason,
                        "failed_tasks": executor.failed_tasks,
                    },
                    recommendation=(
                        "Correlate the removal time with failed tasks, heartbeat loss, container "
                        "exit status, and cluster-manager logs."
                    ),
                )
            )
    return findings


def _analyze_application_heuristics(
    application: ApplicationSummary,
    analytics: dict,
    existing_findings: list[Finding],
) -> list[Finding]:
    """Modern Spark 4 counterparts of SparkLens and Dr. Elephant heuristics."""
    findings: list[Finding] = []
    jobs = list(application.jobs.values())
    failed_jobs = [job for job in jobs if job.status == "FAILED"]
    if jobs and failed_jobs:
        rate = len(failed_jobs) / len(jobs)
        findings.append(
            Finding(
                rule_id="JOB_FAILURE_RATE",
                severity=_rate_severity(rate),
                title="Application has failed jobs",
                message=f"{len(failed_jobs)} of {len(jobs)} jobs failed ({rate:.1%}).",
                evidence={"failed_jobs": len(failed_jobs), "jobs": len(jobs), "rate": rate},
                recommendation="Prioritize the earliest failed job on the execution path.",
            )
        )

    distributions = analytics.get("executor_distributions", {})
    minimums = {
        "task_time_ms": 5 * 60 * 1000,
        "input_bytes": 100 * 1024 * 1024,
        "shuffle_read_bytes": 100 * 1024 * 1024,
        "shuffle_write_bytes": 100 * 1024 * 1024,
        "spill_bytes": 100 * 1024 * 1024,
    }
    for metric, threshold in minimums.items():
        item = distributions.get(metric, {})
        distribution = item.get("distribution", {})
        ratio = item.get("max_to_median")
        maximum = distribution.get("max")
        if maximum is not None and maximum >= threshold and ratio is not None and ratio >= 2:
            findings.append(
                Finding(
                    rule_id="EXECUTOR_LOAD_SKEW",
                    severity="warning",
                    title="Work is unevenly distributed across executors",
                    message=f"Executor {metric} maximum is {ratio:.1f}x the median.",
                    scope="executor",
                    evidence={"metric": metric, "max_to_median": ratio, **distribution},
                    recommendation=(
                        "Correlate the hot executor with task partitions and hosts before changing "
                        "executor sizing."
                    ),
                )
            )

    executor_metrics = analytics.get("executor_metrics", [])
    high_gc = [item for item in executor_metrics if item.get("gc_ratio", 0) >= 0.20]
    if high_gc:
        findings.append(
            Finding(
                rule_id="EXECUTOR_GC_SKEW",
                severity="warning",
                title="One or more executors have high GC pressure",
                message=f"{len(high_gc)} executor(s) spent at least 20% of task time in GC.",
                scope="executor",
                executor_ids=tuple(item["executor_id"] for item in high_gc),
                evidence={
                    "executor_gc_ratios": {
                        item["executor_id"]: round(item["gc_ratio"], 4) for item in high_gc
                    }
                },
                recommendation="Compare heap pressure and partition mix on affected executors.",
            )
        )

    efficiency = analytics.get("efficiency", {})
    utilization = efficiency.get("cluster_utilization")
    available = efficiency.get("available_executor_core_time_ms") or 0
    if utilization is not None and available >= 60_000 and utilization < 0.30:
        findings.append(
            Finding(
                rule_id="LOW_CLUSTER_UTILIZATION",
                severity="warning",
                title="Allocated executor capacity is underused",
                message=f"Task time used {utilization:.1%} of available executor core time.",
                evidence={"cluster_utilization": utilization, "available_core_time_ms": available},
                recommendation=(
                    "Inspect driver gaps and stage parallelism, then reduce idle capacity or "
                    "increase partition-level concurrency."
                ),
            )
        )

    properties = application.properties
    total_io = sum(
        stage.input_bytes + stage.shuffle_read_bytes + stage.shuffle_write_bytes
        for stage in application.stages.values()
    )
    if total_io >= 100 * 1024 * 1024 and "KryoSerializer" not in properties.get(
        "spark.serializer", ""
    ):
        findings.append(
            Finding(
                rule_id="SERIALIZER_RECOMMENDATION",
                severity="info",
                title="High-I/O workload is not using Kryo serialization",
                message="The recorded Spark configuration does not select KryoSerializer.",
                evidence={
                    "total_io_bytes": total_io,
                    "serializer": properties.get("spark.serializer"),
                },
                recommendation=(
                    "Benchmark Kryo with representative data and register hot classes; do not "
                    "change serialization without compatibility testing."
                ),
            )
        )

    has_skew = any(
        finding.rule_id in {"TASK_DURATION_SKEW", "INPUT_SIZE_SKEW"}
        for finding in existing_findings
    )
    if has_skew and properties.get("spark.sql.adaptive.enabled", "true").lower() == "false":
        findings.append(
            Finding(
                rule_id="AQE_DISABLED_WITH_SKEW",
                severity="info",
                title="Adaptive Query Execution is disabled for a skewed workload",
                message="Skew evidence is present while spark.sql.adaptive.enabled=false.",
                evidence={"spark.sql.adaptive.enabled": "false"},
                recommendation=(
                    "Test Spark 4 AQE skew handling and compare the resulting plan and runtime."
                ),
            )
        )
    return findings
