from __future__ import annotations

from statistics import median

from spark_triage.models import ApplicationSummary, DiagnosticReport, Finding


def analyze(
    application: ApplicationSummary,
    *,
    skew_ratio: float = 3.0,
    min_skew_delta_ms: int = 1_000,
    spill_threshold_bytes: int = 64 * 1024 * 1024,
) -> DiagnosticReport:
    if skew_ratio <= 1:
        raise ValueError("skew_ratio must be greater than 1")
    if spill_threshold_bytes < 0:
        raise ValueError("spill_threshold_bytes cannot be negative")

    findings: list[Finding] = []
    for stage_id, stage in sorted(application.stages.items()):
        durations = [duration for duration in stage.task_durations_ms if duration >= 0]
        if len(durations) >= 4:
            median_duration = float(median(durations))
            slowest = max(durations)
            observed_ratio = slowest / median_duration if median_duration else float("inf")
            if (
                slowest - median_duration >= min_skew_delta_ms
                and observed_ratio >= skew_ratio
            ):
                findings.append(
                    Finding(
                        rule_id="DATA_SKEW",
                        severity="warning",
                        stage_id=stage_id,
                        message=(
                            f"Slowest task took {slowest} ms, {observed_ratio:.1f}x the "
                            f"stage median of {median_duration:.0f} ms."
                        ),
                        details={
                            "task_count": len(durations),
                            "median_duration_ms": median_duration,
                            "max_duration_ms": slowest,
                            "ratio": round(observed_ratio, 3),
                        },
                    )
                )

        if stage.total_spill_bytes > spill_threshold_bytes:
            findings.append(
                Finding(
                    rule_id="EXCESSIVE_SPILL",
                    severity="warning",
                    stage_id=stage_id,
                    message=(
                        f"Stage spilled {stage.total_spill_bytes} bytes while processing "
                        "shuffle data."
                    ),
                    details={
                        "memory_spill_bytes": stage.memory_spill_bytes,
                        "disk_spill_bytes": stage.disk_spill_bytes,
                        "threshold_bytes": spill_threshold_bytes,
                    },
                )
            )

        if stage.failed_tasks:
            findings.append(
                Finding(
                    rule_id="FAILED_TASKS",
                    severity="critical",
                    stage_id=stage_id,
                    message=f"Stage recorded {stage.failed_tasks} failed task attempt(s).",
                    details={"failed_task_attempts": stage.failed_tasks},
                )
            )

    return DiagnosticReport(application=application, findings=findings)

