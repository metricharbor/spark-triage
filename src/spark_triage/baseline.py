from __future__ import annotations

import json
from pathlib import Path
from statistics import median
from typing import Any

from spark_triage.models import ApplicationSummary, Finding

BASELINE_SCHEMA_VERSION = "spark-triage-baseline/v1"


def profile(application: ApplicationSummary) -> dict[str, Any]:
    return {
        "run_id": application.run_id,
        "workload_id": application.workload_id,
        "started_at_ms": application.started_at_ms,
        "duration_ms": application.duration_ms,
        "task_count": application.task_count,
        "input_bytes": sum(stage.input_bytes for stage in application.stages.values()),
        "shuffle_read_bytes": sum(
            stage.shuffle_read_bytes for stage in application.stages.values()
        ),
        "shuffle_write_bytes": sum(
            stage.shuffle_write_bytes for stage in application.stages.values()
        ),
        "spill_bytes": sum(stage.total_spill_bytes for stage in application.stages.values()),
        "failed_tasks": sum(stage.failed_tasks for stage in application.stages.values()),
    }


def load_store(path: str | Path) -> dict[str, Any]:
    store_path = Path(path)
    if not store_path.exists():
        return {"schema_version": BASELINE_SCHEMA_VERSION, "workloads": {}}
    try:
        payload = json.loads(store_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read baseline store {store_path}: {exc}") from exc
    if payload.get("schema_version") != BASELINE_SCHEMA_VERSION:
        raise ValueError(f"Unsupported baseline store schema in {store_path}")
    if not isinstance(payload.get("workloads"), dict):
        raise ValueError(f"Invalid baseline store structure in {store_path}")
    return payload


def record(application: ApplicationSummary, path: str | Path, *, max_runs: int = 50) -> int:
    store_path = Path(path)
    payload = load_store(store_path)
    workloads = payload["workloads"]
    history = workloads.setdefault(application.workload_id, [])
    current = profile(application)
    history = [item for item in history if item.get("run_id") != application.run_id]
    history.append(current)
    history.sort(key=lambda item: item.get("started_at_ms") or 0)
    workloads[application.workload_id] = history[-max_runs:]
    store_path.parent.mkdir(parents=True, exist_ok=True)
    store_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return len(workloads[application.workload_id])


def compare(
    application: ApplicationSummary,
    path: str | Path,
    *,
    min_history: int = 3,
    regression_ratio: float = 1.5,
) -> list[Finding]:
    payload = load_store(path)
    history = payload["workloads"].get(application.workload_id, [])
    history = [item for item in history if item.get("run_id") != application.run_id]
    if len(history) < min_history:
        return []

    current = profile(application)
    findings: list[Finding] = []
    checks = [
        (
            "duration_ms",
            "RUNTIME_REGRESSION",
            "Application runtime regressed",
            30_000,
            "Compare the slowest regressed SQL executions and stages against prior runs.",
        ),
        (
            "spill_bytes",
            "SPILL_REGRESSION",
            "Spill volume regressed",
            64 * 1024 * 1024,
            "Check for changed input distribution, plans, partition counts, or executor memory.",
        ),
        (
            "shuffle_read_bytes",
            "SHUFFLE_VOLUME_REGRESSION",
            "Shuffle read volume regressed",
            128 * 1024 * 1024,
            "Compare physical plans and input volume before tuning network or executor settings.",
        ),
    ]
    for metric, rule_id, title, minimum_delta, recommendation in checks:
        historical_values = [item.get(metric) for item in history if item.get(metric) is not None]
        current_value = current.get(metric)
        if current_value is None or len(historical_values) < min_history:
            continue
        baseline = float(median(historical_values))
        ratio = current_value / max(1.0, baseline)
        if current_value - baseline >= minimum_delta and ratio >= regression_ratio:
            findings.append(
                Finding(
                    rule_id=rule_id,
                    severity="warning",
                    title=title,
                    message=(
                        f"{metric} is {current_value}, {ratio:.1f}x the historical median "
                        f"of {baseline:.0f}."
                    ),
                    scope="workload",
                    evidence={
                        "workload_id": application.workload_id,
                        "history_count": len(historical_values),
                        "metric": metric,
                        "current": current_value,
                        "historical_median": baseline,
                        "ratio": round(ratio, 3),
                    },
                    recommendation=recommendation,
                )
            )
    return findings
