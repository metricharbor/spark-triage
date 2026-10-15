from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from spark_triage.models import ApplicationSummary, StageSummary


class EventLogError(ValueError):
    """Raised when an event log cannot be parsed safely."""


def _integer(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    return 0


def _stage(summary: ApplicationSummary, stage_id: int) -> StageSummary:
    if stage_id not in summary.stages:
        summary.stages[stage_id] = StageSummary(stage_id=stage_id)
    return summary.stages[stage_id]


def parse_event_log(path: str | Path) -> ApplicationSummary:
    event_log = Path(path)
    if not event_log.is_file():
        raise EventLogError(f"Event log does not exist or is not a file: {event_log}")

    summary = ApplicationSummary()
    try:
        handle = event_log.open("r", encoding="utf-8")
    except OSError as exc:
        raise EventLogError(f"Cannot read event log: {exc}") from exc

    with handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise EventLogError(
                    f"Invalid JSON on line {line_number}: {exc.msg}"
                ) from exc
            if not isinstance(event, dict):
                raise EventLogError(f"Expected a JSON object on line {line_number}")
            _apply_event(summary, event)

    return summary


def _apply_event(summary: ApplicationSummary, event: dict[str, Any]) -> None:
    event_type = event.get("Event")

    if event_type == "SparkListenerApplicationStart":
        summary.name = str(event.get("App Name") or "unknown")
        summary.application_id = str(event.get("App ID") or "unknown")
        summary.started_at_ms = _integer(event.get("Timestamp")) or None
        return

    if event_type == "SparkListenerApplicationEnd":
        summary.ended_at_ms = _integer(event.get("Timestamp")) or None
        return

    if event_type == "SparkListenerStageSubmitted":
        stage_info = event.get("Stage Info") or {}
        stage_id = _integer(stage_info.get("Stage ID"))
        stage = _stage(summary, stage_id)
        stage.name = str(stage_info.get("Stage Name") or stage.name)
        expected_tasks = _integer(stage_info.get("Number of Tasks"))
        stage.expected_tasks = expected_tasks or stage.expected_tasks
        return

    if event_type != "SparkListenerTaskEnd":
        return

    stage_id = _integer(event.get("Stage ID"))
    stage = _stage(summary, stage_id)
    task_info = event.get("Task Info") or {}
    metrics = event.get("Task Metrics") or {}

    duration = _integer(task_info.get("Finish Time")) - _integer(task_info.get("Launch Time"))
    if duration <= 0:
        duration = _integer(metrics.get("Executor Run Time"))
    if duration >= 0:
        stage.task_durations_ms.append(duration)

    end_reason = event.get("Task End Reason")
    if end_reason != "Success":
        stage.failed_tasks += 1

    input_metrics = metrics.get("Input Metrics") or {}
    shuffle_read = metrics.get("Shuffle Read Metrics") or {}
    shuffle_write = metrics.get("Shuffle Write Metrics") or {}
    stage.input_bytes += _integer(input_metrics.get("Bytes Read"))
    stage.shuffle_read_bytes += _integer(shuffle_read.get("Remote Bytes Read"))
    stage.shuffle_read_bytes += _integer(shuffle_read.get("Local Bytes Read"))
    stage.shuffle_write_bytes += _integer(shuffle_write.get("Shuffle Bytes Written"))
    stage.memory_spill_bytes += _integer(metrics.get("Memory Bytes Spilled"))
    stage.disk_spill_bytes += _integer(metrics.get("Disk Bytes Spilled"))

