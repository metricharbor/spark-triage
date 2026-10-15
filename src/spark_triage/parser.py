from __future__ import annotations

import gzip
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any, TextIO

from spark_triage.models import (
    ApplicationSummary,
    ExecutorSummary,
    JobSummary,
    SQLExecutionSummary,
    StageSummary,
    TaskSummary,
)

SUPPORTED_SPARK_MAJOR = 4
SAFE_SPARK_PROPERTIES = {
    "spark.app.name",
    "spark.dynamicAllocation.enabled",
    "spark.dynamicAllocation.maxExecutors",
    "spark.dynamicAllocation.minExecutors",
    "spark.eventLog.compress",
    "spark.eventLog.compression.codec",
    "spark.eventLog.rolling.enabled",
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.executor.memory",
    "spark.master",
    "spark.serializer",
    "spark.sql.adaptive.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.shuffle.partitions",
    "spark.triage.workload.id",
}


class EventLogError(ValueError):
    """Raised when an event log cannot be replayed safely."""


def _integer(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _optional_integer(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return _integer(value)


def _value(event: dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in event:
            return event[name]
    return default


def _event_name(event: dict[str, Any]) -> str:
    return str(event.get("Event") or "UnknownEvent").rsplit(".", 1)[-1]


def _mapping(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        return {str(key): str(item) for key, item in value.items()}
    if isinstance(value, list):
        result: dict[str, str] = {}
        for item in value:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                result[str(item[0])] = str(item[1])
        return result
    return {}


def _natural_key(path: Path) -> list[str | int]:
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", path.name)]


def _source_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise EventLogError(f"Event log path does not exist: {path}")

    files = [
        candidate
        for candidate in path.iterdir()
        if candidate.is_file()
        and not candidate.name.startswith((".", "_"))
        and not candidate.name.endswith(".crc")
    ]
    if not files:
        raise EventLogError(f"Rolling event-log directory contains no readable files: {path}")
    return sorted(files, key=_natural_key)


def _open_text(path: Path) -> TextIO:
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    if path.suffix.lower() in {".lz4", ".lzf", ".snappy", ".zstd", ".zst"}:
        raise EventLogError(
            f"Compression codec for {path.name} is detected but not installed. "
            "Use an uncompressed Spark 4 event log for this release."
        )
    return path.open("r", encoding="utf-8")


def _events(path: Path) -> Iterator[tuple[dict[str, Any], Path, int]]:
    for source in _source_files(path):
        try:
            handle = _open_text(source)
        except OSError as exc:
            raise EventLogError(f"Cannot read {source}: {exc}") from exc
        with handle:
            for line_number, raw_line in enumerate(handle, start=1):
                if not raw_line.strip():
                    continue
                try:
                    event = json.loads(raw_line)
                except json.JSONDecodeError as exc:
                    raise EventLogError(
                        f"Invalid JSON in {source.name} on line {line_number}: {exc.msg}"
                    ) from exc
                if not isinstance(event, dict):
                    raise EventLogError(
                        f"Expected a JSON object in {source.name} on line {line_number}"
                    )
                yield event, source, line_number


def _stage(summary: ApplicationSummary, stage_id: int, attempt_id: int = 0) -> StageSummary:
    key = (stage_id, attempt_id)
    if key not in summary.stages:
        summary.stages[key] = StageSummary(stage_id=stage_id, attempt_id=attempt_id)
    return summary.stages[key]


def _executor(summary: ApplicationSummary, executor_id: str) -> ExecutorSummary:
    if executor_id not in summary.executors:
        summary.executors[executor_id] = ExecutorSummary(executor_id=executor_id)
    return summary.executors[executor_id]


def parse_event_log(path: str | Path) -> ApplicationSummary:
    event_log = Path(path)
    sources = _source_files(event_log)
    summary = ApplicationSummary()
    summary.replay.source_paths = [str(source) for source in sources]
    summary.replay.source_bytes = sum(source.stat().st_size for source in sources)

    for event, _, _ in _events(event_log):
        summary.replay.event_counts[_event_name(event)] += 1
        _apply_event(summary, event)

    if summary.replay.spark_version == "unknown":
        raise EventLogError(
            "SparkListenerLogStart is missing; Spark version cannot be verified. "
            "Spark Triage intentionally accepts only Spark 4.x logs."
        )
    version_major = _integer(summary.replay.spark_version.split(".", 1)[0], default=-1)
    if version_major != SUPPORTED_SPARK_MAJOR:
        raise EventLogError(
            f"Unsupported Spark version {summary.replay.spark_version}; "
            "only Spark 4.x is supported."
        )

    summary.workload_id = (
        summary.properties.get("spark.triage.workload.id")
        or summary.properties.get("spark.app.name")
        or summary.name
    )
    _correlate(summary)
    return summary


def _apply_event(summary: ApplicationSummary, event: dict[str, Any]) -> None:
    event_type = _event_name(event)

    if event_type == "SparkListenerLogStart":
        summary.replay.spark_version = str(_value(event, "Spark Version", "sparkVersion"))
        return

    if event_type == "SparkListenerApplicationStart":
        summary.name = str(_value(event, "App Name", "appName", default="unknown"))
        summary.application_id = str(_value(event, "App ID", "appId", default="unknown"))
        attempt = _value(event, "App Attempt ID", "appAttemptId")
        summary.attempt_id = str(attempt) if attempt not in (None, "") else None
        summary.user = _value(event, "User", "user")
        summary.started_at_ms = _optional_integer(_value(event, "Timestamp", "time"))
        return

    if event_type == "SparkListenerApplicationEnd":
        summary.ended_at_ms = _optional_integer(_value(event, "Timestamp", "time"))
        summary.exit_code = _optional_integer(_value(event, "Exit Code", "exitCode"))
        return

    if event_type == "SparkListenerEnvironmentUpdate":
        properties = _mapping(_value(event, "Spark Properties", "sparkProperties", default={}))
        summary.properties.update(
            {key: value for key, value in properties.items() if key in SAFE_SPARK_PROPERTIES}
        )
        return

    if event_type == "SparkListenerExecutorAdded":
        executor_id = str(_value(event, "Executor ID", "executorId", default="unknown"))
        info = _value(event, "Executor Info", "executorInfo", default={}) or {}
        executor = _executor(summary, executor_id)
        executor.added_at_ms = _optional_integer(_value(event, "Timestamp", "time"))
        executor.host = str(_value(info, "Host", "executorHost", default=executor.host))
        executor.total_cores = _integer(_value(info, "Total Cores", "totalCores"))
        return

    if event_type == "SparkListenerExecutorRemoved":
        executor_id = str(_value(event, "Executor ID", "executorId", default="unknown"))
        executor = _executor(summary, executor_id)
        executor.removed_at_ms = _optional_integer(_value(event, "Timestamp", "time"))
        executor.removed_reason = str(_value(event, "Removed Reason", "reason", default="unknown"))
        return

    if event_type == "SparkListenerJobStart":
        job_id = _integer(_value(event, "Job ID", "jobId"))
        stage_ids = [
            _integer(stage_id) for stage_id in _value(event, "Stage IDs", "stageIds", default=[])
        ]
        properties = _mapping(_value(event, "Properties", "properties", default={}))
        sql_execution_id = _optional_integer(properties.get("spark.sql.execution.id"))
        summary.jobs[job_id] = JobSummary(
            job_id=job_id,
            stage_ids=stage_ids,
            submission_time_ms=_optional_integer(
                _value(event, "Submission Time", "submissionTime")
            ),
            status="RUNNING",
            sql_execution_id=sql_execution_id,
            properties={
                key: value
                for key, value in properties.items()
                if key in {"spark.job.description", "spark.jobGroup.id", "spark.sql.execution.id"}
            },
        )
        return

    if event_type == "SparkListenerJobEnd":
        job_id = _integer(_value(event, "Job ID", "jobId"))
        job = summary.jobs.setdefault(job_id, JobSummary(job_id=job_id))
        job.completion_time_ms = _optional_integer(
            _value(event, "Completion Time", "completionTime")
        )
        result_value = _value(event, "Job Result", "jobResult", default="unknown")
        if isinstance(result_value, dict):
            job.result = str(_value(result_value, "Result", "result", default="unknown"))
        else:
            job.result = str(result_value)
        job.status = "SUCCEEDED" if "succeed" in job.result.lower() else "FAILED"
        return

    if event_type == "SparkListenerStageSubmitted":
        info = _value(event, "Stage Info", "stageInfo", default={}) or {}
        stage_id = _integer(_value(info, "Stage ID", "stageId"))
        attempt_id = _integer(_value(info, "Stage Attempt ID", "attemptId"))
        stage = _stage(summary, stage_id, attempt_id)
        stage.name = str(_value(info, "Stage Name", "name", default=stage.name))
        stage.parent_ids = [
            _integer(parent) for parent in _value(info, "Parent IDs", "parentIds", default=[])
        ]
        stage.expected_tasks = _optional_integer(_value(info, "Number of Tasks", "numTasks"))
        stage.submission_time_ms = _optional_integer(
            _value(info, "Submission Time", "submissionTime")
        )
        stage.status = "RUNNING"
        return

    if event_type == "SparkListenerStageCompleted":
        info = _value(event, "Stage Info", "stageInfo", default={}) or {}
        stage_id = _integer(_value(info, "Stage ID", "stageId"))
        attempt_id = _integer(_value(info, "Stage Attempt ID", "attemptId"))
        stage = _stage(summary, stage_id, attempt_id)
        stage.name = str(_value(info, "Stage Name", "name", default=stage.name))
        stage.parent_ids = [
            _integer(parent)
            for parent in _value(info, "Parent IDs", "parentIds", default=stage.parent_ids)
        ]
        stage.completion_time_ms = _optional_integer(
            _value(info, "Completion Time", "completionTime")
        )
        failure_reason = _value(info, "Failure Reason", "failureReason")
        stage.failure_reason = str(failure_reason) if failure_reason else None
        stage.status = "FAILED" if stage.failure_reason else "SUCCEEDED"
        return

    if event_type == "SparkListenerTaskEnd":
        _apply_task_end(summary, event)
        return

    if event_type == "SparkListenerSQLExecutionStart":
        execution_id = _integer(_value(event, "executionId", "Execution ID"))
        summary.sql_executions[execution_id] = SQLExecutionSummary(
            execution_id=execution_id,
            root_execution_id=_optional_integer(
                _value(event, "rootExecutionId", "Root Execution ID")
            ),
            query_id=_value(event, "queryId", "Query ID"),
            description=str(_value(event, "description", "Description", default="unknown")),
            details=str(_value(event, "details", "Details", default="")),
            physical_plan_description=str(
                _value(event, "physicalPlanDescription", "Physical Plan Description", default="")
            ),
            start_time_ms=_optional_integer(_value(event, "time", "Time")),
        )
        return

    if event_type == "SparkListenerSQLAdaptiveExecutionUpdate":
        execution_id = _integer(_value(event, "executionId", "Execution ID"))
        execution = summary.sql_executions.setdefault(
            execution_id, SQLExecutionSummary(execution_id=execution_id)
        )
        execution.physical_plan_description = str(
            _value(event, "physicalPlanDescription", "Physical Plan Description", default="")
        )
        return

    if event_type == "SparkListenerSQLExecutionEnd":
        execution_id = _integer(_value(event, "executionId", "Execution ID"))
        execution = summary.sql_executions.setdefault(
            execution_id, SQLExecutionSummary(execution_id=execution_id)
        )
        execution.end_time_ms = _optional_integer(_value(event, "time", "Time"))
        error = _value(event, "errorMessage", "Error Message")
        execution.error_message = str(error) if error else None


def _reason(value: Any) -> tuple[bool, str]:
    if value == "Success":
        return True, "Success"
    if isinstance(value, dict):
        reason = str(_value(value, "Reason", "reason", default="Unknown"))
        return reason == "Success", reason
    return False, str(value or "Unknown")


def _apply_task_end(summary: ApplicationSummary, event: dict[str, Any]) -> None:
    stage_id = _integer(_value(event, "Stage ID", "stageId"))
    attempt_id = _integer(_value(event, "Stage Attempt ID", "stageAttemptId"))
    stage = _stage(summary, stage_id, attempt_id)
    task_info = _value(event, "Task Info", "taskInfo", default={}) or {}
    metrics = _value(event, "Task Metrics", "taskMetrics", default={}) or {}
    input_metrics = _value(metrics, "Input Metrics", "inputMetrics", default={}) or {}
    output_metrics = _value(metrics, "Output Metrics", "outputMetrics", default={}) or {}
    shuffle_read = _value(metrics, "Shuffle Read Metrics", "shuffleReadMetrics", default={}) or {}
    shuffle_write = (
        _value(metrics, "Shuffle Write Metrics", "shuffleWriteMetrics", default={}) or {}
    )

    duration = _integer(_value(task_info, "Finish Time", "finishTime")) - _integer(
        _value(task_info, "Launch Time", "launchTime")
    )
    if duration <= 0:
        duration = _integer(_value(metrics, "Executor Run Time", "executorRunTime"))
    success, end_reason = _reason(_value(event, "Task End Reason", "taskEndReason"))
    executor_id = str(_value(task_info, "Executor ID", "executorId", default="unknown"))
    executor = _executor(summary, executor_id)
    executor.task_count += 1
    executor.failed_tasks += int(not success)
    host = str(_value(task_info, "Host", "host", default=executor.host))
    if executor.host == "unknown" and host != "unknown":
        executor.host = host

    stage.tasks.append(
        TaskSummary(
            task_id=_integer(_value(task_info, "Task ID", "taskId")),
            index=_integer(_value(task_info, "Index", "index")),
            attempt=_integer(_value(task_info, "Attempt", "attempt")),
            executor_id=executor_id,
            host=host,
            duration_ms=max(0, duration),
            executor_run_time_ms=_integer(_value(metrics, "Executor Run Time", "executorRunTime")),
            executor_cpu_time_ns=_integer(_value(metrics, "Executor CPU Time", "executorCpuTime")),
            jvm_gc_time_ms=_integer(_value(metrics, "JVM GC Time", "jvmGCTime")),
            input_bytes=_integer(_value(input_metrics, "Bytes Read", "bytesRead")),
            input_records=_integer(_value(input_metrics, "Records Read", "recordsRead")),
            output_bytes=_integer(_value(output_metrics, "Bytes Written", "bytesWritten")),
            output_records=_integer(_value(output_metrics, "Records Written", "recordsWritten")),
            shuffle_read_bytes=(
                _integer(_value(shuffle_read, "Remote Bytes Read", "remoteBytesRead"))
                + _integer(_value(shuffle_read, "Local Bytes Read", "localBytesRead"))
            ),
            shuffle_read_records=_integer(
                _value(shuffle_read, "Total Records Read", "recordsRead")
            ),
            shuffle_write_bytes=_integer(
                _value(shuffle_write, "Shuffle Bytes Written", "bytesWritten")
            ),
            shuffle_write_records=_integer(
                _value(shuffle_write, "Shuffle Records Written", "recordsWritten")
            ),
            fetch_wait_ms=_integer(_value(shuffle_read, "Fetch Wait Time", "fetchWaitTime")),
            memory_spill_bytes=_integer(
                _value(metrics, "Memory Bytes Spilled", "memoryBytesSpilled")
            ),
            disk_spill_bytes=_integer(_value(metrics, "Disk Bytes Spilled", "diskBytesSpilled")),
            result_size_bytes=_integer(_value(metrics, "Result Size", "resultSize")),
            success=success,
            end_reason=end_reason,
            speculative=bool(_value(task_info, "Speculative", "speculative", default=False)),
        )
    )


def _correlate(summary: ApplicationSummary) -> None:
    for job in summary.jobs.values():
        for stage_id in job.stage_ids:
            for stage in summary.stage_attempts(stage_id):
                if job.job_id not in stage.job_ids:
                    stage.job_ids.append(job.job_id)
        if job.sql_execution_id is not None:
            execution = summary.sql_executions.setdefault(
                job.sql_execution_id,
                SQLExecutionSummary(execution_id=job.sql_execution_id),
            )
            if job.job_id not in execution.job_ids:
                execution.job_ids.append(job.job_id)
