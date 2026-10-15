from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any

SCHEMA_VERSION = "spark-triage/v1"


@dataclass(frozen=True)
class TaskSummary:
    task_id: int
    index: int
    attempt: int
    executor_id: str
    host: str
    duration_ms: int
    executor_run_time_ms: int
    executor_cpu_time_ns: int
    jvm_gc_time_ms: int
    input_bytes: int
    input_records: int
    output_bytes: int
    output_records: int
    shuffle_read_bytes: int
    shuffle_read_records: int
    shuffle_write_bytes: int
    shuffle_write_records: int
    fetch_wait_ms: int
    memory_spill_bytes: int
    disk_spill_bytes: int
    result_size_bytes: int
    success: bool
    end_reason: str
    speculative: bool = False


@dataclass
class StageSummary:
    stage_id: int
    attempt_id: int = 0
    name: str = "unknown"
    expected_tasks: int | None = None
    submission_time_ms: int | None = None
    completion_time_ms: int | None = None
    status: str = "UNKNOWN"
    failure_reason: str | None = None
    parent_ids: list[int] = field(default_factory=list)
    job_ids: list[int] = field(default_factory=list)
    tasks: list[TaskSummary] = field(default_factory=list)

    @property
    def key(self) -> tuple[int, int]:
        return (self.stage_id, self.attempt_id)

    @property
    def task_count(self) -> int:
        return len(self.tasks)

    @property
    def task_durations_ms(self) -> list[int]:
        return [task.duration_ms for task in self.tasks]

    @property
    def failed_tasks(self) -> int:
        return sum(not task.success for task in self.tasks)

    @property
    def input_bytes(self) -> int:
        return sum(task.input_bytes for task in self.tasks)

    @property
    def output_bytes(self) -> int:
        return sum(task.output_bytes for task in self.tasks)

    @property
    def shuffle_read_bytes(self) -> int:
        return sum(task.shuffle_read_bytes for task in self.tasks)

    @property
    def shuffle_write_bytes(self) -> int:
        return sum(task.shuffle_write_bytes for task in self.tasks)

    @property
    def memory_spill_bytes(self) -> int:
        return sum(task.memory_spill_bytes for task in self.tasks)

    @property
    def disk_spill_bytes(self) -> int:
        return sum(task.disk_spill_bytes for task in self.tasks)

    @property
    def total_spill_bytes(self) -> int:
        return self.memory_spill_bytes + self.disk_spill_bytes

    @property
    def executor_run_time_ms(self) -> int:
        return sum(task.executor_run_time_ms for task in self.tasks)

    @property
    def executor_cpu_time_ns(self) -> int:
        return sum(task.executor_cpu_time_ns for task in self.tasks)

    @property
    def jvm_gc_time_ms(self) -> int:
        return sum(task.jvm_gc_time_ms for task in self.tasks)

    @property
    def fetch_wait_ms(self) -> int:
        return sum(task.fetch_wait_ms for task in self.tasks)

    @property
    def result_size_bytes(self) -> int:
        return sum(task.result_size_bytes for task in self.tasks)

    @property
    def duration_ms(self) -> int | None:
        if self.submission_time_ms is None or self.completion_time_ms is None:
            return None
        return max(0, self.completion_time_ms - self.submission_time_ms)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "attempt_id": self.attempt_id,
            "name": self.name,
            "status": self.status,
            "failure_reason": self.failure_reason,
            "parent_ids": self.parent_ids,
            "job_ids": sorted(self.job_ids),
            "expected_tasks": self.expected_tasks,
            "task_count": self.task_count,
            "failed_tasks": self.failed_tasks,
            "submission_time_ms": self.submission_time_ms,
            "completion_time_ms": self.completion_time_ms,
            "duration_ms": self.duration_ms,
            "executor_run_time_ms": self.executor_run_time_ms,
            "executor_cpu_time_ns": self.executor_cpu_time_ns,
            "jvm_gc_time_ms": self.jvm_gc_time_ms,
            "input_bytes": self.input_bytes,
            "output_bytes": self.output_bytes,
            "shuffle_read_bytes": self.shuffle_read_bytes,
            "shuffle_write_bytes": self.shuffle_write_bytes,
            "fetch_wait_ms": self.fetch_wait_ms,
            "memory_spill_bytes": self.memory_spill_bytes,
            "disk_spill_bytes": self.disk_spill_bytes,
            "result_size_bytes": self.result_size_bytes,
        }


@dataclass
class JobSummary:
    job_id: int
    stage_ids: list[int] = field(default_factory=list)
    submission_time_ms: int | None = None
    completion_time_ms: int | None = None
    status: str = "UNKNOWN"
    result: str | None = None
    sql_execution_id: int | None = None
    properties: dict[str, str] = field(default_factory=dict)

    @property
    def duration_ms(self) -> int | None:
        if self.submission_time_ms is None or self.completion_time_ms is None:
            return None
        return max(0, self.completion_time_ms - self.submission_time_ms)


@dataclass
class SQLExecutionSummary:
    execution_id: int
    root_execution_id: int | None = None
    query_id: str | None = None
    description: str = "unknown"
    details: str = ""
    physical_plan_description: str = ""
    plan_operators: list[str] = field(default_factory=list)
    metric_names: dict[int, str] = field(default_factory=dict)
    metric_values: dict[int, int] = field(default_factory=dict)
    start_time_ms: int | None = None
    end_time_ms: int | None = None
    error_message: str | None = None
    job_ids: list[int] = field(default_factory=list)

    @property
    def duration_ms(self) -> int | None:
        if self.start_time_ms is None or self.end_time_ms is None:
            return None
        return max(0, self.end_time_ms - self.start_time_ms)


@dataclass
class ExecutorSummary:
    executor_id: str
    host: str = "unknown"
    total_cores: int = 0
    added_at_ms: int | None = None
    removed_at_ms: int | None = None
    removed_reason: str | None = None
    task_count: int = 0
    failed_tasks: int = 0


@dataclass
class ReplayMetadata:
    spark_version: str = "unknown"
    source_paths: list[str] = field(default_factory=list)
    source_bytes: int = 0
    event_counts: Counter[str] = field(default_factory=Counter)

    @property
    def event_count(self) -> int:
        return sum(self.event_counts.values())

    def to_dict(self) -> dict[str, Any]:
        return {
            "spark_version": self.spark_version,
            "source_paths": self.source_paths,
            "source_bytes": self.source_bytes,
            "event_count": self.event_count,
            "event_counts": dict(sorted(self.event_counts.items())),
        }


@dataclass
class ApplicationSummary:
    name: str = "unknown"
    application_id: str = "unknown"
    attempt_id: str | None = None
    user: str | None = None
    workload_id: str = "unknown"
    started_at_ms: int | None = None
    ended_at_ms: int | None = None
    exit_code: int | None = None
    properties: dict[str, str] = field(default_factory=dict)
    stages: dict[tuple[int, int], StageSummary] = field(default_factory=dict)
    jobs: dict[int, JobSummary] = field(default_factory=dict)
    sql_executions: dict[int, SQLExecutionSummary] = field(default_factory=dict)
    executors: dict[str, ExecutorSummary] = field(default_factory=dict)
    replay: ReplayMetadata = field(default_factory=ReplayMetadata)

    @property
    def run_id(self) -> str:
        if self.attempt_id:
            return f"{self.application_id}/{self.attempt_id}"
        return self.application_id

    @property
    def task_count(self) -> int:
        return sum(stage.task_count for stage in self.stages.values())

    @property
    def duration_ms(self) -> int | None:
        if self.started_at_ms is None or self.ended_at_ms is None:
            return None
        return max(0, self.ended_at_ms - self.started_at_ms)

    def stage_attempts(self, stage_id: int) -> list[StageSummary]:
        return [stage for key, stage in sorted(self.stages.items()) if key[0] == stage_id]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "application_id": self.application_id,
            "attempt_id": self.attempt_id,
            "run_id": self.run_id,
            "workload_id": self.workload_id,
            "user": self.user,
            "started_at_ms": self.started_at_ms,
            "ended_at_ms": self.ended_at_ms,
            "duration_ms": self.duration_ms,
            "exit_code": self.exit_code,
            "stage_count": len(self.stages),
            "job_count": len(self.jobs),
            "sql_execution_count": len(self.sql_executions),
            "executor_count": len(self.executors),
            "task_count": self.task_count,
            "properties": dict(sorted(self.properties.items())),
            "jobs": [
                asdict(job) | {"duration_ms": job.duration_ms}
                for job in sorted(self.jobs.values(), key=lambda item: item.job_id)
            ],
            "stages": [
                stage.to_dict() for stage in sorted(self.stages.values(), key=lambda item: item.key)
            ],
            "sql_executions": [
                asdict(execution) | {"duration_ms": execution.duration_ms}
                for execution in sorted(
                    self.sql_executions.values(), key=lambda item: item.execution_id
                )
            ],
            "executors": [
                asdict(executor)
                for executor in sorted(self.executors.values(), key=lambda item: item.executor_id)
            ],
        }


@dataclass(frozen=True)
class Finding:
    rule_id: str
    severity: str
    title: str
    message: str
    scope: str = "application"
    stage_id: int | None = None
    job_ids: tuple[int, ...] = ()
    sql_execution_ids: tuple[int, ...] = ()
    executor_ids: tuple[str, ...] = ()
    evidence: dict[str, Any] = field(default_factory=dict)
    recommendation: str = ""


@dataclass
class DiagnosticReport:
    application: ApplicationSummary
    findings: list[Finding]
    analytics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "replay": self.application.replay.to_dict(),
            "application": self.application.to_dict(),
            "analytics": self.analytics,
            "findings": [asdict(finding) for finding in self.findings],
        }
