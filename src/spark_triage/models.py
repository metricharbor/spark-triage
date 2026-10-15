from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class StageSummary:
    stage_id: int
    name: str = "unknown"
    expected_tasks: int | None = None
    task_durations_ms: list[int] = field(default_factory=list)
    failed_tasks: int = 0
    input_bytes: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    memory_spill_bytes: int = 0
    disk_spill_bytes: int = 0

    @property
    def task_count(self) -> int:
        return len(self.task_durations_ms)

    @property
    def total_spill_bytes(self) -> int:
        return self.memory_spill_bytes + self.disk_spill_bytes


@dataclass
class ApplicationSummary:
    name: str = "unknown"
    application_id: str = "unknown"
    started_at_ms: int | None = None
    ended_at_ms: int | None = None
    stages: dict[int, StageSummary] = field(default_factory=dict)

    @property
    def task_count(self) -> int:
        return sum(stage.task_count for stage in self.stages.values())


@dataclass(frozen=True)
class Finding:
    rule_id: str
    severity: str
    message: str
    stage_id: int | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class DiagnosticReport:
    application: ApplicationSummary
    findings: list[Finding]

    def to_dict(self) -> dict[str, Any]:
        stages = [asdict(stage) for _, stage in sorted(self.application.stages.items())]
        return {
            "application": {
                "name": self.application.name,
                "application_id": self.application.application_id,
                "started_at_ms": self.application.started_at_ms,
                "ended_at_ms": self.application.ended_at_ms,
                "stage_count": len(stages),
                "task_count": self.application.task_count,
                "stages": stages,
            },
            "findings": [asdict(finding) for finding in self.findings],
        }

