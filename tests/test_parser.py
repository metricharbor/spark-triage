import gzip
from pathlib import Path

import pytest

from spark_triage.parser import EventLogError, parse_event_log

FIXTURE = Path(__file__).parent / "fixtures" / "spark_4_event_log.jsonl"


def test_parse_spark_4_event_log_builds_execution_graph() -> None:
    application = parse_event_log(FIXTURE)

    assert application.name == "checkout-etl"
    assert application.run_id == "app-20261015-0001"
    assert application.workload_id == "checkout-daily"
    assert application.replay.spark_version == "4.0.0"
    assert application.task_count == 5
    assert application.jobs[1].sql_execution_id == 42
    assert application.sql_executions[42].job_ids == [1]
    assert application.sql_executions[42].plan_operators == [
        "HashAggregate",
        "Exchange",
        "Scan parquet checkout",
    ]
    assert application.sql_executions[42].metric_names[101] == "shuffle bytes written"
    assert application.sql_executions[42].metric_values[101] == 251_658_240
    stage = application.stages[(7, 0)]
    assert stage.job_ids == [1]
    assert stage.task_durations_ms == [10_000, 11_000, 10_000, 10_000, 70_000]
    assert stage.input_bytes == 251_658_240
    assert stage.total_spill_bytes == 128 * 1024 * 1024


def test_parse_gzip_and_rolling_logs(tmp_path: Path) -> None:
    content = FIXTURE.read_bytes()
    compressed = tmp_path / "events.jsonl.gz"
    with gzip.open(compressed, "wb") as handle:
        handle.write(content)
    assert parse_event_log(compressed).task_count == 5

    rolling = tmp_path / "rolling"
    rolling.mkdir()
    lines = FIXTURE.read_text(encoding="utf-8").splitlines(keepends=True)
    (rolling / "events_1").write_text("".join(lines[:8]), encoding="utf-8")
    (rolling / "events_2").write_text("".join(lines[8:]), encoding="utf-8")
    replayed = parse_event_log(rolling)
    assert replayed.task_count == 5
    assert len(replayed.replay.source_paths) == 2


def test_rejects_non_spark_4_and_unverifiable_logs(tmp_path: Path) -> None:
    old = tmp_path / "old.jsonl"
    old.write_text('{"Event":"SparkListenerLogStart","Spark Version":"3.5.0"}\n')
    with pytest.raises(EventLogError, match="only Spark 4.x"):
        parse_event_log(old)

    missing = tmp_path / "missing.jsonl"
    missing.write_text('{"Event":"SparkListenerApplicationEnd","Timestamp":1}\n')
    with pytest.raises(EventLogError, match="SparkListenerLogStart is missing"):
        parse_event_log(missing)


def test_reports_bad_json_line(tmp_path: Path) -> None:
    event_log = tmp_path / "bad.jsonl"
    event_log.write_text('{"Event":"ok"}\nnot-json\n', encoding="utf-8")
    with pytest.raises(EventLogError, match="line 2"):
        parse_event_log(event_log)
