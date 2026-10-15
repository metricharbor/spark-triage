from pathlib import Path

import pytest

from spark_triage.parser import EventLogError, parse_event_log

FIXTURE = Path(__file__).parent / "fixtures" / "skewed_event_log.jsonl"


def test_parse_event_log_aggregates_stage_metrics() -> None:
    application = parse_event_log(FIXTURE)

    assert application.name == "checkout-etl"
    assert application.application_id == "app-20261015-0001"
    assert application.task_count == 5
    stage = application.stages[7]
    assert stage.name == "aggregateByKey"
    assert stage.task_durations_ms == [1000, 1100, 900, 1000, 6000]
    assert stage.input_bytes == 14_000
    assert stage.shuffle_read_bytes == 28_500
    assert stage.shuffle_write_bytes == 42_000
    assert stage.memory_spill_bytes == 80 * 1024 * 1024


def test_parse_event_log_reports_bad_json_line(tmp_path: Path) -> None:
    event_log = tmp_path / "bad.jsonl"
    event_log.write_text('{"Event":"ok"}\nnot-json\n', encoding="utf-8")

    with pytest.raises(EventLogError, match="line 2"):
        parse_event_log(event_log)

