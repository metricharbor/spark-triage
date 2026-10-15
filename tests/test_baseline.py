from pathlib import Path

from spark_triage.baseline import compare, load_store, record
from spark_triage.parser import parse_event_log

FIXTURE = Path(__file__).parent / "fixtures" / "spark_4_event_log.jsonl"


def test_records_idempotent_workload_history_and_detects_runtime_regression(
    tmp_path: Path,
) -> None:
    store = tmp_path / "baselines.json"
    for index in range(3):
        app = parse_event_log(FIXTURE)
        app.application_id = f"baseline-{index}"
        app.started_at_ms = index * 1_000_000
        app.ended_at_ms = app.started_at_ms + 120_000
        record(app, store)
        record(app, store)

    assert len(load_store(store)["workloads"]["checkout-daily"]) == 3

    current = parse_event_log(FIXTURE)
    current.application_id = "regressed"
    current.started_at_ms = 4_000_000
    current.ended_at_ms = 4_400_000
    rules = {finding.rule_id for finding in compare(current, store)}
    assert "RUNTIME_REGRESSION" in rules
