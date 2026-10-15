import json
from pathlib import Path

from spark_triage.cli import main

FIXTURE = Path(__file__).parent / "fixtures" / "spark_4_event_log.jsonl"


def test_cli_inspects_event_coverage(capsys) -> None:
    assert main(["inspect", str(FIXTURE)]) == 0
    output = capsys.readouterr().out
    assert "Spark version: 4.0.0 (supported)" in output
    assert "SparkListenerTaskEnd: 5" in output


def test_cli_emits_versioned_report_with_analytics(capsys) -> None:
    assert main(["analyze", str(FIXTURE), "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "spark-triage/v1"
    assert payload["application"]["workload_id"] == "checkout-daily"
    assert payload["analytics"]["efficiency"]["critical_path_ms"] == 94_000
    assert "TASK_DURATION_SKEW" in {item["rule_id"] for item in payload["findings"]}


def test_cli_can_fail_ci_and_record_baseline(capsys, tmp_path: Path) -> None:
    store = tmp_path / "baselines.json"
    exit_code = main(
        [
            "analyze",
            str(FIXTURE),
            "--baseline-store",
            str(store),
            "--record-baseline",
            "--fail-on",
            "warning",
        ]
    )
    assert exit_code == 2
    assert store.exists()
    assert "TASK_DURATION_SKEW" in capsys.readouterr().out


def test_cli_rejects_record_without_store(capsys) -> None:
    assert main(["analyze", str(FIXTURE), "--record-baseline"]) == 1
    assert "requires --baseline-store" in capsys.readouterr().err
