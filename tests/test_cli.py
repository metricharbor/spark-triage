import json
from pathlib import Path

from spark_triage.cli import main

FIXTURE = Path(__file__).parent / "fixtures" / "skewed_event_log.jsonl"


def test_cli_emits_json(capsys) -> None:
    exit_code = main(["analyze", str(FIXTURE), "--format", "json"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["application"]["name"] == "checkout-etl"
    assert len(payload["findings"]) == 2


def test_cli_can_fail_on_warning(capsys) -> None:
    exit_code = main(["analyze", str(FIXTURE), "--fail-on", "warning"])

    assert exit_code == 2
    assert "DATA_SKEW" in capsys.readouterr().out

