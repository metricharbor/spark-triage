from pathlib import Path

from spark_triage.analyzer import analyze
from spark_triage.parser import parse_event_log

FIXTURE = Path(__file__).parent / "fixtures" / "skewed_event_log.jsonl"


def test_analyze_detects_skew_and_spill() -> None:
    report = analyze(parse_event_log(FIXTURE))

    assert [finding.rule_id for finding in report.findings] == [
        "DATA_SKEW",
        "EXCESSIVE_SPILL",
    ]
    skew = report.findings[0]
    assert skew.stage_id == 7
    assert skew.details["ratio"] == 6.0


def test_thresholds_are_configurable() -> None:
    report = analyze(
        parse_event_log(FIXTURE),
        skew_ratio=10.0,
        spill_threshold_bytes=100 * 1024 * 1024,
    )

    assert report.findings == []

