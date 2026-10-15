from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from spark_triage import __version__
from spark_triage.analyzer import analyze
from spark_triage.models import DiagnosticReport
from spark_triage.parser import EventLogError, parse_event_log

SEVERITY_RANK = {"warning": 1, "critical": 2}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spark-triage",
        description="Analyze Apache Spark event logs for evidence-backed findings.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    analyze_parser = subparsers.add_parser("analyze", help="Analyze one Spark event log")
    analyze_parser.add_argument("event_log", help="Path to a newline-delimited Spark event log")
    analyze_parser.add_argument("--format", choices=("text", "json"), default="text")
    analyze_parser.add_argument("--skew-ratio", type=float, default=3.0)
    analyze_parser.add_argument("--spill-threshold-mb", type=float, default=64.0)
    analyze_parser.add_argument(
        "--fail-on",
        choices=("warning", "critical"),
        help="Exit with code 2 when a finding at or above this severity is present",
    )
    return parser


def render_text(report: DiagnosticReport) -> str:
    application = report.application
    lines = [
        f"Application: {application.name} ({application.application_id})",
        (
            f"Stages: {len(application.stages)} | Tasks: {application.task_count} | "
            f"Findings: {len(report.findings)}"
        ),
    ]
    if not report.findings:
        lines.extend(["", "No findings matched the configured thresholds."])
    for finding in report.findings:
        stage = f" stage={finding.stage_id}" if finding.stage_id is not None else ""
        lines.extend(
            ["", f"[{finding.severity.upper()}] {finding.rule_id}{stage}", f"  {finding.message}"]
        )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        application = parse_event_log(args.event_log)
        report = analyze(
            application,
            skew_ratio=args.skew_ratio,
            spill_threshold_bytes=int(args.spill_threshold_mb * 1024 * 1024),
        )
    except (EventLogError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print(render_text(report))

    if args.fail_on:
        threshold = SEVERITY_RANK[args.fail_on]
        if any(SEVERITY_RANK[finding.severity] >= threshold for finding in report.findings):
            return 2
    return 0


def entrypoint() -> None:
    raise SystemExit(main())

