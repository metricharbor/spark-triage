from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from spark_triage import __version__
from spark_triage.analyzer import analyze
from spark_triage.baseline import compare as compare_baseline
from spark_triage.baseline import record as record_baseline
from spark_triage.models import ApplicationSummary, DiagnosticReport
from spark_triage.parser import EventLogError, parse_event_log

SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spark-triage",
        description="Replay Spark 4 event logs into evidence-backed diagnostics.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_parser = subparsers.add_parser(
        "inspect", help="Validate a Spark 4 event log and show captured event coverage"
    )
    inspect_parser.add_argument("event_log", help="Event-log file or rolling log directory")
    inspect_parser.add_argument("--format", choices=("text", "json"), default="text")

    analyze_parser = subparsers.add_parser(
        "analyze", help="Correlate events and run deterministic RCA rules"
    )
    analyze_parser.add_argument("event_log", help="Event-log file or rolling log directory")
    analyze_parser.add_argument("--format", choices=("text", "json"), default="text")
    analyze_parser.add_argument("--workload-id", help="Stable ID shared by repeated runs")
    analyze_parser.add_argument("--skew-ratio", type=float, default=3.0)
    analyze_parser.add_argument("--input-skew-ratio", type=float, default=4.0)
    analyze_parser.add_argument("--spill-threshold-mb", type=float, default=64.0)
    analyze_parser.add_argument(
        "--baseline-store",
        help="JSON baseline store used for cross-run anomaly detection",
    )
    analyze_parser.add_argument(
        "--record-baseline",
        action="store_true",
        help="Record this run after analysis; requires --baseline-store",
    )
    analyze_parser.add_argument(
        "--fail-on",
        choices=("warning", "critical"),
        help="Exit with code 2 when a finding at or above this severity is present",
    )
    return parser


def render_text(report: DiagnosticReport) -> str:
    application = report.application
    lines = [
        f"Application: {application.name}",
        f"Run ID: {application.run_id}",
        f"Workload ID: {application.workload_id}",
        f"Spark: {application.replay.spark_version}",
        (
            f"SQL: {len(application.sql_executions)} | Jobs: {len(application.jobs)} | "
            f"Stages: {len(application.stages)} | Tasks: {application.task_count} | "
            f"Executors: {len(application.executors)}"
        ),
        f"Findings: {len(report.findings)}",
    ]
    if not report.findings:
        lines.extend(["", "No findings matched the configured thresholds."])
    for finding in report.findings:
        context = []
        if finding.sql_execution_ids:
            context.append("sql=" + ",".join(map(str, finding.sql_execution_ids)))
        if finding.job_ids:
            context.append("jobs=" + ",".join(map(str, finding.job_ids)))
        if finding.stage_id is not None:
            context.append(f"stage={finding.stage_id}")
        if finding.executor_ids:
            context.append("executors=" + ",".join(finding.executor_ids))
        suffix = " " + " ".join(context) if context else ""
        lines.extend(
            [
                "",
                f"[{finding.severity.upper()}] {finding.rule_id}{suffix}",
                f"  {finding.title}: {finding.message}",
            ]
        )
        if finding.recommendation:
            lines.append(f"  Next: {finding.recommendation}")
    return "\n".join(lines)


def render_inspection(application: ApplicationSummary) -> str:
    replay = application.replay
    lines = [
        f"Spark version: {replay.spark_version} (supported)",
        f"Application: {application.name} ({application.run_id})",
        f"Workload ID: {application.workload_id}",
        f"Sources: {len(replay.source_paths)} files, {replay.source_bytes} bytes",
        f"Events: {replay.event_count}",
        "",
        "Event coverage:",
    ]
    lines.extend(
        f"  {event_type}: {count}" for event_type, count in sorted(replay.event_counts.items())
    )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if getattr(args, "record_baseline", False) and not args.baseline_store:
        print("error: --record-baseline requires --baseline-store", file=sys.stderr)
        return 1

    try:
        application = parse_event_log(args.event_log)
        if args.command == "inspect":
            if args.format == "json":
                payload = {
                    "replay": application.replay.to_dict(),
                    "application": application.to_dict(),
                }
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print(render_inspection(application))
            return 0

        if args.workload_id:
            application.workload_id = args.workload_id
        report = analyze(
            application,
            skew_ratio=args.skew_ratio,
            input_skew_ratio=args.input_skew_ratio,
            spill_threshold_bytes=int(args.spill_threshold_mb * 1024 * 1024),
        )
        if args.baseline_store:
            report.findings.extend(compare_baseline(application, args.baseline_store))
        if args.record_baseline:
            record_baseline(application, args.baseline_store)
    except (EventLogError, OSError, ValueError) as exc:
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
