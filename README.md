# Spark Triage

[![CI](https://github.com/metricharbor/spark-triage/actions/workflows/ci.yml/badge.svg)](https://github.com/metricharbor/spark-triage/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB.svg)](https://www.python.org/)

Spark Triage is an open-source, offline-first CLI that turns Apache Spark event logs into
evidence-backed performance and reliability findings.

The first release focuses on a narrow promise: point the CLI at an event log and quickly find
the stages that deserve an engineer's attention. It currently detects:

- task-duration skew within a stage;
- excessive memory or disk spill;
- failed task attempts; and
- stage-level task, input, and shuffle metrics.

No cluster credentials or hosted service are required. Event logs stay on the machine where the
CLI runs.

## Quick start

```bash
git clone https://github.com/metricharbor/spark-triage.git
cd spark-triage
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -e .
spark-triage analyze /path/to/spark-event-log --format text
```

For machine-readable output:

```bash
spark-triage analyze /path/to/spark-event-log --format json
```

Example text output:

```text
Application: checkout-etl (app-20261015-0001)
Stages: 1 | Tasks: 5 | Findings: 2

[WARNING] DATA_SKEW stage=7
  Slowest task took 6000 ms, 6.0x the stage median of 1000 ms.
[WARNING] EXCESSIVE_SPILL stage=7
  Stage spilled 83886080 bytes while processing shuffle data.
```

Exit codes are `0` for a completed analysis, `1` for invalid input, and `2` when `--fail-on`
matches a finding. This makes the CLI usable in CI and scheduled diagnostics:

```bash
spark-triage analyze event-log.json --fail-on warning
```

## What the CLI reads

Spark event logs are newline-delimited JSON files produced by Spark's event logging system. Enable
them with `spark.eventLog.enabled=true`, then pass a completed event log to Spark Triage.

The parser is intentionally tolerant of missing optional metrics, but reports malformed JSON with
the exact line number. Compressed logs and rolling event-log directories are not supported yet.

## Diagnostic model

Every finding contains a rule identifier, severity, stage ID, human-readable evidence, and
structured details. The initial rules are deterministic and inspectable:

| Rule | Default trigger |
| --- | --- |
| `DATA_SKEW` | At least 4 tasks and the slowest task is at least 3x the median and 1s slower |
| `EXCESSIVE_SPILL` | Memory plus disk spill exceeds 64 MiB in one stage |
| `FAILED_TASKS` | One or more task attempts did not finish with `Success` |

Use `--skew-ratio` and `--spill-threshold-mb` to tune the first two rules.

## Development

```bash
python -m pip install -e ".[dev]"
ruff check .
pytest
```

The test fixture in `tests/fixtures/` is synthetic and contains no production or customer data.

## Roadmap

- Spark SQL execution and operator-level metrics
- compressed and rolling event logs
- executor-loss and out-of-memory diagnosis
- history-server ingestion
- comparison across repeated application runs
- optional AI-generated explanations built on the deterministic evidence layer

See the [issue tracker](https://github.com/metricharbor/spark-triage/issues) for the first milestone.

## Contributing

Issues and focused pull requests are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before
submitting a change.

## License

Licensed under the [Apache License 2.0](LICENSE).

