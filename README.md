# Spark Triage

[![CI](https://github.com/metricharbor/spark-triage/actions/workflows/ci.yml/badge.svg)](https://github.com/metricharbor/spark-triage/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Spark-4.x-E25A1C.svg)](https://spark.apache.org/)

Spark Triage is a Spark 4-native diagnostic engine for event logs. It rebuilds a queryable
`SQL -> Job -> Stage -> Task -> Executor` execution graph, calculates timelines and efficiency,
and emits deterministic root-cause findings with evidence and recommended next actions.

Version 1.0 combines the Spark-specific diagnostic ideas of
[Dr. Elephant](https://github.com/linkedin/dr-elephant) and
[SparkLens](https://github.com/qubole/sparklens) in a small, dependency-free Python core. It is a
clean Spark 4 implementation, not a source-code concatenation or a replacement for Spark History
Server.

## What V1.0 does

- Replays plain, gzip, and rolling Spark 4 event logs.
- Correlates SQL executions, jobs, stage attempts, task attempts, executors, and hosts.
- Reports application, job, stage, executor, and host timelines.
- Computes job overlap, stage overlap, critical path, driver gaps, executor utilization, task skew,
  I/O, shuffle, spill, GC, and executor distributions.
- Simulates dependency-aware job wall-clock time at seven executor-count levels.
- Diagnoses failures, retries, task/input skew, spill, GC pressure, shuffle wait, executor loss,
  Cartesian joins, low utilization, uneven executor load, and relevant Spark configuration.
- Tracks a stable workload ID and detects runtime, spill, and shuffle regressions against previous
  runs.
- Produces a versioned JSON contract and CI-friendly exit codes.

See the exact [feature and legacy-test mapping](docs/feature-matrix.md).

## Quick start

```bash
git clone https://github.com/metricharbor/spark-triage.git
cd spark-triage
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -e .

spark-triage inspect /path/to/event-log
spark-triage analyze /path/to/event-log --format text
spark-triage analyze /path/to/rolling-event-log-directory --format json
```

Spark Triage intentionally accepts Spark 4.x logs only. `inspect` validates the version and shows
which event types were captured before any diagnosis is trusted.

## Repeated-run anomaly detection

Set `spark.triage.workload.id` in the Spark properties, or pass `--workload-id`, so repeated
application IDs map to one logical workload:

```bash
spark-triage analyze events.json \
  --workload-id checkout-daily \
  --baseline-store .spark-triage/baselines.json \
  --record-baseline
```

After three prior runs, the next analysis compares runtime, spill, and shuffle volume with the
historical median. The local baseline store retains the latest 50 runs per workload.

## Automation contract

```bash
spark-triage analyze events.json --format json --fail-on warning
```

Exit codes are:

- `0`: analysis completed and did not cross `--fail-on`;
- `1`: invalid input, unsupported Spark version, or invalid options;
- `2`: at least one finding crossed the requested severity.

JSON output uses schema `spark-triage/v1` and contains `replay`, `application`, `analytics`, and
`findings`. Every finding includes its scope, correlated IDs, measured evidence, and a next action.

## History Server and Spark Triage

Spark History Server replays event logs to reconstruct Spark's UI. Spark Triage consumes the same
kind of persisted evidence but answers a different question: *what is abnormal, why is it likely
happening, and what should an engineer inspect next?* The two tools are complementary.

V1.0 is offline-first and does not require cluster credentials. Event logs and baselines stay on
the machine where the CLI runs. Spark listener, SQL plan-hook, and History Server ingestion are
the next delivery track; see [architecture](docs/architecture.md).

## Development

```bash
python -m pip install -e ".[dev]"
ruff check .
pytest
```

The test suite ports the relevant semantic scenarios from the two legacy projects and adds Spark 4
fixtures for replay, correlation, gzip/rolling logs, RCA, baseline anomalies, and the CLI contract.
Fixtures are synthetic and contain no production data.

## Attribution and scope

Dr. Elephant and SparkLens are Apache-2.0 projects. Spark Triage reimplements their Spark-relevant
analysis concepts against a new Spark 4 model. No upstream source file is vendored. Dr. Elephant's
MapReduce, Tez, Tony, web UI, database, and scheduler tests are outside this repository's Spark-only
scope. The detailed mapping is documented in [docs/feature-matrix.md](docs/feature-matrix.md).

## Contributing and license

Issues and focused pull requests are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md). Spark Triage
is licensed under the [Apache License 2.0](LICENSE).
