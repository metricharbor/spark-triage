# Architecture and delivery boundary

## V1.0 data path

```text
Spark 4 event log (file, gzip, rolling directory)
        |
        v
version gate + replay inventory
        |
        v
SQL -> Job -> Stage attempt -> Task attempt -> Executor/Host
        |                         |
        +------ analytics --------+
        |   timelines, overlap, critical path, distributions, scaling
        v
deterministic RCA rules + workload baseline comparison
        |
        v
human text / spark-triage/v1 JSON / CI exit code
```

The parser stores only an allowlist of Spark properties so reports do not accidentally echo secrets.
The evidence layer is deterministic; a future AI explanation layer must consume these facts rather
than invent metrics.

## Runtime track after V1.0

The offline engine is deliberately reusable by three future Spark 4 integrations:

1. A thread-safe `QueryExecutionListener` captures successful and failed query executions.
2. `SparkSessionExtensions` adds read-only resolution and optimizer inspection rules. Rules must not
   change query results.
3. A `SparkPlugin`/listener streams selected lifecycle and task events to the same normalized model.

Runtime events will use `spark.triage.workload.id` plus application/attempt/query IDs. This keeps
online observations and offline replay comparable and enables anomaly detection without binding the
diagnostic engine to a particular cluster manager or hosted backend.
