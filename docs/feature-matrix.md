# V1.0 feature and test matrix

Spark Triage V1.0 is a clean Spark 4 implementation of the Spark-relevant concepts in Dr. Elephant
and SparkLens. "Ported" means the behavior or scenario is asserted against Spark Triage's own model;
it does not mean old Scala or Java source was copied.

## SparkLens analyzer coverage

| SparkLens concept | Spark Triage V1.0 | Test coverage |
| --- | --- | --- |
| AppTimeline | application/job/stage/executor intervals and driver gaps | parser, analytics |
| EfficiencyStatistics | task time, core capacity, utilization, critical path | analyzer, analytics |
| ExecutorTimeline | add/remove time, task metrics, removal RCA | parser, analyzer |
| ExecutorWallclock | dependency-aware executor-count simulation | analytics |
| HostTimeline | executor grouping and host intervals | analytics |
| JobOverlap | SQL/job groups and pairwise overlap | analytics |
| SimpleApp | application summary and versioned JSON | parser, CLI |
| StageOverlap | concurrent stage intervals per job | analytics |
| StageSkew | task/median ratio, stage ratio, I/O, GC, fetch, spill | analyzer, analytics |

Ported scenario families include interval union and overlap, maximum concurrent executors (including
100,000 intervals), serial and parallel stage DAG scheduling, multiple executor-count estimates,
and equivalent parser-to-report output.

## Dr. Elephant Spark heuristic coverage

| Dr. Elephant Spark heuristic | Spark Triage V1.0 |
| --- | --- |
| JobsHeuristic | per-job failure plus application job-failure rate |
| StagesHeuristic | stage failure/retry, task-failure rate, long stages, skew, spill, fetch wait |
| ExecutorsHeuristic | min/p25/median/p75/max distributions and max-to-median load skew |
| ExecutorGcHeuristic | stage GC pressure and executor-level GC skew |
| ConfigurationHeuristic | evidence-gated serializer and AQE recommendations |

The original rate boundaries (10%, 30%, 50%) and minimum materiality gates (five minutes for time,
100 MiB for byte distributions) informed the port. Recommendations were rewritten for Spark 4;
legacy assumptions such as always enabling an external shuffle service are not copied blindly.

## Native Spark Triage coverage

- Spark 4-only version gate and event inventory.
- SQL-to-executor correlation with stable run and workload IDs.
- Plain, gzip, and rolling event-log input.
- Cartesian-plan, incomplete-log, and unexpected executor-removal RCA.
- Cross-run median baselines for runtime, spill, and shuffle regressions.
- Deterministic evidence/recommendations and CI severity gates.

## Explicit exclusions

The following Dr. Elephant areas are unrelated to a Spark 4 diagnostic core and are not claimed as
ported: Hadoop MapReduce, Tez, Tony, Azkaban integration, Play UI, persistence/database, LDAP, and
scheduler tests. SparkLens's old Spark/Scala compatibility test is replaced by a strict Spark 4
event-format gate. Neither upstream project is bundled at runtime.

Upstream references:

- Dr. Elephant: <https://github.com/linkedin/dr-elephant>
- SparkLens: <https://github.com/qubole/sparklens>
