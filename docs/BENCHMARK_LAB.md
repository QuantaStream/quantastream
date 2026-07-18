# Benchmark Lab

The Benchmark Lab is the performance counterpart to the MySQL Compatibility
Lab. Compatibility answers whether QuantaStream behaves like MySQL for a
supported SQL surface. Benchmarking answers how QuantaStream performs under a
controlled deployment and workload.

Keep these signals separate. Correctness failures belong in compatibility or
SQL roadmap suites. Timing comparisons belong here only when the environment is
controlled enough to produce useful evidence.

## Goals

1. Measure QuantaStream performance under repeatable deployment profiles.
2. Compare QuantaStream and stock MySQL only when both systems run in comparable
   environments.
3. Preserve enough environment metadata that a result can be interpreted later.
4. Separate local directional runs from marketing-grade benchmark evidence.

## Non-Goals

- Do not use GitHub-hosted runner timing as benchmark evidence.
- Do not mix compatibility pass/fail decisions with benchmark pass/fail
  decisions.
- Do not tune MySQL or QuantaStream in undocumented one-off ways.
- Do not publish single-run wall-clock numbers as representative performance.

## Benchmark Tiers

### Tier 1: CI Compatibility

Purpose: correctness and smoke signal.

Environment: GitHub-hosted runners or similarly noisy shared runners.

Use for:

- SQLRunner parse and execution smoke tests
- compatibility report generation
- regression checks that do not depend on stable timing

Do not use for public timing claims.

### Tier 2: Developer Benchmark

Purpose: directional performance signal while developing.

Environment: laptop, workstation, or ad hoc cloud instance.

Use for:

- identifying obvious regressions
- validating that an optimization moves the right direction
- collecting preliminary query timings before a reference run

Treat results as local evidence only. Record hardware and deployment notes when
sharing numbers.

### Tier 3: Reference Benchmark

Purpose: repeatable benchmark evidence suitable for technical marketing or
release notes.

Environment: same cloud provider, region, instance family, storage class, and
network placement for QuantaStream and the comparison MySQL instance.

Required controls:

- Same cloud provider and region.
- Comparable CPU, memory, and storage classes.
- Benchmark runner on the same private network or equivalently documented
  network path.
- Fixed dataset and loader procedure.
- Warm-up runs separated from measured runs.
- Repeated measured runs with median and tail values, not a single sample.
- Explicit QuantaStream deployment mode, such as `inabox-standard`.
- Explicit MySQL version and configuration profile.

## Reference Deployment Shape

A useful apples-to-apples comparison is:

- QuantaStream deployed in `inabox-standard` mode.
- MySQL deployed in the same provider, region, and instance class.
- The benchmark runner deployed beside both systems on the same private network.
- The same logical dataset loaded into both systems.
- SQLRunner compatibility suites used for correctness gates before timing is
  interpreted.

This is not perfect equivalence because QuantaStream and MySQL use different
storage and execution models. It is, however, a disciplined comparison when the
purpose is to understand user-observable SQL behavior and response time.

## MySQL Reference Deployment Recipe

For a Tier 3 reference benchmark, deploy MySQL as a peer system rather than as a
local convenience process:

1. Choose one cloud provider, region, instance family, CPU count, memory size,
   storage class, and network placement.
2. Deploy QuantaStream using the `inabox-standard` profile on one instance.
3. Deploy stock MySQL on a matching instance in the same private network.
4. Run the benchmark driver from a third instance in the same placement group or
   from an explicitly documented equivalent network path.
5. Load the same logical dataset into both systems using recorded loader
   commands.
6. Run SQLRunner compatibility checks first. Treat benchmark numbers as invalid
   when correctness does not pass for the same suite and dataset.
7. Run warm-up passes, then repeated measured passes, and publish median and
   tail values with the recorded environment metadata.

MySQL configuration should be ordinary and documented. Avoid bespoke tuning
unless the same level of tuning is also documented for QuantaStream and the goal
of the run is explicitly a tuned-system comparison.

Once both systems are running and loaded, `sqlrunner/run-mysql-benchmark-compare.sh`
captures benchmark reports for stock MySQL and the selected QuantaStream target,
then renders a comparison with MySQL as the baseline:

```bash
cd sqlrunner
MYSQL_DSN='user:pass@tcp(mysql-host:3306)/tpch' \
TARGET_ENGINE=inabox-standard \
TARGET_HOST=127.0.0.1 \
TARGET_PORT=4000 \
SUITE_FILE=../tpc-h-benchmark/sqltests/tpch_queries.yaml \
BENCHMARK_RUNS=3 \
  ./run-mysql-benchmark-compare.sh
```

The script writes JSON reports and the markdown comparison under ignored local
paths by default. These artifacts are evidence for the current run, not source
controlled compatibility contracts.

## Measurement Rules

For every benchmark run, record:

- Git commit or release identifier.
- Deployment mode and topology.
- Cloud provider, region, instance type, and storage class.
- Dataset name, scale factor, and loader command.
- Suite name and exact command.
- Warm-up count and measured run count.
- Per-query median, p95 where available, and error count.
- Whether caches were cold, warm, or explicitly cleared.

A benchmark run should be reproducible from the recorded metadata without
relying on chat history or local memory.

## Relationship To SQLRunner

SQLRunner can support benchmark collection, but the compatibility lab should
remain correctness-first:

- `-capture_expected` and `-engine_diff` establish expected SQL behavior.
- `-compat_report` summarizes correctness-oriented compatibility categories.
- Timing comparison should be introduced as benchmark-lab reporting, not as a
  default compatibility failure mode.

The current scaffold supports `-benchmark_report`, `-benchmark_profile`,
`-benchmark_warmup`, `-benchmark_runs`, and `-benchmark_metadata` for normal
SQLRunner suite execution. `-benchmark_summary` renders one report, and
`-benchmark_compare` compares two or more reports with the first report as the
baseline. The helper scripts `sqlrunner/run-benchmark.sh`,
`sqlrunner/run-benchmark-compare.sh`, and
`sqlrunner/run-mysql-benchmark-compare.sh` wrap those flags for local developer
runs. These reports are benchmark artifacts, not compatibility verdicts.

Future benchmark tooling can add richer report formats and deployment metadata
collection. Those additions should preserve the boundary between correctness and
performance.

## Initial Work Items

1. Extend the benchmark report with host and storage metadata auto-detection for
   developer-local and `inabox-standard` runs.
2. Add a MySQL reference deployment template once the preferred cloud baseline is
   selected.
3. Promote a small read-only benchmark suite that is safe to repeat without
   mutating benchmark state.
4. Promote a controlled report archive convention once reference benchmark runs
   become repeatable enough to preserve.
5. Promote benchmark results only after correctness has passed for the same
   suite and dataset.
