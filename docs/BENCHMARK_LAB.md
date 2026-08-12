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

## Default Benchmark Suite

Benchmark helper scripts default to
`tpc-h-benchmark/sqltests/tpch_benchmark_readonly.yaml`. This suite is small,
read-only, and intentionally repeats a representative mix of the current engine
surface: seed counts, grouped lineitem aggregation, Q3-style graph aggregation,
Q5-style regional graph filtering, Q6/Q12/Q16/Q19 predicates, Q21 sibling
membership, and a date-function grouping case.

Use this suite for routine local and reference comparisons because it avoids
mutating benchmark state and keeps the signal focused. Use
`tpc-h-benchmark/sqltests/tpch_queries.yaml` as an explicit `SUITE_FILE`
override when the goal is a broader query-roadmap pass rather than a compact
benchmark checkpoint.

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
cd tpc-h-benchmark
MYSQL_HOST=mysql-host \
MYSQL_USER=bench \
MYSQL_PASSWORD='secret' \
MYSQL_DATABASE=tpch \
MYSQL_INDEX_PROFILE=benchmark \
  ./load-mysql-tpch.sh local/data/sf-0.01

cd sqlrunner
MYSQL_DSN='bench:secret@tcp(mysql-host:3306)/tpch' \
TARGET_ENGINE=inabox-standard \
TARGET_HOST=127.0.0.1 \
TARGET_PORT=4000 \
BENCHMARK_RUNS=3 \
  ./run-mysql-benchmark-compare.sh
```

The script writes JSON reports and the markdown comparison under ignored local
paths by default. These artifacts are evidence for the current run, not source
controlled compatibility contracts.

The benchmark helper scripts add common dataset metadata automatically. For
TPC-H suites, `dataset=tpch` is inferred from the suite path. Set
`BENCHMARK_SCALE_FACTOR`, `TPCH_SCALE_FACTOR`, or `SCALE_FACTOR` to record the
scale factor, and set `BENCHMARK_DATASET` to override the dataset label for
non-TPC-H workloads.

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

SQLRunner benchmark reports automatically add best-effort local metadata for
developer and `inabox-standard` runs, including host OS/architecture/CPU count,
Go version, current repository commit/branch/dirty status, working directory,
and the filesystem/mount information for the working directory when the host can
provide it. Explicit `-benchmark_metadata key=value` entries override
auto-detected values so reference runs can still record curated cloud/provider
metadata.

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
baseline. Benchmark comparisons include compact baseline and target metadata so
the generated `comparison.md` remains useful even when copied away from the
source JSON reports. The helper scripts `sqlrunner/run-benchmark.sh`,
`sqlrunner/run-benchmark-compare.sh`, and
`sqlrunner/run-mysql-benchmark-compare.sh` wrap those flags for local developer
runs. These reports are benchmark artifacts, not compatibility verdicts.

When `-capture_profile` is combined with repeated benchmark runs, each case
keeps the first captured profile in the historical `profile` field and records
all measured profiles in `profile_runs`. Use `profile_runs` for noisy query
analysis because run-to-run variance can move phase timings even when the case
median looks stable.

Future benchmark tooling can add richer report formats and deployment metadata
collection. Those additions should preserve the boundary between correctness and
performance.

## Checkpoint Notes

Detailed dated benchmark checkpoints live in
[`TPCH_BENCHMARK_NOTES.md`](TPCH_BENCHMARK_NOTES.md). These notes capture
engineering evidence, interpretation, and follow-up work for specific reference
runs. Keep durable methodology here in the Benchmark Lab document, and keep
run-specific conclusions in the checkpoint notes.

## Native Ingest Micro-Benchmarks

The standard-native loader path has a focused Go benchmark that mounts an
`inabox-standard` process, connects to its native gRPC node surface, routes
deterministic nested TPC-H order/lineitem envelopes through `SessionRouter`, and
reports loader throughput plus PutRow/flush profile totals:

```bash
cd /home/gmolinari/projects/quantastream/tpc-h-benchmark
ORDERS=100 \
LINEITEMS=4 \
SHARDS=1 \
RUNS=3 \
REPLAYS=1 \
PROFILE=standard-native-tpch-ingest \
  ./run-native-ingest-benchmark.sh
```

Set `PRIMARY_KEY_MODE=assume_new` only for validated fresh-load runs where the
input is known not to contain existing primary keys. The default
`verify_existing` mode preserves idempotent primary-key checks through the
configured authority. The product/default authority is BSI-backed.

Compare the guarded fresh-load path against the conservative default with:

```bash
cd /home/gmolinari/projects/quantastream/tpc-h-benchmark
ORDERS=1000 \
LINEITEMS=4 \
SHARDS=1 \
RUNS=3 \
  ./run-native-ingest-pk-mode-compare.sh
```

The single-mode benchmark script writes its JSON report and console log under
`tpc-h-benchmark/local/ingest-benchmarks/<timestamp>/` by default. Set
`BENCHMARK_REPORT` or `BENCHMARK_OUTPUT_DIR` to choose a specific destination.
The PK-mode comparison wrapper writes both JSON reports and `comparison.md`
under one ignored local output directory. The benchmark verifies final SQL
counts after timing stops. Normal `go test` does not run it.

Native ingest reports include enqueue wall time, drain wall time, PutRow stage
timings, flush timings, and a primary-key resolver profile. Enqueue time is the
foreground cost of routing and queueing envelopes. Drain time is the wall-clock
cost of closing the router and waiting for async writers to finish. Reports
also include worker/shard drain attribution so a slow close can be separated
into balanced parallel drain versus one shard doing most of the work. Flush time
is a summed internal profile across flush operations, so it is useful for
attribution but is not the same thing as drain wall time. The resolver profile
breaks identity work into lookup-required rows, local batch-cache lookups and
hits, BSI lookups and hits, rownum allocation, provided/direct column-id paths,
and staged primary-key cache writes.

Set `REPLAYS` to repeat the same deterministic envelopes inside each measured
operation. This is useful for primary-key resolver experiments because the
second and later passes exercise existing-key behavior without changing the
logical input shape.

`PRIMARY_KEY_AUTHORITY` defaults to the go-forward native BSI primary-key
authority lane. Explicit `bsi` and `default` values select the same product
path. Set `PRIMARY_KEY_AUTHORITY=none` only for focused diagnostics that
deliberately exercise missing-authority fail-closed behavior.

Capture a replayed BSI authority benchmark for before/after comparison:

```bash
cd /home/gmolinari/projects/quantastream/tpc-h-benchmark

BENCHMARK_OUTPUT_DIR=local/ingest-benchmarks/pk-authority-replay \
PROFILE=bsi-authority-replay-4shards \
ORDERS=100 \
LINEITEMS=4 \
SHARDS=4 \
RUNS=1 \
REPLAYS=2 \
BENCHMARK_REPORT=local/ingest-benchmarks/pk-authority-replay/bsi.json \
  ./run-native-ingest-benchmark.sh
```

Compare two native ingest benchmark reports with:

```bash
cd /home/gmolinari/projects/quantastream/tpc-h-benchmark
./run-native-ingest-compare.sh \
  local/ingest-benchmarks/before/standard-native-tpch-ingest.json \
  local/ingest-benchmarks/after/standard-native-tpch-ingest.json \
  local/ingest-benchmarks/comparison.md
```

The comparison treats throughput metrics as higher-is-better and per-operation
cost metrics as lower-is-better. The rendered markdown starts with benchmark
configuration, then a curated load-path summary for enqueue, drain, flush, and
primary-key signals, then keeps the complete metric table below it for detailed
analysis. The summary includes BSI hit and lookup cost rows when the reports
contain primary-key resolver data. The JSON reports and rendered markdown are
local benchmark artifacts and should stay out of source control unless a
reference run is intentionally being archived.

## Query Profile Capture

Use query profile capture when a case passes but the wall time needs
explanation. This is different from benchmark comparison: the output is a
per-query execution trace, not a correctness verdict or a publishable timing
claim.

Profile capture currently works through QuantaStream socket engines because the
profile is stored on the MySQL-compatible session that executed the query.
Supported engines include `inabox-standard`, `inabox-local`, and `distributed`.
`inabox-direct` has no running SQL endpoint, so direct-mode investigation should
use SQLRunner timings and runtime probes instead.

For TPC-H profile targets, prefer the helper script:

```bash
cd tpc-h-benchmark
CASE=tpch_profile.q5.formal_revenue \
  ./run-profile-capture.sh
```

When testing a local Go workspace overlay, pass `GOWORK` through the helper:

```bash
cd tpc-h-benchmark
GOWORK=/tmp/qs-roaring.work \
CASE=tpch_profile.q21.formal_supplier_wait_exists_not_exists_count \
  ./run-profile-capture.sh
```

The helper defaults to:

- `ENGINE=inabox-standard`
- `SUITE=sqltests/tpch_profile.yaml`
- `HOST=127.0.0.1`
- `PORT=4000`
- `DB=quanta`
- `VERBOSE=1`

It writes ignored local logs under `tpc-h-benchmark/local/logs`. Use `CASE=` to
target one query, `SUITE=` to switch profile suites, and `SLOW_THRESHOLD=2s` to
include SQLRunner's slow-case summary.

Raw SQLRunner profile capture is also available:

```bash
cd sqlrunner
go run . \
  -engine inabox-standard \
  -suite_file ../tpc-h-benchmark/sqltests/tpch_profile.yaml \
  -case tpch_profile.q21.formal_supplier_wait_exists_not_exists_count \
  -port 4000 \
  -verbose \
  -capture_profile
```

The profile rows are also queryable manually on the same MySQL session after a
query runs:

```sql
SHOW QUANTASTREAM PROFILE;
SELECT * FROM quantastream_last_query_profile;
```

Useful profile sections include:

- `direct_bitmap` for candidate-set seeding and bitmap predicate fragments.
- `relationship_join` for graph reduction, relationship-vector projection,
  parent-key mapping, and grouped relationship aggregation phases.
- `same_row_comparison` for BSI-to-BSI same-row comparisons.
- `native_projection_materialization` for field reads and dictionary or
  backing-string rehydration.
- `aggregate` and `grouped_aggregate` for materialization and residual-scan
  phases outside relationship-vector graph execution.
- `query_scratchpad` for per-query cache hits, misses, and stores.

When interpreting profiles, look for the largest elapsed phases first, then
check whether they are repeated within one query. Repeated misses or repeated
large field reads usually point to a missing scratchpad reuse opportunity.
Elapsed profile rows should guide follow-up hypotheses; they are not stable
enough on a laptop to be benchmark evidence by themselves.

## Initial Work Items

1. Add a MySQL reference deployment template once the preferred cloud baseline is
   selected.
2. Promote a controlled report archive convention once reference benchmark runs
   become repeatable enough to preserve.
3. Promote benchmark results only after correctness has passed for the same
   suite and dataset.
