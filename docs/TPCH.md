# TPC-H Validation

TPC-H is QuantaStream's primary analytical validation suite. It is used to
prove query-shape correctness, protect optimizer changes, and produce repeatable
benchmark artifacts when the environment is controlled.

Formal performance comparisons belong in the Benchmark Lab methodology described
in [BENCHMARK_LAB.md](BENCHMARK_LAB.md). Historical TPC-H optimization notes are
kept in the internal project-memory repository.

## Suites

TPC-H SQLRunner suites live under `tpc-h-benchmark/sqltests`.

Use the smoke suite after loading data:

```bash
cd sqlrunner
go run . \
  -engine inabox-direct \
  -suite_file ../tpc-h-benchmark/sqltests/tpch_smoke.yaml \
  -consul 127.0.0.1:8500 \
  -precise_timing
```

Use the read-only benchmark suite for compact query-shape validation:

```bash
cd sqlrunner
go run . \
  -engine inabox-direct \
  -suite_file ../tpc-h-benchmark/sqltests/tpch_benchmark_readonly_scale.yaml \
  -consul 127.0.0.1:8500 \
  -precise_timing \
  -benchmark_warmup 1 \
  -benchmark_runs 3 \
  -benchmark_report /tmp/quantastream-tpch-readonly.json \
  -benchmark_profile quantastream-tpch-readonly
```

Use the profile suite when validating optimizer and runtime changes:

```bash
cd sqlrunner
go run . \
  -engine inabox-direct \
  -suite_file ../tpc-h-benchmark/sqltests/tpch_profile_scale.yaml \
  -consul 127.0.0.1:8500 \
  -precise_timing \
  -capture_profile \
  -benchmark_runs 3 \
  -benchmark_report /tmp/quantastream-tpch-profile.json \
  -benchmark_profile quantastream-tpch-profile
```

## Schema Model

The repository contains TPC-H table descriptors under
`tpc-h-benchmark/config`. These descriptors map TPC-H keys and relationships to
bitmap-native storage:

- table keys such as `customer.c_custkey`, `orders.o_orderkey`, and
  `lineitem.l_orderkey` are configured as QuantaStream row identifiers or
  relationship vectors;
- categorical values use bitmap-oriented string representations;
- numeric, timestamp, and relationship values use BSI-backed representations;
- selected parent-to-child edges can use persisted relationship artifacts when
  the query family benefits from them.

General descriptor guidance is documented in [SCHEMA_DESIGN.md](SCHEMA_DESIGN.md).

## Validation Rules

TPC-H suites should stay executable and deterministic:

- correctness suites should assert stable result values;
- benchmark reports should include repository metadata and run counts;
- optimizer changes should keep a before/after report when they affect shared
  runtime paths;
- unsupported SQL shapes should live in focused compatibility suites or GitHub
  Issues, not as duplicate planning prose.

## Baseline Artifacts

Current public benchmark methodology and notes are maintained in
[TPCH_BENCHMARK_NOTES.md](TPCH_BENCHMARK_NOTES.md). Larger raw benchmark bundles
and historical tuning narratives are retained outside the public quick-start
path.
