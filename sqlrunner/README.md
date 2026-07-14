# SQLRunner

SQLRunner executes YAML roadmap suites. It supports MySQL socket harnesses for
the product paths, an in-memory runtime harness for qsbridge-only slices, and an
inabox-direct harness for exercising the query stack against a running local
QuantaStream node cluster.

## Roadmap Suites

Run from the `sqlrunner` directory:

```bash
go run . \
  -engine proxy \
  -suite_file sqltests/joins_sql.yaml \
  -host 127.0.0.1 \
  -user MOLIG004 \
  -db quanta \
  -port 4000
```

The migrated suites are:

- `sqltests/basic_queries.yaml`
- `sqltests/insert_tests.yaml`
- `sqltests/mutate_tests_body.yaml`
- `sqltests/joins_sql.yaml`
- `sqltests/group_by.yaml`
- `sqltests/join_group_by.yaml`
- `sqltests/subqueries.yaml`
- `sqltests/multi_table_joins.yaml`

Cases are marked `supported`, `xfail`, or `skip`. A supported case must pass.
An `xfail` case records desired behavior that is not yet correct. If an
`xfail` case begins passing, SQLRunner reports `XPASS` and fails the suite
until its expectations are reviewed and the case is promoted.

Use `-case` to run one exact case ID from a suite without creating a temporary
YAML file:

```bash
go run . \
  -engine runtime \
  -suite_file sqltests/runtime_smoke.yaml \
  -case runtime_smoke.001.project_order_keys
```

See [`roadmap/FORMAT.md`](roadmap/FORMAT.md) for the complete format.

## Harnesses

`-engine proxy` is the default and runs suites through an explicitly addressed
MySQL-compatible QuantaStream endpoint. It requires `-host` and `-user`, and is
kept for existing integration and benchmark scripts.

`-engine runtime` runs the in-process qsbridge/qsruntime path without the MySQL
wire protocol or a local node cluster. This mode is useful for parser, planner,
executor, result-shape, and SQLRunner contract tests over fixture-backed native
runtime data:

```bash
go run . \
  -engine runtime \
  -suite_file sqltests/basic_queries.yaml
```

`-engine runtime-inspect` runs the same in-process planning path but returns
inspection rows instead of query results. It is the refactor debugging harness
for checking lowered fragments, runtime route selection, call-plan steps, join
edges, and blocking diagnostics without executing bitmap work:

```bash
go run . \
  -engine runtime-inspect \
  -suite_file sqltests/runtime_inspection.yaml
```

Inspection cases may use `expected_diagnostics` to assert an intentional
runtime boundary, such as a relationship-vector join that has been planned but
is not wired to execution yet. When the diagnostic codes match exactly,
SQLRunner still validates the returned inspection rows.

The inspection row stream includes `query_shape` rows that summarize the parsed
and bound SQL shape before execution. These rows intentionally track broad
planner-relevant counts, including sources, joins, memberships, predicates,
grouping, ordering, limits, aggregate functions, conditional aggregates,
arithmetic aggregates, and distinct aggregates. The runtime inspection suite
keeps small TPCH-shaped examples for these aggregate families so planner changes
can be checked without needing a local node cluster.

Membership inspection rows describe SQL-level semi/anti membership edges such
as `IN` and `NOT IN` subqueries. Relationship adapter rows describe runtime
join-vector execution boundaries. Keeping those rows separate makes cases like
TPC-H Q16 easier to reason about: the anti-membership may be a peer-value
filter while the `part -> partsupp` relationship-vector join remains a separate
execution boundary.

`-engine inabox-direct` runs SQLRunner through the new qsbridge/qsruntime
planning path, then adapts the lowered Quanta intermediate query into the
direct bitmap/node execution path. It requires a running local QuantaStream
node cluster and Consul, but it bypasses the MySQL proxy and network wire path.
This mode proves SQL against real QuantaStream bitmap data without the MySQL socket:

```bash
go run . \
  -engine inabox-direct \
  -suite_file sqltests/inabox_direct_smoke.yaml \
  -consul 127.0.0.1:8500
```

The inabox-direct smoke suite currently covers single-table `count(*)`
predicates over numeric BSI fields, StringEnum exact/`IN` predicates, and narrow
projection materialization through the direct runtime path. It is not the
compatibility route for general SQL; unsupported query shapes should stay in the
proxy or in-memory runtime suites until the new planner/runtime grows the needed
primitive.

`sqltests/inabox_direct_qa_basic.yaml` is the portable QA-table checkpoint for
the direct path. It creates and loads `customers_qa` and `orders_qa`, including
the simple multiplicity-set `phoneType` values used by later mutation and join
coverage, then asserts the stable direct-read surface over the QA catalog:

```bash
go run . \
  -engine inabox-direct \
  -suite_file sqltests/inabox_direct_qa_basic.yaml \
  -consul 127.0.0.1:8500
```

`sqltests/inabox_direct_basic.yaml` is the promoted inabox-direct contract for
basic one-table execution. It keeps a smaller, deliberate set of supported
cases: numeric and StringEnum filtering, multi-column projection,
`LIMIT/OFFSET`, and simple global numeric aggregates. Use it when validating
that a local node cluster can execute the current qsbridge/qsruntime vertical
slice:

```bash
go run . \
  -engine inabox-direct \
  -suite_file sqltests/inabox_direct_basic.yaml \
  -consul 127.0.0.1:8500
```

The inabox-direct joins suite is the QA-backed relationship-vector
checkpoint. It creates and loads `customers_qa` and `orders_qa`, then validates
join count, projection, filtering, grouped aggregate, and distinct aggregate
cases over the customer-orders edge. Use it as the portable join regression
gate before reaching for TPC-H catalog data:

```bash
go run . \
  -engine inabox-direct \
  -suite_file sqltests/inabox_direct_joins.yaml \
  -consul 127.0.0.1:8500
```

`sqltests/inabox_direct_tpch_kernels.yaml` is the broad TPC-H kernel regression
suite for the inabox-direct path. It does not run formal TPC-H verbatim end to
end; instead it captures the staged kernels that proved the planner, relationship
vector reductions, grouped materialization, searched CASE aggregates, membership
subqueries, and physical time-shard windowing needed by the query roadmap. It
requires a loaded TPC-H catalog and is intentionally heavier than the QA suites:

```bash
go run . \
  -engine inabox-direct \
  -suite_file sqltests/inabox_direct_tpch_kernels.yaml \
  -consul 127.0.0.1:8500
```

Because this suite can take several minutes, keep it out of fast package CI
unless a dedicated inabox-direct integration job is created. Use it as a
pre-retirement gate for the old proxy and as a performance watchpoint for
materialization-heavy cases such as Q19, Q18, Q21 late receipt, and Q5 graph
revenue kernels.

For the current proxy-retirement readiness gate, run the quick inabox-direct
suites from the `sqlrunner` directory:

```bash
./run-inabox-direct-readiness.sh
```

The broad TPC-H kernel suite is opt-in:

```bash
RUN_TPCH=1 SLOW_THRESHOLD=10s ./run-inabox-direct-readiness.sh
```

`-engine inabox-standard` runs SQLRunner through the standalone
`cmd/quantastream` process. It defaults to `127.0.0.1`, `MOLIG004`,
database `quanta`, and does not require Consul because the target process owns
its local in-process node adapter:

```bash
./run-inabox-standard-smoke.sh
```

The helper starts a temporary `quantastream` process on port `4400` by default,
stages a tiny file-backed catalog, runs CREATE/INSERT/COMMIT/SELECT/DROP over
the MySQL socket, and then removes the temporary data directory. Use
`START_SERVER=0` to target an already running process, which defaults back to
port `4000`.
Roadmap `admin` bootstrap cases are accepted as deprecated shorthand and run
through the SQL engine as `CREATE TABLE`, `DROP TABLE`, or `TRUNCATE TABLE`
statements. New suites should prefer `kind: statement` with SQL DDL directly.

The default standard smoke suite is `sqltests/inabox_standard_qa_smoke.yaml`.
It checks a self-contained QA slice over the MySQL socket. Override the target
with `HOST`, `PORT`, `SQL_USER`, `DB`, `SUITE`, and optionally `CASE`:

```bash
HOST=127.0.0.1 PORT=4000 START_SERVER=0 CASE=inabox_standard_qa_smoke.006.customer_city_count ./run-inabox-standard-smoke.sh
```

The older read-only TPCH smoke remains available for already staged data:

```bash
START_SERVER=0 SUITE=inabox_standard_smoke.yaml CASE=inabox_standard_smoke.001.part_count ./run-inabox-standard-smoke.sh
```

`-engine inabox-local` runs SQLRunner through the native MySQL-compatible
server/proxy on port `4000` while a local distributed-shape harness is running.
Use it when the local harness is running the proxy and you want to validate the
socket/wire path plus local Consul/gRPC/node boundaries rather than hosting the
query engine inside SQLRunner:

```bash
go run . -engine inabox-local -suite_file sqltests/basic_queries.yaml
go run . -engine inabox-local -suite_file sqltests/inabox_direct_joins.yaml
go run . -engine inabox-local -suite_file sqltests/mutate_tests_body.yaml
```

These suites mirror the current proxy smoke checkpoint: basic SQL,
relationship-vector joins, and mutation coverage over the new MySQL protocol
front door.

## Benchmark Report Mode

SQLRunner can write a local JSON benchmark artifact for repeated measured suite
runs. This mode is correctness-gated: measured runs that contain `FAIL` or
`XPASS` still fail the command.

Use this for local developer evidence only unless the deployment follows the
Benchmark Lab controls in `../docs/BENCHMARK_LAB.md`:

```bash
go run . \
  -engine inabox-direct \
  -suite_file sqltests/inabox_direct_tpch_kernels.yaml \
  -benchmark_profile developer-local \
  -benchmark_warmup 1 \
  -benchmark_runs 3 \
  -benchmark_metadata commit=$(git rev-parse --short HEAD),cache=warm \
  -benchmark_report expected/local/tpch-kernels.json
```

The wrapper script uses the same flags with environment-variable defaults:

```bash
BENCHMARK_RUNS=3 ./run-benchmark.sh
```

Generated benchmark artifacts should stay under ignored local paths unless a
specific report is intentionally promoted. Render a human-readable summary with:

```bash
go run . -benchmark_summary expected/local/tpch-kernels.json
```

## TPC-H Suites

TPC-H-specific suites live with the benchmark assets under
`../tpc-h-benchmark/sqltests`. Run them through SQLRunner from this directory:

```bash
go run . \
  -engine proxy \
  -suite_file ../tpc-h-benchmark/sqltests/tpch_smoke.yaml \
  -host 127.0.0.1 \
  -user MOLIG004 \
  -db quanta \
  -port 4000
```

Add `-verbose` to print each roadmap case SQL before it runs and
millisecond-level timing after it completes. Add `-dump_actual` while debugging
mismatches to print the actual query rows for failing cases. Add
`-slow_threshold 10s` to print a longest-first summary of cases at or above the
threshold after normal suite output. Normal suite output includes a rounded
duration on each result line.

`tpch_smoke.yaml` validates that a generated/load TPC-H fixture is complete and
relationship traversal is sane. `tpch_queries.yaml` is the formal query roadmap
suite and should grow incrementally as Quanta's TPC-H query support matures.

## Connection Options

- `engine`: execution harness; `proxy`, `runtime`, or `inabox-direct`, defaults
  to `proxy`.
- `suite_file`: YAML roadmap suite to execute. Required.
- `host`: Quanta proxy host. Required for `proxy`.
- `user`: Quanta user. Required for `proxy`.
- `password`: Quanta password.
- `db`: database name; defaults to `quanta`.
- `port`: proxy port; defaults to `4000`.
- `consul`: Consul address; defaults to `127.0.0.1:8500`.
- `log_level`: use `DEBUG` for verbose logging.
- `verbose`: print roadmap case SQL and detailed per-case timing.
- `dump_actual`: print actual query rows when a roadmap query case mismatches.
- `slow_threshold`: print a slow-case summary for cases at or above this
  duration, for example `10s`.

The `proxy` and `inabox-direct` harnesses expect Consul and a green local
cluster to be running. `proxy` talks through the MySQL proxy. `inabox-direct`
uses the cluster catalog and bitmap sessions directly from the SQLRunner
process.
