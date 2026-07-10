# SQLRunner

SQLRunner executes YAML roadmap suites. It supports a proxy harness for the
full product path, an in-memory runtime harness for qsbridge-only slices, and a
legacy-direct harness for exercising the refactor stack against a running
legacy Quanta cluster.

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

`-engine proxy` is the default and runs suites through the current MySQL proxy
and Quanta cluster. It is the compatibility path for existing integration and
benchmark scripts.

`-engine runtime` runs the in-process qsbridge/qsruntime path without the MySQL
wire protocol or a legacy cluster. This mode is useful for parser, planner,
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
can be checked without needing a legacy cluster.

Membership inspection rows describe SQL-level semi/anti membership edges such
as `IN` and `NOT IN` subqueries. Relationship adapter rows describe runtime
join-vector execution boundaries. Keeping those rows separate makes cases like
TPC-H Q16 easier to reason about: the anti-membership may be a peer-value
filter while the `part -> partsupp` relationship-vector join remains a separate
execution boundary.

`-engine legacy-direct` runs SQLRunner through the new qsbridge/qsruntime
planning path, then adapts the lowered Quanta intermediate query directly into
legacy bitmap execution. It requires a running legacy local cluster and Consul,
but it bypasses the MySQL proxy and network wire path. This mode is the current
vertical-slice bridge for proving simple SQL against real Quanta bitmap data:

```bash
go run . \
  -engine legacy-direct \
  -suite_file sqltests/legacy_direct_smoke.yaml \
  -consul 127.0.0.1:8500
```

The legacy-direct smoke suite currently covers single-table `count(*)`
predicates over numeric BSI fields, StringEnum exact/`IN` predicates, and narrow
projection materialization through the direct runtime path. It is not the
compatibility route for general SQL; unsupported query shapes should stay in the
proxy or in-memory runtime suites until the new planner/runtime grows the needed
primitive.

`sqltests/legacy_direct_qa_basic.yaml` is the portable QA-table checkpoint for
the direct path. It creates and loads `customers_qa` and `orders_qa`, including
the simple multiplicity-set `phoneType` values used by later mutation and join
coverage, then asserts the stable direct-read surface over the QA catalog:

```bash
go run . \
  -engine legacy-direct \
  -suite_file sqltests/legacy_direct_qa_basic.yaml \
  -consul 127.0.0.1:8500
```

`sqltests/legacy_direct_basic.yaml` is the promoted legacy-direct contract for
basic one-table execution. It keeps a smaller, deliberate set of supported
cases: numeric and StringEnum filtering, multi-column projection,
`LIMIT/OFFSET`, and simple global numeric aggregates. Use it when validating
that a local legacy cluster can execute the current qsbridge/qsruntime vertical
slice:

```bash
go run . \
  -engine legacy-direct \
  -suite_file sqltests/legacy_direct_basic.yaml \
  -consul 127.0.0.1:8500
```

The legacy-direct joins suite is the QA-backed relationship-vector
checkpoint. It creates and loads `customers_qa` and `orders_qa`, then validates
join count, projection, filtering, grouped aggregate, and distinct aggregate
cases over the customer-orders edge. Use it as the portable join regression
gate before reaching for TPC-H catalog data:

```bash
go run . \
  -engine legacy-direct \
  -suite_file sqltests/legacy_direct_joins.yaml \
  -consul 127.0.0.1:8500
```

`sqltests/legacy_direct_tpch_kernels.yaml` is the broad TPC-H kernel regression
suite for the legacy-direct path. It does not run formal TPC-H verbatim end to
end; instead it captures the staged kernels that proved the planner, relationship
vector reductions, grouped materialization, searched CASE aggregates, membership
subqueries, and physical time-shard windowing needed by the query roadmap. It
requires a loaded TPC-H catalog and is intentionally heavier than the QA suites:

```bash
go run . \
  -engine legacy-direct \
  -suite_file sqltests/legacy_direct_tpch_kernels.yaml \
  -consul 127.0.0.1:8500
```

Because this suite can take several minutes, keep it out of fast package CI
unless a dedicated legacy-direct integration job is created. Use it as a
pre-retirement gate for the legacy proxy and as a performance watchpoint for
materialization-heavy cases such as Q19, Q18, Q21 late receipt, and Q5 graph
revenue kernels.

For the current proxy-retirement readiness gate, run the quick legacy-direct
suites from the `sqlrunner` directory:

```bash
./run-legacy-direct-readiness.sh
```

The broad TPC-H kernel suite is opt-in:

```bash
RUN_TPCH=1 SLOW_THRESHOLD=10s ./run-legacy-direct-readiness.sh
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

- `engine`: execution harness; `proxy`, `runtime`, or `legacy-direct`, defaults
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

The `proxy` and `legacy-direct` harnesses expect Consul and a green local
cluster to be running. `proxy` talks through the MySQL proxy. `legacy-direct`
uses the cluster catalog and bitmap sessions directly from the SQLRunner
process.
