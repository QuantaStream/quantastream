# DEVELOPMENT.md

# Quanta Development Guide

## Build

```bash
make build_all
```

---

# Run Tests

```bash
make test
```

---

# Integration Testing

Start Quanta-in-a-Box:

```bash
cd start-local
go run .
```

Then run SQLRunner:

```bash
cd sqlrunner
go run . \
  -suite_file sqltests/joins_sql.yaml \
  -host 127.0.0.1 \
  -user MOLIG004 \
  -db quanta \
  -port 4000
```

---

# Important Development Notes

## Quanta-in-a-Box

The preferred development environment is:

- local
- in-process
- reproducible
- low-friction

Most development should happen against QIAB.

QIAB is a development and conformance topology. Changes that affect node
identity, persistence, networking, lifecycle, or recovery must also be
considered against the deployment models in
[`DEPLOYMENT.md`](DEPLOYMENT.md).

---

## SQL Layer

Schema design is part of query planning in Quanta. Before adding new benchmark
schemas or substantial test data, review [`SCHEMA_DESIGN.md`](SCHEMA_DESIGN.md)
and the field-level reference in [`../configuration/README.md`](../configuration/README.md).

The SQL layer currently supports a practical analytical SQL subset.
Supported custom SQL extensions are tracked in
[`SUPPORTED_SQL.md`](SUPPORTED_SQL.md), and known unsupported or partial SQL
syntax is tracked in [`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md). The long-term
direction for a Quanta-native SQL engine is captured in
[`NATIVE_SQL_ENGINE_PLAN.md`](NATIVE_SQL_ENGINE_PLAN.md).

The bitmap execution engine is the primary architectural focus.

Avoid broad SQL/parser rewrites unless necessary.

### Subquery Roadmap

Subquery support is intentionally incremental. `IN` and `NOT IN` use
membership-oriented semi/anti join rewrites, and correlated `EXISTS` /
`NOT EXISTS` with an equality predicate can rewrite to semi/anti joins.

Remaining scalar and non-correlated subqueries should not be forced through the
join path. Non-correlated `EXISTS` needs a subquery-gate execution step that
runs the child query once and either passes or suppresses the parent result set.
Scalar predicate and `SELECT`-list subqueries need a scalar-subquery execution
step that runs the child query once, validates that it produced one scalar
value, and injects that value into predicate or projection evaluation.

Defer those cases until the planner has an explicit scalar/subquery-gate task
shape instead of adding another special case to `JoinMerge`.

### MySQL Database/Schema Compatibility

Quanta currently treats the MySQL database name mostly as a connection/schema
grouping and only partially implements MySQL database semantics. This is enough
for the current proxy and SQLRunner paths, but some MySQL-compatible tools issue
database commands during connect or startup and report noisy errors even though
queries can still run.

Roadmap work should make database/schema handling explicit and tool-friendly:

- accept and track `USE <database>` where it maps cleanly to a Quanta schema
- return sensible results for common database discovery commands used by
  clients such as `mycli`
- preserve the current default `quanta` behavior for existing scripts
- avoid conflating SQL schema grouping with physical storage topology

## Session Ownership

`core.Session` should be treated as a single-owner object. It is intentionally
not thread-safe. Do not share one session across concurrent goroutines unless a
higher-level owner serializes all access.

Parallel ingestion should use deterministic routing to multiple session workers
instead. The Kinesis consumer follows this pattern by selecting the target table
from schema selectors, hashing the configured loader shard key, and sending each
record to one internal shard channel/session owner.

---

## qlbridge

Quanta currently contains a modified/forked qlbridge implementation.

Known technical debt areas include:

- join logic
- aggregate planning
- GROUP BY behavior
- query edge cases

Stabilization is preferred over broad refactoring.

---

# Current Development Priorities

1. QIAB stabilization
2. TPC-H support
3. SQL correctness
4. Streaming demo restoration
5. Documentation improvements
6. OSS onboarding simplification

Future onboarding work includes analyzer tooling for profiling representative
data samples and proposing Quanta schemas. See [`ANALYZER.md`](ANALYZER.md).

---

# Known Technical Debt

- commented-out tests
- dead code cleanup
- startup ergonomics
- ingestion abstraction cleanup
- SQL planner complexity
- qlbridge maintenance burden

---

# Recommended Workflow

```bash
make build_all
make test

cd start-local
go run .

cd sqlrunner
go run . -suite_file sqltests/joins_sql.yaml \
  -host 127.0.0.1 -user MOLIG004 -db quanta -port 4000
```

The goal is to maintain:

- reproducible local startup
- repeatable query validation
- stable integration behavior

The YAML suites under `sqlrunner/sqltests` are the SQL behavior map. Supported
cases protect current behavior; `xfail` cases retain roadmap goals without
blocking incremental engine work. The older line-oriented scripts remain for
compatibility with lower-level test fragments.

TPC-H is a benchmark roadmap, but core SQL behavior discovered while working on
TPC-H should be backfilled into the broader SQLRunner suites. The TPC-H suites
should prove benchmark-shaped behavior; the broad suites should preserve engine
contracts such as joins, grouping, aggregate ordering, field-to-field
predicates, and projection metadata.

Grouped join execution currently has two important paths. The newer
multi-table grouped path is for non-outer grouped joins with `count(*)` and
`sum(...)` aggregates, and is used by the staged Q15/Q16-style kernels. Grouped
joins with outer semantics or reducers such as `min`, `max`, and `avg` must
continue to fall back to the compatibility weighted aggregate path until the
fast path has explicit reducer support. SQLRunner broad coverage should protect
both routes.

Projector-local caches are acceptable when their lifetime is bounded to one
query/projector and they preserve normal projection semantics for missing
fields. Server-side query-local caches are also acceptable for immutable
fragments assembled more than once during one bitmap query, such as repeated
`timeRangeBSI` assembly for the same `index/field/fromTime/toTime` window.
Those caches must stay scoped to a single request unless they carry explicit
invalidation metadata. Any cache that crosses query, session, front-door, shard
sync, or mutation boundaries should be inventoried here and treated as part of a
future coherent cache layer.

Cross-query or cross-session reusable fragments should not be added to the
projector layer; they belong in a future query-front-door fragment cache with
explicit versioning and invalidation.

### Query Cancellation Roadmap

The MySQL client sends `KILL QUERY <connection_id>` after `Ctrl+C`, but
long-running Quanta queries can remain hung with no prompt. Bouncing the
MySQL front door or query process releases the client, which indicates that
cancellation is not yet propagating from the MySQL protocol/session layer into
the active planner and executor contexts.

Cancellation support should map connection/query ids to active execution
contexts and make residual scans, grouped aggregation, subquery membership, and
join/projector loops observe cancellation. Until that exists, hung analytical
queries may require a front-door or cluster bounce as the operational recovery.

### Function Expression Roadmap

Function and expression execution should be mapped in
`sqlrunner/sqltests/function_expressions.yaml`, a dedicated broad
SQLRunner suite separate from the TPC-H benchmark suites. That suite
should record where expressions are supported and where they remain
roadmap goals across these SQL locations:

- scalar expressions and functions in the `SELECT` list
- functions and arithmetic expressions in `WHERE` predicates
- expressions inside aggregate arguments, such as `sum(age + 1)` and formal
  TPC-H revenue arithmetic
- expressions in `GROUP BY`
- expressions and aliases in `ORDER BY`
- function and expression behavior across joined inputs

Start with supported baseline cases, then retain unsupported expression shapes
as `xfail` roadmap goals. This lets Quanta incrementally expand expression
support without blocking forward progress or forcing a broad SQL/parser rewrite
prematurely.

### Core Suite Backfill

When TPC-H work stabilizes a general engine behavior, add or strengthen coverage
in the broad suites as well as the benchmark suite. Near-term backfill targets
include:

- `joins` for multi-table joins, residual field-to-field predicates, and
  projection metadata that does not expose helper columns
- `group_by` for multiple aggregates, aggregate aliases, numeric aggregate
  ordering, and decimal comparison/rendering behavior
- `join_group_by` for grouped join visible headers, multiple aggregates over
  joins, grouped outer joins, aggregate ordering, and weighted join
  multiplicity
- `basic` for scalar predicate edge cases discovered during benchmark work,
  including any confirmed `IN` predicate anomalies


`sqlrunner/sqltests/group_by.yaml` is the focused roadmap for the partially
implemented native grouping path. It currently records standard bitmap
grouping, multiple aggregates, multiple grouping fields, aliases, BSI grouping,
`HAVING`, and aggregate ordering. Cases remain `xfail` until complete
SQL-visible values are correct, even when the underlying bitmap cardinalities
appear correct.

### GROUP BY Execution Roadmap

The likely incremental direction is to select the grouping implementation
according to query capabilities:

- use the native `core.Projector.AggregateAndGroup` path when grouping
  expressions are simple columns, every grouping column is backed by a
  standard bitmap, and all requested aggregates and result semantics are
  supported
- otherwise retain the normal qlbridge grouping and `HAVING` processing as a
  correctness fallback, even when that path is less performant
- reject a query only when neither path can preserve its SQL semantics

Native-path eligibility must eventually account for aliases, projection order,
null behavior, aggregate expressions, `DISTINCT`, joins, `HAVING`, and result
ordering rather than checking storage type alone. Quanta should suppress the
qlbridge group task only after the complete query has been classified as
native-capable.

TPC-H queries should be used to evaluate and expand this boundary. Their
grouping and aggregation patterns are representative analytical workloads and
should help identify which operations provide the greatest benefit when moved
onto bitmap-native execution, while the fallback path preserves incremental
correctness as support grows.

Native single-table grouping supports multiple `COUNT`, `MIN`, `MAX`, `SUM`,
and `AVG` projections. The focused join path now supports the same aggregate
set for scalar and grouped inner joins, plus grouped outer joins, when grouping
and aggregate columns come from the left-side parent table.

`sqlrunner/sqltests/join_group_by.yaml` protects this boundary. Join
relationship multiplicity is retained as BSI weights: grouped `COUNT`, `SUM`,
and `AVG` use those weights, while `MIN` and `MAX` use normal existence
semantics. Unmatched parent rows in an outer join receive multiplicity one.
Qualified aggregate arguments such as `sum(c.age)` are supported, and
duplicate aggregate columns introduced by join source projection are
normalized before results are exposed.

### Custom Function Inventory

Custom expression functions live under `custom/functions` and are registered by
`custom/functions/load.go`. This package should be treated as part of Quanta's
SQL extension surface and reviewed deliberately during SQL engine refactor
work.

Currently relevant registered functions:

- `sample_stratified(fieldName, percentage)`
- `version()`
- `database()`
- `add(...)`
- `timediff(end_time, start_time, unit)`

The S3 bucket inspection helpers are also registered in `LoadAll`, but they are
not part of the near-term SQL roadmap. They perform live AWS SDK calls, and
`is_bucket_writable` writes and deletes `_TEST`, so exclude them from ordinary
SQLRunner coverage and engine-refactor planning unless a separate integration
surface is deliberately restored.

Current scan notes:

- `timediff(...)` is covered in SQLRunner and documented as supported custom
  SQL.
- `sample_stratified(...)` is present and wired through `SamplePct`, but still
  needs verification, fixes if required, and SQLRunner coverage before it is
  documented as supported.
- `database()` currently validates as a zero-arg function but returns
  `versionEval` from `DatabaseFunc.Validate`; this should be fixed or
  intentionally documented before relying on it for MySQL compatibility.
- `Avg` exists in `custom/functions/arithmetic.go`, but `LoadAll` does not
  register it. Ignore that helper for roadmap purposes because native SQL
  aggregate `avg(...)` owns the supported surface.
- `add(...)` is registered using the `Sum` evaluator. It is a scalar helper,
  not the SQL aggregate `sum(...)`.

### TOPN Aggregate Roadmap

Quanta has an existing `topn()` aggregate capability documented as custom SQL
in [`SUPPORTED_SQL.md`](SUPPORTED_SQL.md). It should be retained as part of the
SQL roadmap because it uses the same bitmap-native ideas as standard-bitmap
grouping: group membership and cardinality can be computed efficiently without
materializing every row through the generic SQL engine.

Future work should characterize existing `topn()` behavior in a focused
SQLRunner suite before changing implementation details. The suite should
distinguish currently supported native behavior from roadmap goals such as
aliases, ordering, ties, limits, joined inputs, and interaction with other
aggregates.

### Data Sampling Custom Function Roadmap

Quanta should retain, verify, and extend the existing custom SQL capability for
data sampling. The current function is registered as
`sample_stratified(fieldName, percentage)` in `custom/functions/load.go` and
implemented in `custom/functions/sampling.go`. It annotates query fragments with
`SamplePct`, and the shared/server bitmap query path carries that sampling
metadata during execution.

Sampling is a natural Quanta-native feature because it can support schema
discovery, analyst exploration, smoke-test generation, and approximate
profiling without forcing callers to materialize broad result sets.

Before documenting sampling as supported SQL, `sample_stratified(...)` should
be verified, fixed where needed, and covered in SQLRunner. The refreshed
capability should define:

- supported SQL shape and expected placement, especially predicate usage
- deterministic versus random sampling semantics
- requested sample size and default limit behavior
- filtered input behavior
- projected row versus value-distribution output shape
- interaction with `StringEnum`, BSI-backed fields, and backing-store strings
- current BSI restrictions and whether they should remain
- single-table, joined-input, and grouped-input support boundaries
- whether the implementation is row-oriented, bitmap-guided, or shard-aware

Until coverage exists, treat sampling as an existing custom SQL roadmap
capability rather than a supported extension.

### BSI GROUP BY Fallback

Grouping by BSI-backed fields is a planner boundary rather than only a storage
type question. The native bitmap grouping path is strongest when every grouping
expression is backed by a standard bitmap. When a query groups by BSI fields,
or mixes standard bitmap and BSI grouping expressions, a future optimizer should
be able to choose the generic qlbridge grouping task as a correctness fallback.

That fallback may be slower, but it avoids rejecting otherwise valid SQL while
native bitmap support remains incomplete. The planner should make this decision
from complete query semantics, including projection aliases, aggregate
expressions, `HAVING`, ordering, joins, null behavior, and result shape.

The implementation remains intentionally localized. It does not yet provide a
general join aggregation planner. Current limitations include:

- grouping and aggregate fields must belong to the left-side parent table
- group fields must use standard bitmap mappings
- aggregate value fields must use BSI mappings
- aggregate expressions, `DISTINCT`, `HAVING`, and broader multi-join grouping
  remain roadmap work

`sqlrunner/sqltests/subqueries.yaml` owns subquery and anti-join behavior.
Quanta currently rewrites single-column `IN` and `NOT IN` subqueries as joins;
`NOT IN` becomes the same inequality join form used by explicit anti-joins.
The suite preserves currently supported subquery behavior, including filtered
single-column `IN` subqueries. Direct anti-join projection, scalar `COUNT(*)`,
and grouping are supported.

The next subquery implementation phase should remain explicit about SQL
semantics. `IN` rewriting now uses membership-oriented semi-join behavior
instead of preserving duplicate matching inner rows. Correlated `EXISTS` and
`NOT EXISTS` forms with an inner equality predicate are rewritten as semi-joins
and anti-joins. Non-correlated `EXISTS`, scalar predicate subqueries, and scalar
select-list subqueries remain parser/planner work. Future work should also
verify SQL null semantics.

`sqlrunner/sqltests/multi_table_joins.yaml` characterizes chained join
execution independently of subquery rewriting. Its deterministic
`customers -> orders -> lineitems -> deliveries` fixture exercises
three-table projection, multiplicity, grouping, table order, mixed outer joins,
final-position anti-joins, and four-table composition. Cases remain `xfail`
until their behavior is both correct and understood; this suite should guide
localized fixes before any broad join-engine reorganization.

Initial characterization shows that the fixture and each adjacent two-table
parent relation are correct. Three- and four-table scalar counts are supported
after making chained join driver selection deterministic. Chained projection
and grouping still return no rows. Four-table projection also reaches a final
projection error resolving the deepest `deliveries_qa` relationship,
indicating that the current projection path does not traverse an arbitrary
parent-relation chain.

## Line-Oriented SQLRunner Compatibility Debt

Some retention, restart, topology, and Docker integration tests still use the
older line-oriented scripts under `sqlrunner/sqlscripts`. In particular, they
depend on reusable load, body, and bug-reproduction fragments that are separate
from the primary SQL conformance suites.

For now:

- new SQL behavior and roadmap coverage must use YAML suites
- existing line-oriented fragments should remain until their owning tests are revised
- new line-oriented scripts should not be added
- line-oriented parser and script removal should happen as part of a focused topology
  or test-infrastructure refactor

This is intentional compatibility debt. Migrating these specialized fixtures
now would add risk without materially improving the SQL implementation roadmap.
