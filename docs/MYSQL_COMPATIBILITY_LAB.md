# MySQL Compatibility Lab

The MySQL Compatibility Lab is a removable testing workstream that measures
QuantaStream behavior against stock MySQL without driving core engine design
from the outside in. It should live mostly in SQLRunner, SQL test suites, and
reporting helpers. Core engine changes should only happen later when a measured
compatibility gap is intentionally selected for implementation.

## Goals

1. Confirm QuantaStream behaves like a MySQL drop-in where the SQL surface is
   explicitly supported.
2. Expand coverage beyond TPC-H query shapes into everyday SQL used by client
   drivers, tools, and applications.
3. Categorize differences clearly instead of treating all divergence as the
   same kind of failure.
4. Keep the compatibility lab easy to remove, ignore, or run out of band.

## Non-Goals

- Do not adopt the full MySQL server test framework as routine CI.
- Do not implement MySQL-specific internals that conflict with the
  bitmap-native analytical engine.
- Do not make compatibility lab code a dependency of core execution packages.
- Do not hide known differences through overly broad normalization.

## Current Status

The lean lab framework is in place:

- Compatibility metadata and report categories are supported in SQLRunner.
- Canonical result capture can generate normal runnable SQLRunner suites.
- Differential mode can capture from one engine and run the generated suite
  against another engine in one command.
- Seed suites cover select, predicates, functions, grouping/order, joins,
  subqueries, and mutations.
- Live MySQL remains caller-provided; normal CI should not depend on it.

Further work should deepen the compatibility corpus and use the lab output to
prioritize engine fixes.

## Work Slices

### Slice 1: Roadmap Metadata

Add optional suite metadata to SQLRunner roadmap cases:

- `feature`: the compatibility feature family, such as `select_projection`.
- `compatibility`: the intended compatibility target, initially `mysql`.
- `requires`: smaller capability tags needed by the case.

Existing suites must continue to parse unchanged.

### Slice 2: Result Categories

Add compatibility-level categories that can be reported separately from raw
SQLRunner result states:

- `PASS`: supported behavior matches the reference.
- `FAIL`: supported behavior diverges from the reference.
- `UNSUPPORTED`: the case marks a visible but intentionally unsupported gap.
- `TYPE_WARN`: values match but type names or display formats differ.
- `PERF_WARN`: values match but the runtime exceeds a configured threshold.

The first implementation can derive categories from existing SQLRunner case
results. Later implementations can compare two engines directly.

### Slice 3: Canonical Results

Add canonicalization helpers for result sets:

- Normalize column names when configured.
- Normalize common MySQL and QuantaStream type aliases.
- Preserve SQL `NULL`.
- Normalize numeric display only when the column type is numeric.
- Optionally sort rows when the SQL case declares unordered output.

Canonicalization must be conservative. For example, string values with leading
zeros should not be rewritten unless the column type is numeric.

### Slice 4: Seed Suites

Create small compatibility suites by SQL surface:

- `mysql_compat_select.yaml`
- `mysql_compat_functions.yaml`
- `mysql_compat_predicates.yaml`
- `mysql_compat_group_order.yaml`
- `mysql_compat_joins.yaml`
- `mysql_compat_subqueries.yaml`
- `mysql_compat_mutations.yaml`

The first seed suite should be tiny and parser-focused. Coverage breadth matters
more than performance at this layer.

### Slice 5: Reference Execution

Add a MySQL reference engine mode behind DSN configuration:

```bash
go run ./sqlrunner -engine mysql-reference -suite_file sqltests/mysql_compat_select.yaml
```

This should execute against a caller-provided MySQL instance. SQLRunner should
not own MySQL server startup.

### Slice 6: Differential Compare

Add capture/compare and then one-command differential mode:

```bash
go run ./sqlrunner -engine-diff mysql-reference,qsbridge -suite_file sqltests/mysql_compat_select.yaml
```

The report should group results by feature and category so we can see whether a
change improves compatibility broadly or only fixes one case.

## Live MySQL Workflow

SQLRunner intentionally does not own MySQL startup. The lab expects a
caller-provided DSN for stock MySQL. From the `sqlrunner` directory:

```bash
MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/test' ./run-mysql-compat.sh
MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/test' MYSQL_COMPAT_MODE=diff TARGET_ENGINE=legacy-direct ./run-mysql-compat.sh
```

The helper writes local capture output under `expected/local/` by default and
uses `-engine_diff mysql-reference,<target>` for one-command comparisons.

## Generated Suite Convention

Generated compatibility suites should default to ignored local paths such as
`sqlrunner/expected/local/`. Curated generated suites may be promoted under
`sqlrunner/expected/` when they become reviewed compatibility contracts. This
keeps one-off captures out of source control while leaving room for stable
reference contracts later.

Current scaffolding includes canonical expected-result capture structures and
query-result comparison helpers. The CLI now supports `-capture_expected` for
writing a runnable SQLRunner suite whose `expect` blocks are captured from any
configured SQLRunner engine. The intended workflow is to capture from stock
MySQL, then run the generated suite against QuantaStream. The CLI also supports
`-engine_diff reference,target` as a one-command version of that workflow. Live
MySQL remains caller-provided through `-engine mysql-reference` and
`-mysql_dsn`.
