# MySQL Compatibility Testing

QuantaStream should measure MySQL compatibility with repeatable, relevant tests rather than attempting to adopt the full MySQL server test suite wholesale. The goal is to compare behavior against stock MySQL where that behavior matters to users, while keeping the test surface aligned with QuantaStream's bitmap-native analytical engine.

## Recommended Approach

Build a MySQL differential mode into SQLRunner.

The basic workflow should be:

```bash
go run ./sqlrunner \
  -engine mysql-reference \
  -suite_file sqltests/mysql_compat_basic.yaml \
  -capture_expected out/mysql_compat_basic.expected.yaml

go run ./sqlrunner \
  -engine qsbridge \
  -suite_file sqltests/mysql_compat_basic.yaml \
  -expected out/mysql_compat_basic.expected.yaml
```

A later convenience mode could run both sides in one command:

```bash
go run ./sqlrunner \
  -engine-diff mysql,qsbridge \
  -suite_file sqltests/mysql_compat_basic.yaml
```

SQLRunner should canonicalize row and column output enough to make comparisons stable, while still preserving useful type and formatting warnings where MySQL and QuantaStream differ.

## Suite Organization

Compatibility suites should be organized by SQL surface area instead of by implementation package:

- `mysql_compat_select.yaml`
- `mysql_compat_functions.yaml`
- `mysql_compat_predicates.yaml`
- `mysql_compat_group_order.yaml`
- `mysql_compat_joins.yaml`
- `mysql_compat_subqueries.yaml`
- `mysql_compat_mutations.yaml`

Each case can carry feature metadata so SQLRunner can report compatibility by capability:

```yaml
feature: scalar_functions
compatibility: mysql
requires:
  - order_by
  - substr
  - lower
```

## Result Categories

The compatibility runner should distinguish correctness, unsupported features, and performance warnings:

- `PASS`: QuantaStream matches the MySQL reference result.
- `FAIL`: The feature is expected to work but produces a wrong result or wrong error.
- `UNSUPPORTED`: The feature is intentionally outside the current supported surface.
- `PERF_WARN`: The result is correct but slower than the suite threshold.
- `TYPE_WARN`: Values match but type names, display formats, or precision differ in a known way.

This keeps unsupported SQL visible without pretending every unsupported MySQL behavior is a defect.

## External Test Suites

Several existing projects are useful, but none should be treated as a direct replacement for SQLRunner.

### MySQL Test Framework

The official MySQL test framework is broad and valuable as a reference. It includes `mysql-test-run.pl` and many `.test` / `.result` cases in the MySQL source tree. Some tests can run against an external server, but many assume MySQL internals: server variables, storage engines, plugins, replication, filesystem behavior, privilege details, and other implementation-specific features.

Use it as a quarry for ideas and edge cases, not as the primary QuantaStream CI suite.

### sqllogictest

`sqllogictest` is closer to the desired model because it focuses on SQL result correctness across engines. Its style maps well to a SQLRunner differential mode: execute a statement, capture or compare canonical results, and keep the test independent of engine internals.

This is the best external model for QuantaStream compatibility testing.

### SQLancer

SQLancer can generate schemas and queries and then use logic oracles to find result bugs. It is useful once the parser and execution stack are mature enough to survive fuzzing. It should be treated as a later robustness tool rather than a stable compatibility scorecard.

### HammerDB

HammerDB is useful for database performance benchmarks, including transaction and analytical-style workloads, but it is not a SQL compatibility suite. It may become useful for performance comparisons after the core SQL surface is more stable.

## Testing Ladder

The preferred testing ladder is:

1. Package-level unit tests for parser, planner, IR, executor kernels, diagnostics, and in-memory fixtures.
2. SQLRunner compatibility suites against controlled fixture data.
3. SQLRunner differential suites against stock MySQL for compatibility scoring.
4. Legacy-direct and TPCH kernel suites for bitmap-engine acceptance and performance confidence.
5. Full profiling or benchmark runs only when intentionally measuring performance.

This lets day-to-day development stay fast while still preserving a credible path to MySQL compatibility claims.

## Design Notes

- Compatibility testing should not force QuantaStream to copy every MySQL server behavior.
- Differences should be explicit, documented, and reported with useful categories.
- SQLRunner should remain the main compatibility control surface because it can target QuantaStream's actual architecture.
- The official MySQL suite is too broad for routine CI, but it is valuable source material.
- The long-term compatibility story should measure both SQL correctness and user-visible performance.

## Implementation Plan

See `MYSQL_COMPATIBILITY_LAB.md` for the implementation checklist and near-term work slices.
