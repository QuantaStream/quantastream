# SQL Roadmap Suite Format

SQL roadmap suites use YAML and describe externally visible behavior. They do
not prescribe how Quanta implements a feature.

```yaml
version: 1
name: joins

tests:
  - id: joins.inner.rows
    status: supported
    kind: query
    order: rowsort
    capabilities: [inner-join]
    feature: joins_inner
    compatibility: mysql
    requires: [select, inner_join]
    sql: |
      select c.cust_id, o.order_id
      from customers_qa as c
      inner join orders_qa as o on o.cust_id = c.cust_id
    expect:
      columns: [cust_id, order_id]
      rows:
        - ["1", "1001"]
        - ["1", "1002"]

  - id: aggregates.multiple
    status: xfail
    kind: query
    capabilities: [multiple-aggregates]
    issue: "roadmap"
    sql: select count(*), max(order_id) from orders_qa
    expect:
      rows:
        - [4, "1004"]
```

## Case status

- `supported`: must pass.
- `xfail`: retained roadmap goal. A failure is reported as `XFAIL`; success is
  reported as `XPASS` and makes the suite fail until the case is promoted.
- `skip`: not executed.

## Case timeout

Cases default to a 30 second timeout. Long-running roadmap cases may declare a
Go-style duration string such as `timeout: 60s`. Use this sparingly so slow
queries remain visible.

## Case kind

- `query`: validates complete rows and optional column names and database types.
- `statement`: validates success, an expected error, or `affected_rows`.
- `admin`: deprecated bootstrap shorthand. SQLRunner normalizes `create name`,
  `drop name`, and `truncate name` into SQL DDL and executes them as statements.
  New suites should use `statement` with SQL directly.

`kind` may be omitted for statements beginning with `select` or `show`.

## Result ordering

- `exact`: rows must be returned in the listed order.
- `rowsort`: expected and actual rows are sorted before comparison.

Use `exact` when ordering is part of the SQL contract. Use `rowsort` when row
order is intentionally unspecified.

## Values and errors

YAML `null` represents SQL `NULL`. Other values are compared through their
database text representation. Expected errors use a case-insensitive substring:

```yaml
expect:
  error_contains: attribute 'missing' not found
```

During migration from legacy SQLRunner scripts, a query may use:

```yaml
expect:
  row_count: 3
```

`row_count` preserves existing coverage but is weaker than `rows`. New and
expanded tests should prefer complete expected rows.

Numeric cells are parsed and compared numerically when both expected and actual
values are valid numbers. By default SQLRunner allows a small relative
tolerance to avoid formatting-only differences such as scientific notation. A
query case may opt into an absolute tolerance when the expected value is rounded
or when equivalent engines expose slightly different floating-point precision:

```yaml
expect:
  numeric_tolerance: 0.01
  rows:
    - ["22923.03"]
```

`numeric_relative_tolerance` may also be set for cases that need a custom
scale-relative threshold. These tolerances only apply to numeric cells; text and
`NULL` comparisons remain exact.

## Expected diagnostics

Inspection-oriented engines may expose internal planner or runtime diagnostics
as result rows. A supported case can declare the exact diagnostic codes it
expects while still validating those rows:

```yaml
status: supported
expected_diagnostics: [unsupported_join]
sql: |
  select c.first_name, o.order_id
  from customers_qa as c
  inner join orders_qa as o on c.cust_id = o.cust_id
expect:
  rows:
    - [sql, supported, false, ""]
    - [diagnostic, "001", unsupported_join, "unsupported_join: relationship-vector join execution is not wired yet: c.cust_id -> o.cust_id"]
```

This is intended for `runtime-inspect` cases that deliberately cross a known
implementation boundary. Normal execution suites should continue to use
`expect.error_contains` for user-visible SQL errors.

## Compatibility metadata

MySQL compatibility suites may add optional metadata used by compatibility
reports:

```yaml
feature: scalar_functions
compatibility: mysql
requires: [select, lower, substring]
```

`feature` names the reporting bucket. `compatibility` names the reference
surface, currently `mysql`, `quanta`, or `quanta_extension`. `requires` carries
smaller capability tags for future scorecards and filtering.
