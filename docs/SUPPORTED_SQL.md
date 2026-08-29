# Supported SQL

QuantaStream supports a practical analytical SQL subset plus a small set of
QuantaStream-specific SQL extensions. This document records supported behavior
that is useful to users, including behavior that is intentionally
QuantaStream-specific rather than portable MySQL syntax.

Precise SQL boundaries outside the current 1.0 surface are tracked separately
in [`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md).

## Core Query Surface

QuantaStream supports the core analytical query shapes used by current
SQLRunner and TPC-H validation suites:

- `SELECT` projections, aliases, predicates, ordering, `LIMIT`, and `OFFSET`;
- `GROUP BY`, aggregate functions, grouped ordering, and covered `HAVING`
  shapes;
- inner joins and covered outer joins over declared relationship paths;
- `UNION` and `UNION ALL` in covered query-composition shapes;
- prepared-statement execution through MySQL-compatible clients;
- MySQL metadata queries used by command-line clients, MySQL Workbench, and
  driver inspection paths.

## Subqueries

QuantaStream supports subqueries for the main analytical shapes used by the
current compatibility suites:

- scalar subqueries in `SELECT`;
- scalar aggregate subqueries in predicates;
- `IN` and `NOT IN` membership subqueries;
- row-value `IN` subqueries for covered tuple shapes;
- correlated `EXISTS` and `NOT EXISTS` with equality predicates;
- subqueries over derived-table sources.

Subquery support is part of the current SQL surface. Recursive query forms and
arbitrary deeply nested query blocks are tracked as advanced query boundaries in
[`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md).

## Date And Time Helpers

QuantaStream supports a focused set of date/time scalar helpers in covered
select-list and grouping shapes:

- `year(date_value)` returns the four-digit year.
- `mm(date_value)` returns the month number.
- `monthofyear(date_value)` is an alias for `mm(date_value)`.
- `dayofweek(date_value)` returns the weekday number using Go's weekday
  convention, where Sunday is `0`.
- `hourofday(date_value)` returns the hour of day.

Example:

```sql
select order_id,
       year(order_date) as order_year,
       mm(order_date) as order_month,
       dayofweek(order_date) as order_dow,
       hourofday(order_date) as order_hour
from orders_qa;
```

These helpers are the covered date/time surface. Additional MySQL-style aliases
such as `month(...)` and `hour(...)` should be added with SQLRunner coverage
when they are exposed.

## Type Coercion Helpers

QuantaStream supports a focused set of compatibility type-coercion helpers in
covered select-list and predicate shapes:

- `tostring(value)` coerces a value to string.
- `toint(value)` coerces a value to integer.
- `tonumber(value)` coerces a value to number.

Example:

```sql
select first_name, tostring(age) as age_text
from customers_qa;

select first_name
from customers_qa
where toint(age) >= 40;
```

MySQL `CAST(... AS ...)` compatibility should be treated separately from these
function-style helpers.

## String Scalar Helpers

QuantaStream supports a focused set of string scalar helpers in covered
select-list shapes:

- `lower(value)` lowercases a string value; `lcase(value)` is also covered.
- `upper(value)` uppercases a string value; `ucase(value)` is also covered.
- `length(value)` returns the string length; `char_length(value)` is also
  covered.
- `substr(value, start, length)` returns a substring; `substring(...)` and
  `mid(...)` are also covered. The SQL-facing behavior is one-based for the
  covered shapes.

Predicate coverage currently includes `lower(...)`, `lcase(...)`,
`upper(...)`, `ucase(...)`, `length(...)`, `char_length(...)`, `substr(...)`,
`substring(...)`, and `mid(...)` shapes.

Example:

```sql
select first_name, lower(city) as city_lower
from customers_qa;

select first_name, substr(first_name, 1, 2) as name_prefix
from customers_qa;
```

These functions are covered in focused scalar-expression shapes. Add aliases or
broader mixed-projection, join, grouping, and aggregate shapes with SQLRunner
coverage.

## Text Search

QuantaStream supports native text-search predicates for text fields configured
as searchable in the table descriptor:

```sql
select c_comment
from customer
where match(c_comment) against ('epitaphs nag' in natural language mode);
```

The current SQL contract is predicate-oriented: `MATCH(field) AGAINST (...)`
resolves search terms through the string-search service, materializes matching
term hashes, and lowers the predicate to bitmap-native equality over the hidden
search artifact. This is intended for fast filtering, not MySQL relevance-score
ranking.

Searchable text fields use two durable artifacts:

- the string-search KV index, which maps searchable text to term filters;
- a hidden BSI hash field, which maps table rows to searchable text hashes.

Those artifacts are part of the QuantaStream storage model and participate in
backup/restore through the local storage backup manifest.

Deployment support: text-search predicates are covered in distributed/native
cluster paths. The single-node standard endpoint supports the same schema model,
but local text-search execution is not enabled until the standard-mode
StringSearch helper is mounted.

Covered syntax:

- one field inside `MATCH(...)`;
- string literal or prepared-statement parameter inside `AGAINST(...)`;
- optional `IN NATURAL LANGUAGE MODE` or `IN BOOLEAN MODE` parsing, with both
  modes using the current term-search behavior.

Advanced full-text ranking and MySQL text-search edge cases are tracked in
[`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md).

## Views

QuantaStream supports named views for the current MySQL compatibility surface:

- `CREATE VIEW ... AS SELECT ...`
- `CREATE OR REPLACE VIEW ... AS SELECT ...`
- optional inline view column aliases, such as
  `CREATE VIEW v (a, b) AS SELECT ...`
- `DROP VIEW` and `DROP VIEW IF EXISTS`
- querying named views through the normal SELECT path
- joins inside supported view definitions

Views are stored in the catalog and expanded at query-planning time. They are
logical views, not materialized views. MySQL metadata and advanced view syntax
boundaries are tracked in [`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md).

## Derived Tables

QuantaStream supports focused MySQL-compatible query composition shapes:

- derived tables in `FROM`;
- derived tables with inner predicates and outer predicates;
- derived tables used as join sources;
- grouped aggregate derived sources.

These are logical query-composition features, not temporary storage. Advanced
query-form boundaries outside the current 1.0 surface are tracked in
[`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md).

## Temporary Tables And CTAS

QuantaStream supports session-scoped temporary tables for the current MySQL
compatibility surface:

- `CREATE TEMPORARY TABLE ... (...)`;
- `CREATE TEMPORARY TABLE ... AS SELECT ...`;
- `CREATE TEMPORARY TABLE ... LIKE ...`;
- `INSERT`, `INSERT ... SELECT`, `SELECT`, and `DROP TEMPORARY TABLE` over
  temporary tables.

QuantaStream also supports persistent `CREATE TABLE ... AS SELECT ...` as the
SQL materialization path. Production table definitions remain descriptor-driven:
use a catalog/YAML descriptor with `CREATE TABLE table_name` to activate a
first-class configured table. Inline MySQL `CREATE TABLE (...)` definitions are
not the production schema path.

## Schema DDL

QuantaStream supports descriptor-backed schema activation and focused online
schema changes:

- `CREATE TABLE table_name` activates a catalog/YAML descriptor for a
  first-class configured table.
- `ALTER TABLE table_name ADD COLUMN column_name type` appends a nullable
  column to an active table, including tables that already contain data.
  Existing rows read the new column as `NULL`; future writes may populate it.
- `ALTER TABLE table_name ADD PRIMARY KEY (...)` is covered for the guarded
  metadata path where validation can prove the table is empty or otherwise safe
  to promote.

Mapper-specific schema design remains descriptor-driven. Broad MySQL DDL
grammar, default backfills, column drops, relationship changes, and partitioning
changes are tracked in [`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md).

## QuantaStream Custom SQL

### `timediff(end_time, start_time, unit)`

`timediff(...)` is a QuantaStream scalar function for computing elapsed time
between two date/time values. Current SQLRunner coverage exercises the
`'hours'` unit in select-list, predicate, aggregate-filter, and joined
select-list shapes.

Examples:

```sql
select order_id, timediff(ship_date, order_date, 'hours') as ship_hours
from orders_qa;

select count(*) as delayed_order_count
from orders_qa
where timediff(ship_date, order_date, 'hours') > 3;
```

Covered result values include integer and fractional hour strings, such as
`'3'`, `'4.5'`, and `'2'`.

This is custom QuantaStream SQL, not a MySQL-compatible `TIMEDIFF()`
implementation.
Additional units, null handling, timezone behavior, result typing, negative
durations, and parity expectations belong in focused compatibility coverage
when those behaviors are exposed.

### `topn(field[, count])`

`topn(field[, count])` is a QuantaStream aggregate extension that returns the
most frequent values for a field, their counts, and their percentage of the
scanned result set. The optional `count` argument is a positive integer literal
that limits the concrete value rows; when remaining values exist, QuantaStream
adds an `OTHER:` bucket before the final `TOTAL:` row.

Example:

```sql
select topn(l_shipmode, 5)
from lineitem;
```

Example result shape:

```text
+-----------------+------------+--------------+
| topn_l_shipmode | topn_count | topn_percent |
+-----------------+------------+--------------+
| TRUCK           |       8710 |        14.47 |
| MAIL            |       8669 |        14.41 |
| FOB             |       8641 |        14.36 |
| REG AIR         |       8616 |        14.32 |
| RAIL            |       8566 |        14.24 |
| OTHER:          |      16973 |        28.20 |
| TOTAL:          |      60175 |       100.00 |
+-----------------+------------+--------------+
```

This is custom QuantaStream SQL, not MySQL-compatible syntax. It should be
retained because it maps naturally to bitmap-native cardinality work and is
useful for profiling categorical distributions.

Additional `topn(...)` coverage should characterize:

- alias behavior
- ordering guarantees
- tie behavior
- filtered input behavior
- joined input behavior
- interaction with other aggregates
