# Unsupported SQL Syntax

Quanta intentionally supports a practical analytical SQL subset while the
bitmap execution engine matures. This document records syntax and semantics
that are currently unsupported or only partially supported so the roadmap
suites can stay focused on executable behavior instead of becoming noisy with
every known SQL gap.

Use SQLRunner `xfail` cases for near-term implementation targets that exercise
important engine behavior. Use this document for broader SQL compatibility
gaps, syntax that would require a new execution primitive, or features that are
not yet part of the current milestone.

## Planner And Engine Refactor Blockers

Several unsupported shapes are symptoms of planner/executor boundaries rather
than isolated missing functions. These should be treated as native SQL engine
design requirements and kept aligned with the public architecture guide and the
internal native-engine roadmap:

- preserve structured query shape through parsing, especially compound
  predicates containing subqueries
- bind table aliases as distinct table instances so repeated roles of the same
  base table are reliable
- classify result-producing `SELECT` statements explicitly, even when function
  expressions appear in predicates or projections
- lower membership subqueries, scalar subqueries, and non-correlated `EXISTS`
  into distinct plan nodes instead of forcing all subquery work through joins
- classify predicates as bitmap/BSI pushdown, same-table residual work,
  relationship joins, or unsupported mixed-table residual predicates before
  execution
- propagate query cancellation from the MySQL protocol layer into scans,
  projections, joins, aggregates, and remote shard work

Until those boundaries exist, unsupported shapes should fail with clear errors.
Crashes, empty-success result behavior, and hung clients are engine defects even
when the SQL feature remains unsupported.

## Pattern Matching

`LIKE` support is shape- and storage-type-dependent.

`StringEnum` fields support SQL-style `%` and `_` pattern matching by scanning
the compact enum dictionary and emitting bitmap batch predicates over matching
enum row IDs. This covers prefix, suffix, contains, and `NOT LIKE` shapes used
by the TPC-H roadmap, such as:

```sql
p_type like 'PROMO%'
p_type like '%BRASS'
p_type not like 'MEDIUM POLISHED%'
```

Backing-store string fields also support residual SQL `LIKE` evaluation over
hydrated values, which covers probes such as `p_name like 'green%'` and
`p_name like '%green%'`. This path is useful for correctness but should not be
confused with an indexed text-search plan.

Fields configured as searchable `StringHashBSI` values still have a
bloom-filter-style search path. That path is useful as a pragmatic
high-cardinality string search experiment, but it should eventually be exposed
through a clearer text-search operator or function rather than treated as the
default relational `LIKE` implementation.

Remaining pattern-matching gaps include escape semantics, broad collation
behavior, and efficient indexed planning for high-cardinality backing strings.
Future ordered string storage should use an ordered representation or secondary
index that can preserve lexicographic order. Left-anchored patterns such as
`'PROMO%'` can then be planned as a range:

```text
value >= 'PROMO' and value < next_prefix('PROMO')
```

That should be treated as a new storage/index capability, not a bloom-filter
extension.

## Function Predicate Membership

Function-expression membership predicates are not currently supported
reliably. Shapes such as:

```sql
substr(first_name, 1, 2) in ('Ab', 'An')
substr(first_name, 1, 2) not in ('Ab')
substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
```

return statement-style `Query OK, 0 rows affected` behavior instead of a
result set. OR-expanded equality predicates over the same function expression
can work and should be used in executable roadmap probes until the planner
supports function-expression `IN` / `NOT IN` predicates directly.

## Subqueries

Supported subquery behavior is limited and implementation-specific:

- `IN` and `NOT IN` use membership-oriented semi/anti join rewrites
- correlated `EXISTS` and `NOT EXISTS` with an equality predicate can rewrite
  to semi/anti joins

Unsupported subquery shapes include:

- scalar subqueries in predicates, such as
  `age > (select avg(age) from customers)`
- scalar subqueries in the `SELECT` list
- non-correlated `EXISTS` / `NOT EXISTS`
- subqueries embedded inside larger compound predicates when the parser/planner
  fails to preserve them as expression-tree nodes
- arbitrary nested query blocks and derived tables

These require explicit scalar-subquery or subquery-gate execution steps. They
should not be forced into `JoinMerge`. Compound `WHERE` subqueries are a known
native-planner requirement because several TPC-H formal shapes fail when
subqueries are only represented through the current `SqlWhere.Source` side
channel.

## Derived Tables And Common Table Expressions

Derived tables, inline views, and `WITH` common table expressions are not part
of the current supported SQL subset.

## Repeated Table Aliases And Self-Joins

Using the same base table multiple times in one query through different aliases
is not generally supported. TPC-H Q7 and Q8 both require `nation` to appear in
two roles, such as supplier nation and customer nation. Those query shapes
should remain outside executable suites until the planner, join graph, and
projection metadata can distinguish repeated base-table aliases reliably.

## Views And Temporary Tables

`CREATE VIEW`, querying named views, and view expansion are not currently
supported. TPC-H queries that depend on view definitions, such as Q15, should be
tracked here until Quanta has explicit view catalog and planner support.

Temporary tables are also unsupported. Today, `SELECT INTO` is used for export
file workflows rather than materializing temporary relational tables that can be
queried later in the same session. Future support should distinguish:

- export-oriented `SELECT INTO` file output
- session-scoped temporary tables
- any persisted or shared materialized result shape

Temporary table support will need catalog metadata, lifecycle cleanup, session
visibility rules, and mutation/load semantics that fit Quanta's bitmap storage
model.

## Conditional Expressions

Searched `CASE WHEN ... THEN ... ELSE ... END` is supported for the conditional
aggregate expression shape used by TPC-H Q14. This should not yet be treated as
complete CASE support across every clause.

Remaining conditional-expression gaps include:

- projection-only CASE shapes without aggregate wrapping
- CASE in `WHERE`, `GROUP BY`, and `ORDER BY`
- nested CASE
- simple CASE syntax
- string-valued CASE outputs
- MySQL-style `IF(condition, true_expr, false_expr)` inside aggregate
  expressions

TPC-H Q12-style conditional aggregates over joined order priority remain a
roadmap target because they depend on both conditional aggregate evaluation and
the relevant joined grouping shape.

## Field-To-Field Predicates

Quanta supports selected field-to-field predicates in join and residual-filter
paths. Same-table date comparisons used by staged TPC-H Q12 coverage now work,
including:

```sql
l_commitdate < l_receiptdate
l_shipdate < l_commitdate
```

This is still not a general field-to-field predicate guarantee. Mixed-table
residual predicates outside declared join relationships remain limited and
should return a useful unsupported error rather than being pushed down
incorrectly, for example:

```sql
ps.ps_suppkey = l.l_suppkey
```

Future planner work should classify field-to-field predicates as relationship
joins, same-table residual comparisons, or unsupported mixed-table residuals
before execution.

## Joined Predicate Coverage

Literal predicates over joined tables can work well when each predicate is
planned against the table that owns its fields. TPC-H Q19 is now covered as a
formal discounted revenue shape with mixed-table `OR` branches over
`lineitem -> part`.

Broad residual predicate evaluation over arbitrary joined rows is still
limited. Future work should continue to distinguish predicates that can be
pushed down into each joined table's bitmap/BSI reduction from predicates that
genuinely need post-join residual evaluation.

## Date And Interval Syntax

Quanta supports date comparisons using string literal forms already covered in
the TPC-H suites, such as:

```sql
o_orderdate >= '1994-01-01'
```

Formal SQL date literals and interval arithmetic are not broadly supported:

```sql
date '1994-01-01'
date '1994-01-01' + interval '1' year
```

MySQL-style helper functions such as `year(date_field)` are supported in
covered projection and grouping shapes. Some historical/custom date/time
helpers are not currently supported through QuantaStream SQL planning. Probes
for `yy(date_field)`, `yymm(date_field)`, `hourofweek(date_field)`, and
`seconds(date_field)` fail during planning with:

```text
QLBridge.plan: No datasource found
```

Those helpers should remain undocumented as supported SQL until the planner
routes them through the same residual projection path used by `year(...)`,
`mm(...)`, `monthofyear(...)`, `dayofweek(...)`, and `hourofday(...)`.

Formal SQL date-part extraction remains unsupported:

```sql
extract(year from l_shipdate)
```

## MySQL Database Semantics

Quanta only partially implements MySQL database/schema behavior. Database names
currently act mostly as connection or schema groupings, not full MySQL
catalogs. Client compatibility work, such as `USE <database>` and common schema
discovery commands, is tracked separately from analytical query support.


## Query Cancellation

The MySQL client can send `KILL QUERY <connection_id>` after `Ctrl+C`, but
long-running Quanta queries can still leave the client waiting until the
MySQL front door or query process is bounced. Query cancellation should become
an explicit execution contract in the native engine: every long-running scan,
projection, join, aggregate, and remote shard request needs a
cancellation-aware context.

## Full SQL Compatibility

The following broad SQL features should not be assumed supported unless a
specific SQLRunner suite already covers the exact shape:

- arbitrary joins outside the current bitmap relationship model
- outer join behavior beyond covered roadmap cases
- broad `HAVING` expression coverage, especially direct aggregate-expression
  `HAVING`; alias-based fixed-threshold `HAVING` has staged coverage
- arbitrary functions in `GROUP BY` and `ORDER BY`; covered cases include
  single-table `year(...)`, `substr(...)`, and selected function ordering
- window functions
- recursive queries
- transaction isolation semantics beyond the current Quanta mutation workflow
- DDL compatibility with MySQL beyond Quanta-supported table creation paths
