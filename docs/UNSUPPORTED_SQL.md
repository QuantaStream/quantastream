# SQL Boundaries

QuantaStream supports the MySQL wire protocol and a focused analytical SQL
surface for everyday query, integration, and validation work. The supported
surface includes projections, predicates, ordering, limits, grouping,
aggregates, joins, subqueries, views, derived tables, temporary tables, CTAS,
prepared-statement execution, and MySQL client metadata used by common tools.

This page records the narrow SQL shapes outside the current QuantaStream 1.0
contract. Executable compatibility coverage lives in SQLRunner; forward work is
tracked in GitHub Issues.

## Persistent Table Definition

Production QuantaStream tables are descriptor-driven. A first-class persistent
table is defined by catalog/configuration metadata and then activated with:

```sql
create table table_name;
```

The following MySQL-style persistent table definition is not the production
schema path:

```sql
create table table_name (
  id bigint primary key,
  name varchar(255)
);
```

Use descriptor files for configured persistent tables. Use
`CREATE TEMPORARY TABLE ... (...)` for session-scoped scratch tables, or
`CREATE TABLE ... AS SELECT ...` for persistent materialization from a query.

## SQL ALTER TABLE DDL

QuantaStream exposes table structure through descriptors and administrative
tools, not the full MySQL `ALTER TABLE` grammar.

The SQL surface does not currently include:

```sql
alter table table_name add column new_field int;
alter table table_name drop column old_field;
alter table table_name add primary key (id);
alter table child_table add foreign key (parent_id) references parent_table(id);
```

Primary keys, relationship vectors, mapper types, and reverse relationship
artifacts are physical schema decisions in QuantaStream. They should be
declared in the table descriptor or managed through explicit administrative
flows rather than inferred from broad MySQL DDL syntax.

## Transaction Rollback Semantics

QuantaStream accepts common transaction statements for client compatibility:

```sql
begin;
commit;
rollback;
set autocommit = 0;
set autocommit = 1;
```

The current mutation path applies catalog-backed writes immediately.
`ROLLBACK` does not provide full MySQL MVCC semantics or undo previously
applied writes. Treat QuantaStream transaction statements as session/client
compatibility controls unless a future release documents durable transactional
write rollback.

## Advanced MySQL Objects

The 1.0 SQL surface is centered on tables, views, temporary tables, CTAS, and
analytical queries. The following MySQL object families are outside that
surface:

- stored procedures and stored functions;
- triggers and events;
- user-defined SQL functions installed through MySQL plugin syntax;
- MySQL storage-engine, partitioning, tablespace, and charset/collation DDL
  options.

QuantaStream has its own extension points for bitmap-native mappers, loaders,
and administrative tooling.

## Advanced Query Forms

The analytical query surface is intentionally broad enough for the current
TPC-H and MySQL compatibility suites. These query forms remain outside the
current contract unless a focused suite covers the exact shape:

```sql
with recursive ...

select ..., row_number() over (partition by ... order by ...)
from ...
```

In other words, recursive CTEs and SQL window functions are not part of the
current 1.0 query surface.

## View Boundaries

Logical views are supported, including joins inside view definitions. The
following view-related features remain outside the current surface:

- materialized views;
- MySQL `ALGORITHM`, `DEFINER`, and `SQL SECURITY` view clauses;
- exact byte-for-byte MySQL `SHOW CREATE VIEW` formatting parity.

Use CTAS when a query result should become a persistent materialized table.

## Dependency Cascades

QuantaStream protects catalog integrity by requiring explicit dependent-object
management. `DROP TABLE` should not be treated as a recursive dependency
deletion mechanism for active views or parent/child relationships.

Simple `DROP ... CASCADE` syntax is accepted where covered, but recursive
dependent-object deletion is not the current contract. Drop dependent views,
child relationships, or configured tables intentionally.

## Text Search And Collation

SQL `LIKE` and `NOT LIKE` are supported for covered bitmap and residual string
paths. The following text semantics are outside the current surface:

- full MySQL collation parity across every comparison and ordering edge case;
- `REGEXP` and `RLIKE`;
- indexed full-text search syntax such as `MATCH ... AGAINST`.

StringEnum, StringLexBSI, and backing-string representations should be chosen
from the descriptor based on the query pattern the application needs.
