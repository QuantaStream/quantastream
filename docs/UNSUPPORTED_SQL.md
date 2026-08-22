# SQL Boundaries

This page records SQL shapes outside the current QuantaStream 1.0 contract.
Supported behavior is recorded in [`SUPPORTED_SQL.md`](SUPPORTED_SQL.md).
Executable compatibility coverage lives in SQLRunner; forward work is tracked
in GitHub Issues.

## Persistent Table Definition

The following MySQL-style persistent table definition is not part of the
production schema path:

```sql
create table table_name (
  id bigint primary key,
  name varchar(255)
);
```

## SQL ALTER TABLE DDL

The SQL surface does not currently include the full MySQL `ALTER TABLE`
grammar. Unsupported examples include:

```sql
alter table table_name add column new_field int;
alter table table_name drop column old_field;
alter table table_name add primary key (id);
alter table child_table add foreign key (parent_id) references parent_table(id);
```

Primary-key, relationship-vector, mapper-type, and reverse-artifact changes via
broad MySQL DDL remain outside the 1.0 SQL contract.

## Transaction Rollback Semantics

The current 1.0 SQL contract does not include full MySQL transaction
semantics. Unsupported transaction behavior includes:

- full transactional rollback of applied writes;
- MySQL MVCC isolation semantics;
- undo of already-applied catalog-backed writes;
- multi-statement write transactions with isolated read-your-own-write
  behavior.

## Advanced MySQL Objects

The following MySQL object families are outside the 1.0 SQL surface:

- stored procedures and stored functions;
- triggers and events;
- user-defined SQL functions installed through MySQL plugin syntax;
- MySQL storage-engine, partitioning, tablespace, and charset/collation DDL
  options.

QuantaStream has its own extension points for bitmap-native mappers, loaders,
and administrative tooling.

## Advanced Query Forms

These query forms remain outside the current contract unless a focused suite
covers the exact shape:

```sql
with cte_name as (...)
select ... from cte_name;

with recursive ...

select ..., row_number() over (partition by ... order by ...)
from ...
```

In other words, common table expressions, recursive CTEs, SQL window functions,
and arbitrary deeply nested query composition outside covered SQLRunner shapes
are not part of the current 1.0 query surface.

## View Boundaries

The following view-related features remain outside the current surface:

- materialized views;
- MySQL `ALGORITHM`, `DEFINER`, and `SQL SECURITY` view clauses;
- exact byte-for-byte MySQL `SHOW CREATE VIEW` formatting parity.

## Dependency Cascades

Recursive dependent-object deletion is not part of the current contract.
Unsupported cascade behavior includes:

- recursive deletion of active views when a referenced table is dropped;
- recursive deletion of child relationships when a parent table is dropped;
- broad MySQL-style dependency cleanup through `DROP ... CASCADE`.

## Text Search And Collation Boundaries

The following text-search and collation features are outside the current 1.0
SQL contract:

- `MATCH ... AGAINST` over fields that are not configured as searchable;
- full MySQL collation parity across every comparison and ordering edge case;
- `REGEXP` and `RLIKE`;
- multi-column `MATCH(field_a, field_b) AGAINST (...)`;
- MySQL relevance-score projections from `MATCH(...) AGAINST(...)`;
- `WITH QUERY EXPANSION` and MySQL-specific full-text ranking behavior;
- `MATCH ... AGAINST` inside mixed boolean expression trees outside the covered
  top-level predicate shapes.
