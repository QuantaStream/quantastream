# Schema Design Guide

QuantaStream schemas are physical design documents, not just logical table
definitions. The mapping strategy chosen for each attribute determines how the
engine can filter, join, group, aggregate, project, and reload that data.

The field-level configuration reference lives in
[`configuration/SCHEMA_CONFIG_REFERENCE.md`](../configuration/SCHEMA_CONFIG_REFERENCE.md).
This guide focuses on how to choose schema shapes.

For existing MySQL databases, the
[`qstream-migrate`](https://github.com/QuantaStream/qstream-migrate) toolkit can
inspect source metadata and representative data, then generate a draft
QuantaStream schema plan for review. Treat the generated output as a starting
point for physical design: relationship modeling, time partitioning, and
high-cardinality string choices still benefit from workload knowledge.

## Mental Model

Each table is stored as bitmap and BSI-backed fields keyed by QuantaStream row
IDs. For many analytical queries, the best schema is the one that lets
QuantaStream answer
as much of the query as possible with bitmap/BSI operations before rows are
materialized for projection.

Important schema concepts:

- `tableName` names the physical table/index.
- `primaryKey` identifies the logical key used by loads and SQL mutation paths.
- `columnID: true` means the field value is also the QuantaStream row ID.
- `mappingStrategy` chooses the physical representation.
- `timeQuantumType` partitions table data into time-based shards.
- `ParentRelation` models relationships to parent tables.
- string strategies decide whether strings are enum-like, reconstructed from a
  backing store, or searched through a secondary index.

## Inspecting Physical Schema With `DESCRIBE`

QuantaStream supports a MySQL-style `DESCRIBE <table>` view that layers
QuantaStream-specific physical schema information onto familiar SQL metadata. This is
useful when validating that a loaded table is using the intended bitmap, BSI,
string, and relationship strategies.

The `Extra` column exposes the physical mapping strategy, and the `Key` column
adds QuantaStream relationship and key hints such as `FK: <table>`, `PK`, and `PK*`.
Treat this as a diagnostic view rather than a lossless replacement for the
schema YAML: when a field participates in multiple concepts, such as a
relationship and a logical key, the schema file remains the source of truth.

Example from the TPC-H `lineitem` table:

```text
mysql> describe lineitem;
+-----------------+--------+------+--------------+---------+----------------+
| Field           | Type   | Null | Key          | Default | Extra          |
+-----------------+--------+------+--------------+---------+----------------+
| l_comment       | string | true | -            | ""      | StringLexBSI   |
| l_commitdate    | time   | true | -            | ""      | TimestampBSI   |
| l_discount      | number | true | -            | ""      | FloatScaleBSI  |
| l_extendedprice | number | true | -            | ""      | FloatScaleBSI  |
| l_linenumber    | int    | true | PK           | ""      | IntBSI         |
| l_linestatus    | string | true | -            | ""      | StringEnum     |
| l_orderkey      | int    | true | FK: orders   | ""      | ParentRelation |
| l_partkey       | int    | true | FK: part     | ""      | ParentRelation |
| l_quantity      | int    | true | -            | ""      | IntBSI         |
| l_receiptdate   | time   | true | -            | ""      | TimestampBSI   |
| l_returnflag    | string | true | -            | ""      | StringEnum     |
| l_shipdate      | time   | true | PK*          | ""      | TimestampBSI   |
| l_shipinstruct  | string | true | -            | ""      | StringEnum     |
| l_shipmode      | string | true | -            | ""      | StringEnum     |
| l_suppkey       | int    | true | FK: supplier | ""      | ParentRelation |
| l_tax           | number | true | -            | ""      | FloatScaleBSI  |
+-----------------+--------+------+--------------+---------+----------------+
```

This single view shows the layered design: low-cardinality dimensions use
`StringEnum`, measures and dates use BSI-backed strategies, high-cardinality
comments use `StringLexBSI`, and join paths use `ParentRelation`.

## Choosing Mapping Strategies

### Numeric, Date, And Measure Fields

Use BSI-backed strategies for fields that need range predicates, ordering,
aggregation, or arithmetic:

- `IntBSI` for integer values and surrogate keys.
- `FloatScaleBSI` for fixed-scale decimal values.
- `TimestampBSI` for timestamp values, with granularity such as second,
  millisecond, microsecond, or nanosecond.
- date/time bucket strategies such as `YearToDay` when bucketed bitmap
  semantics are intended instead of general range arithmetic.

BSI fields are usually the right choice for facts and measures: prices,
quantities, balances, dates, timestamps, and numeric foreign keys.

### Low-Cardinality Strings

Use `StringEnum` for dimension-like strings with small cardinality, especially
when the field appears in filters, grouping, or top-N style analytics.

Good examples:

- `region.r_name`
- `nation.n_name`
- `customer.c_mktsegment`
- `orders.o_orderstatus`
- `lineitem.l_returnflag`
- `lineitem.l_shipmode`

`StringEnum` keeps string predicates bitmap-native and avoids the cost of
rehydrating arbitrary strings during filtering.

Avoid using `StringEnum` for high-cardinality identifiers, comments, addresses,
or free-form text. High-cardinality enum fields create many sparse bitmap
values and do not match the intended access pattern.

### High-Cardinality Projected Strings

Use `StringLexBSI` when string values have higher cardinality and must still
participate in bitmap-native filtering or projection.

Examples:

- names
- addresses
- phone numbers
- clerk/order labels
- comments

When the configured `length` is zero or negative, QuantaStream encodes the full
UTF-8 value into the BSI and does not use a backing store. This is a good fit
for bounded identifiers such as names, brands, phone numbers, and short labels.
For longer free-form fields, configure a prefix length and `maxLen`; comments in
the TPC-H schema use an eight-character BSI prefix with a KV-backed remainder.
Exact equality and `IN` predicates are currently lowered only for full-inline
`StringLexBSI` fields. Prefix-plus-remainder fields need a follow-up suffix
rehydration check before they can be treated as exact matches.

`StringLexBSI` still exists for compatibility with older schemas, but new
schema work should prefer `StringLexBSI`. Hash strings solve equality lookups,
but projection can become expensive because QuantaStream must use a backing
store to rehydrate values. That cost matters most when large result sets
project string columns.

### Searchable Strings

QuantaStream has secondary searchable-string support backed by a bloom-filter
style index. This is currently a pragmatic high-cardinality string workaround
rather than a complete SQL string indexing model.

Use searchable strings for fields where lookup/search behavior is more
important than SQL range semantics. It can provide a practical path for
text-ish or high-cardinality fields without pretending they are low-cardinality
enums.

Current limitation: string range predicates are not a first-class capability.
Searchable strings should be documented and tested as search support, not as a
replacement for ordered string indexing.

## Relations And Keys

For analytical relationship graphs, prefer numeric surrogate keys that can be
represented directly as row IDs.

The TPC-H schemas under [`tpc-h-benchmark/config`](../tpc-h-benchmark/config)
use this pattern:

- parent key fields such as `customer.c_custkey`, `orders.o_orderkey`, and
  `supplier.s_suppkey` are marked `columnID: true`
- child relationship fields use `ParentRelation`
- foreign-key values point directly to the parent table's row IDs

This avoids extra primary-key lookup work during joins and makes bitmap/BSI
relationship traversal cheaper.

A `ParentRelation` can also opt into maintained relationship artifacts:

```yaml
relationshipArtifacts:
  parentToChild: true
```

Use this for relationship edges where parent-domain filters are frequently
expanded into child-domain candidates. The runtime treats this as an eligibility
signal, not a command: the optimizer may still choose a regular
relationship-vector path when that is cheaper for a specific query shape.

When possible:

- make stable integer primary keys the row ID via `columnID: true`
- model child-to-parent links with `ParentRelation`
- avoid string primary keys in high-volume join paths
- avoid relationship designs that require repeated lookup of string keys during
  query execution

For streaming sources that naturally arrive as parent envelopes with child
arrays, model the parent-side array with `ChildRelation` and the child-side
edge back to the parent with `ParentRelation`. This lets the loader expand the
child rows and map the enclosing parent relationship within the same session.
Use this for event-shaped streams, not as a substitute for parent-first
table-by-table batch loads.

## Time Quantum Design

`timeQuantumType` partitions table data into time-based shards. This can be a
major win when queries include predicates on the partitioning date/time field.
It can also increase fan-out when queries touch non-date fields across many
time shards.

Use time partitioning when:

- data naturally arrives or expires by time
- most large queries include bounded date predicates
- operational retention/purge requirements are time-based

Be cautious when:

- queries frequently scan all time
- joins project non-date fields from a heavily partitioned table
- a small dimension table does not benefit from time partitioning

TPC-H highlights both sides. Date predicates in formal queries should
eventually help restrict shard selection. But projecting order/date fields
without early time pruning can still trigger broad BSI work across many shards.

## Table Design Workflow

1. Identify the row ID.

   Prefer an integer surrogate key that can be marked `columnID: true`.

2. Classify fields by query behavior.

   Decide whether each field is primarily used for filtering, grouping,
   joining, aggregation, projection, search, or metadata.

3. Choose physical representations.

   Use BSI for numeric/range/measure values, `StringEnum` for low-cardinality
   dimensions, `StringLexBSI` for projected high-cardinality strings, and
   searchable string support for search-style high-cardinality fields.

4. Model relationships deliberately.

   Use `ParentRelation` for foreign keys and prefer direct parent row-ID
   references.

5. Decide whether time partitioning is worth the shard fan-out.

   Use `timeQuantumType` when date predicates and retention behavior justify
   it.

6. Backfill SQLRunner coverage.

   Add schema-specific queries that prove the intended access paths: filters,
   joins, grouped aggregates, projection, and reload behavior.

## Common Anti-Patterns

- using `StringEnum` for high-cardinality free-form text
- projecting large high-cardinality string fields in broad scans when the query only
  needs them after filtering
- time-partitioning tables that are usually queried without date predicates
- using string keys in hot join paths when numeric surrogate keys are available
- assuming string range predicates are supported because numeric range
  predicates are supported
- treating schema YAML as only logical metadata rather than physical design

## TPC-H Notes

TPC-H is a good benchmark for joins, grouping, numeric/date predicates, and
relationship traversal. It is not a strong benchmark for arbitrary
high-cardinality string search.

For TPC-H:

- low-cardinality dimensions such as region, nation, segment, return flag, and
  ship mode fit `StringEnum`
- high-cardinality names, addresses, phones, comments, and clerk fields fit
  `StringLexBSI`
- surrogate integer keys and `ParentRelation` links are central to efficient
  joins
- date fields expose the importance of time-aware planning and shard pruning

## Roadmap

Potential future schema/storage work:

- ordered string encodings that preserve lexicographic order in BSI form
- dictionary or ordinal string encodings for ordered high-cardinality values
- prefix or partial-string BSI indexes for pruning before backing-store lookup
- richer searchable-string support and SQL-visible search predicates
- planner hints or schema annotations for join order and projection cost
- analyzer tooling that profiles representative data and proposes schemas
- clearer tooling to analyze existing schemas and recommend mapping changes
