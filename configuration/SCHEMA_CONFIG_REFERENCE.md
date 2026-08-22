# Schema Config Reference

This is the field-level reference for QuantaStream schema configuration files.
For modeling guidance and tradeoffs, start with the
[Schema Design Guide](../docs/SCHEMA_DESIGN.md). This document focuses on the
exact file layout, YAML keys, mapper names, and mapper-specific options.

## Configuration Layout

QuantaStream reads table schemas from a configuration root. In file-backed
single-node mode, the common layout is:

```text
configuration-root/
  CATALOG_OBJECTS
  customer/
    schema.yaml
  orders/
    schema.yaml
  lineitem/
    schema.yaml
  views/
    q3_order_line_base.yaml
```

Table schemas live at:

```text
<config-dir>/<table-name>/schema.yaml
```

Logical view definitions live at:

```text
<config-dir>/views/<view-name>.yaml
```

`CATALOG_OBJECTS` is the file-backed active catalog manifest. It tracks active
tables and views by schema name, object name, object type, creation date, and
modification date. If a `CATALOG_OBJECTS` file exists, QuantaStream uses it as
the active catalog. Older or hand-staged directories may rely on discovered
`<table>/schema.yaml` directories when no active table manifest is present.

Distributed mode stores catalog metadata in Consul. The same table and view
metadata shapes are used, but the backing store is Consul rather than local
files.

## Minimal Table Schema

```yaml
tableName: customer
primaryKey: c_custkey
selector: type="customer"
attributes:
- fieldName: c_custkey
  sourceName: /data/c_custkey
  type: Integer
  mappingStrategy: IntBSI
  columnID: true
- fieldName: c_mktsegment
  sourceName: /data/c_mktsegment
  type: String
  mappingStrategy: StringEnum
```

## Table-Level Keys

| Key | Required | Description |
| --- | --- | --- |
| `tableName` | yes | Physical table/index name. This should match the table directory name. |
| `primaryKey` | yes | Logical primary-key field. Compound keys use `+`, for example `l_orderkey+l_linenumber`. |
| `selector` | no | Loader routing expression used to match source events to this table, for example `type="orders"`. |
| `timeQuantumType` | no | Time partitioning quantum, commonly `YMD`. Omit for non-time-partitioned tables. |
| `timeQuantumField` | conditional | Field used for time partitioning. Required when `timeQuantumType` is set unless the first compound primary-key field is the date/time partition field. |
| `attributes` | yes | List of field definitions. |
| `isViewOf` | no | Legacy physical-view metadata. Logical SQL views created with `CREATE VIEW` are represented under `views/`, not as table schemas. |

### Primary Keys

`primaryKey` names one or more attributes from `attributes`. For a compound key,
join field names with `+`:

```yaml
primaryKey: l_orderkey+l_linenumber
```

When a table is time-partitioned, the partition field must be a `Date` or
`DateTime` attribute. If `timeQuantumField` is omitted, the first field in a
compound primary key is treated as the partition field and must be date-like.

### Selectors

Selectors are used by loaders that receive mixed event streams. They allow one
incoming stream to route different record shapes to different table schemas.

Example:

```yaml
selector: type="lineitem"
```

When no selector is configured, the caller is expected to route records to the
table explicitly.

### Time Quantum Partitioning

```yaml
timeQuantumType: YMD
timeQuantumField: l_shipdate
```

Use time quantum partitioning when the table is naturally queried, retained, or
loaded by time. It is a physical storage decision: it can reduce work for
bounded time predicates, but it can increase fan-out for broad scans that do
not constrain the time field.

## Attribute-Level Keys

| Key | Required | Description |
| --- | --- | --- |
| `fieldName` | yes | SQL-visible column name. If omitted in older schemas, it may default from `sourceName`; new schemas should set it explicitly. |
| `sourceName` | no | Source event or source file path for the value, commonly `/data/<source-field>`. |
| `type` | yes | Logical value type: `String`, `Integer`, `Float`, `Date`, `DateTime`, `Boolean`, `JSON`, `NotExist`, or `NotDefined`. |
| `mappingStrategy` | yes | Physical mapper used to encode the field. See [Mapping Strategies](#mapping-strategies). |
| `foreignKey` | conditional | Parent table name for `ParentRelation` fields. |
| `childTable` | conditional | Child table name for child-side relationship helper metadata. |
| `relationshipArtifacts` | no | Optional maintained relationship artifacts. Currently supports `parentToChild: true`. |
| `columnID` | no | When true, the attribute value is also the QuantaStream row ID. This is common for parent table surrogate keys. |
| `scale` | conditional | Decimal scale for `FloatScaleBSI`. For TPC-H money/decimal fields, `scale: 2` is typical. |
| `values` | no | Explicit enum values for value-based mappers. Each entry can include `value`, `rowID`, and `desc`. |
| `configuration` | no | Mapper-specific options. Required by some custom or specialized mappers. |
| `maxLen` | no | Maximum source string length. Used by string encoding metadata and SQL type display. |
| `required` | no | Write-path validation hint. When true, online inserts should provide a value or a default. |
| `defaultValue` | no | Default expression used by blind inserts when no value is supplied. Example: `now()`. |
| `searchable` | no | Enables secondary text-search metadata for string fields. The loader/server creates supporting search artifacts. |
| `nonExclusive` | no | Current schema YAML key for set-valued bitmap fields. This maps to planner/catalog `multiplicity=set`; `multiplicity` is not accepted as a schema YAML key yet. |
| `system` | no | Marks a generated internal field. User schemas normally should not set this manually. |
| `callTransform` | no | Legacy/custom mapper hook. Use only with mapper code that explicitly expects it. |
| `sourceOrdinal` | no | Source column order for bulk loaders and generated descriptors. |

### Source And Field Names

`fieldName` is the SQL-visible column name. `sourceName` is the path or column
name used by loaders. New schemas should specify both.

```yaml
- fieldName: l_shipmode
  sourceName: /data/l_shipmode
  type: String
  mappingStrategy: StringEnum
```

### Column IDs

`columnID: true` means the attribute value is the physical QuantaStream row ID.
This is useful for stable integer surrogate keys in parent/dimension tables:

```yaml
- fieldName: c_custkey
  sourceName: /data/c_custkey
  type: Integer
  mappingStrategy: IntBSI
  columnID: true
```

For hot join paths, prefer numeric parent primary keys that can be represented
directly as row IDs. Child tables can then reference those rows through
`ParentRelation` without repeated string-key lookup work.

### Default Values

`defaultValue` is evaluated by online insert paths when a value is omitted.
Bulk loader enforcement is intentionally conservative and should not be
treated as a substitute for clean source data.

```yaml
- fieldName: last_updated_dt
  sourceName: /data/last_updated_dt
  type: DateTime
  mappingStrategy: TimestampBSI
  configuration:
    granularity: second
  defaultValue: now()
```

## Logical Types

| Type | Use |
| --- | --- |
| `String` | Text, labels, enum-like values, identifiers, comments, and searchable fields. |
| `Integer` | Integer values, surrogate keys, relationship keys, counters, and numeric dimensions. |
| `Float` | Decimal or floating-point values, usually with `FloatScaleBSI` and `scale`. |
| `Date` | Date values without time-of-day semantics. |
| `DateTime` | Timestamp values. Use `TimestampBSI` for range, ordering, and grouping support. |
| `Boolean` | Boolean values. |
| `JSON` | Structured values consumed by custom mappers. |
| `NotExist` | Analyzer output when a source type could not be determined. Avoid in hand-written production schemas. |
| `NotDefined` | Placeholder/unknown type. Avoid in hand-written production schemas. |

## Mapping Strategies

Recommended production mappers:

| Mapper | Typical type | Query behavior |
| --- | --- | --- |
| `IntBSI` | `Integer` | Integer BSI for equality, range, ordering, grouping, aggregation, and projection. |
| `FloatScaleBSI` | `Float` | Fixed-scale decimal values stored in BSI form. |
| `TimestampBSI` | `Date` or `DateTime` | Timestamp/date values stored in BSI form with configurable granularity. |
| `StringEnum` | `String` | Low-cardinality strings stored as bitmap enum values. Best for dimensions and grouping keys. |
| `StringLexBSI` | `String` | Lexical string encoding for high-cardinality projected strings. Can be full-inline or prefix plus backing remainder. |
| `ParentRelation` | `Integer` | Child-to-parent relationship edge. Requires `foreignKey`. |
| `BoolDirect` | `Boolean` | Boolean bitmap mapping. |
| `UUIDBSI` | `String` | UUID encoded into BSI form for UUID-shaped identifiers. |

## Mapper-Specific Options

### `IntBSI`

Use for integer values that need equality, range, grouping, ordering, or
projection.

```yaml
- fieldName: o_orderkey
  sourceName: /data/o_orderkey
  type: Integer
  mappingStrategy: IntBSI
  columnID: true
```

### `FloatScaleBSI`

Use for fixed-scale decimal values. `scale` controls how many decimal places
are preserved when values are converted into BSI integers.

```yaml
- fieldName: l_extendedprice
  sourceName: /data/l_extendedprice
  type: Float
  mappingStrategy: FloatScaleBSI
  scale: 2
```

### `TimestampBSI`

Use for date/time fields that need range predicates, ordering, grouping, or
projection.

```yaml
- fieldName: l_shipdate
  sourceName: /data/l_shipdate
  type: DateTime
  mappingStrategy: TimestampBSI
  configuration:
    granularity: millisecond
```

Accepted granularity values:

| Canonical value | Accepted aliases |
| --- | --- |
| `second` | `s`, `sec`, `seconds` |
| `millisecond` | `ms`, `milli`, `milliseconds` |
| `microsecond` | `us`, `micro`, `microseconds` |
| `nanosecond` | `ns`, `nano`, `nanoseconds` |

If granularity is omitted or unknown, the compatibility path defaults to
millisecond precision.

### `StringEnum`

Use for low-cardinality strings.

```yaml
- fieldName: l_shipmode
  sourceName: /data/l_shipmode
  type: String
  mappingStrategy: StringEnum
```

Optional explicit values:

```yaml
values:
- value: AIR
  rowID: 1
  desc: Air freight
- value: SHIP
  rowID: 2
  desc: Ship freight
```

For set-valued enum fields, use:

```yaml
nonExclusive: true
configuration:
  delim: ","
```

`configuration.delim` controls splitting for multi-value source strings.
The internal planner and catalog metadata call this `multiplicity=set`, but the
schema YAML field is still `nonExclusive`. Do not write `multiplicity: set` in
`schema.yaml` until the parser supports that alias.

### `StringLexBSI`

Use for high-cardinality strings that must be projected or filtered in a
bitmap-native plan.

```yaml
- fieldName: c_name
  sourceName: /data/c_name
  type: String
  mappingStrategy: StringLexBSI
  configuration:
    length: "25"
  maxLen: 25
```

The prefix length can be configured with any of these keys:

- `length`
- `prefixLength`
- `chars`
- `characters`

`length: "0"` means full-inline encoding. A positive length stores a BSI prefix
and may use a backing remainder store when `maxLen` exceeds the prefix length.

For long text that should be searchable:

```yaml
- fieldName: c_comment
  sourceName: /data/c_comment
  type: String
  mappingStrategy: StringLexBSI
  configuration:
    length: "8"
  searchable: true
  maxLen: 256
```

`searchable: true` enables secondary text-search artifacts. Search artifacts
are first-class storage artifacts and must be included in backup/restore and
manifest workflows.

### `ParentRelation`

Use for child-to-parent join edges. `foreignKey` names the parent table.

```yaml
- fieldName: l_orderkey
  sourceName: /data/l_orderkey
  type: Integer
  mappingStrategy: ParentRelation
  foreignKey: orders
  relationshipArtifacts:
    parentToChild: true
```

Internally, parent relation values are represented as integer BSI-compatible
relationship vectors. They are not string or BigValue projection fields.

### Relationship Artifacts

`relationshipArtifacts.parentToChild: true` asks QuantaStream to maintain a
reverse parent-to-child artifact for the relationship.

Use it when:

- parent-domain filters are frequently expanded into child-domain candidates;
- parent keys are reasonably low-cardinality for the workload;
- star-query or relationship-reduction plans need fast child lookup.

Do not treat it as mandatory for every foreign key. It is a physical
optimization and carries storage/load-time cost.

### `BoolRegex`

```yaml
mappingStrategy: BoolRegex
configuration:
  regex: "^(Y|yes|true)$"
```

`configuration.regex` is required.

### `Custom` And `CustomBSI`

Custom mappers use `configuration` to identify plugin-specific behavior.
`Custom` is for ranked/time-series style bitmap fields. `CustomBSI` is for
BSI-backed fields.

```yaml
mappingStrategy: CustomBSI
configuration:
  plugin: example_plugin
  name: custom_metric_mapper
```

`Delegated` can be used for target fields populated by a custom mapper rather
than directly from a source field.

## Values

Some mappers can use explicit value metadata:

```yaml
values:
- value: BUILDING
  rowID: 1
  desc: Building segment
```

| Key | Description |
| --- | --- |
| `value` | Source/logical value. |
| `rowID` | Explicit bitmap row ID for the value. |
| `desc` | Optional human-readable description. |

Explicit values are most useful for controlled enums and generated schemas.

## Views

Logical SQL views are not stored as table `schema.yaml` files. File-backed view
definitions live under:

```text
<config-dir>/views/<view-name>.yaml
```

The YAML shape is:

```yaml
schema_name: quanta
view_name: q3_order_line_base
sql: |
  create view q3_order_line_base as
  select
    c.c_mktsegment as market_segment,
    o.o_orderkey as order_key,
    o.o_orderdate as order_date,
    o.o_shippriority as ship_priority,
    l.l_extendedprice as extended_price,
    l.l_discount as discount,
    l.l_shipdate as ship_date
  from customer as c
  inner join orders as o on c.c_custkey = o.o_custkey
  inner join lineitem as l on o.o_orderkey = l.l_orderkey
canonical_sql: "<normalized SQL, when available>"
columns:
- name: market_segment
  type: string
dependencies:
- object_name: customer
  object_type: TABLE
creation_date: 2026-08-16T00:00:00Z
modification_date: 2026-08-16T00:00:00Z
```

`CREATE VIEW` and `DROP VIEW` manage these definitions and update
`CATALOG_OBJECTS` in file-backed mode. Distributed mode persists logical views
through the distributed catalog path.

## CATALOG_OBJECTS

`CATALOG_OBJECTS` records active file-backed catalog entries:

```yaml
objects:
- schema_name: quanta
  table_name: customer
  object_type: TABLE
  creation_date: 2026-08-16T00:00:00Z
  modification_date: 2026-08-16T00:00:00Z
- schema_name: quanta
  table_name: q3_order_line_base
  object_type: VIEW
  creation_date: 2026-08-16T00:00:00Z
  modification_date: 2026-08-16T00:00:00Z
```

`object_type` is `TABLE` or `VIEW`. Table names refer to physical table schema
directories. View names refer to files under `views/`.

## TPC-H Examples

### Compound Key And Time Partition

```yaml
tableName: lineitem
primaryKey: l_orderkey+l_linenumber
selector: type="lineitem"
timeQuantumType: YMD
timeQuantumField: l_shipdate
```

### Star-Query Relationship Edge

```yaml
- fieldName: l_partkey
  sourceName: /data/l_partkey
  type: Integer
  mappingStrategy: ParentRelation
  foreignKey: part
  relationshipArtifacts:
    parentToChild: true
```

### Fixed-Scale Measure

```yaml
- fieldName: l_discount
  sourceName: /data/l_discount
  type: Float
  mappingStrategy: FloatScaleBSI
  scale: 2
```

### Searchable Comment

```yaml
- fieldName: c_comment
  sourceName: /data/c_comment
  type: String
  mappingStrategy: StringLexBSI
  configuration:
    length: "8"
  searchable: true
  maxLen: 256
```

## Validation Checklist

- `tableName` matches the table directory name.
- `primaryKey` references existing `fieldName` values.
- Compound keys use `+`; multiple secondary keys use commas.
- Time quantum tables identify a `Date` or `DateTime` partition field.
- `ParentRelation` fields include `foreignKey`.
- Reverse relationship artifacts are enabled only where the workload benefits.
- `FloatScaleBSI` fields define the intended `scale`.
- `TimestampBSI` fields use an explicit granularity when precision matters.
- Low-cardinality strings use `StringEnum`.
- High-cardinality projected strings use `StringLexBSI`.
- Searchable strings set `searchable: true` and are included in
  backup/restore planning.
- Generated/internal fields are marked `system: true` only when intentionally
  produced by QuantaStream tooling.
