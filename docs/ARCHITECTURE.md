# ARCHITECTURE.md

# Quanta Architecture Overview

## Overview

Quanta is a distributed bitmap-oriented analytical processing engine optimized for:

- high-speed filtering
- distributed joins
- analytical aggregation
- streaming ingestion

---

# Major Components

## Data Nodes

Quanta data nodes:

- own shards
- store bitmap and BSI data
- store backing string data (where applicable)
- execute bitmap operations
- participate in distributed query execution

Data nodes are shard-aware and coordinated through Consul.

---

## Query Front Door And Processor

The MySQL-compatible query front door and query processor:

- accepts SQL requests
- performs SQL parsing/planning
- generates logical query plans
- coordinates distributed query execution

The current product-facing network surface exposes a MySQL-compatible interface.

### MySQL Wire Adapter Boundary

`qsmysql` owns the MySQL wire-protocol byte model: packet framing, initial
handshake payloads, and command packet decoding. It deliberately does not own
SQL planning, bitmap execution, catalog access, authentication policy, or
result-set production.

`qsruntime` mounts this adapter readiness into the native MySQL front door so
the protocol surface can mature independently from the SQL engine. This keeps
`qsbridge` protocol-neutral while still giving QuantaStream a MySQL-compatible
network path.

---

## Logical Query Layer

SQL queries are translated into an intermediate logical query representation before execution.

This layer separates:

- SQL syntax/planning
from:
- bitmap execution semantics

### Planning And Per-Table Execution Boundary

Quanta nodes should remain deliberately simple. A node-facing execution request
is expected to describe work for one table at a time: bitmap predicates, BSI
predicates, dictionary predicates, projection reads, and local aggregate work
that can be evaluated against that table's shards.

The broader SQL meaning of a query belongs above that boundary. The planner and
query processor must understand the full query graph before issuing node work:

- table instances and aliases
- join relationships and join type
- per-table pushdown fragments
- residual predicates
- hidden projection fields required for joins, grouping, ordering, and output
- driver legality and result assembly strategy

This separation is intentional. The Quanta intermediate language should stay
close to node-executable, per-table physical intent, while the planner owns
relationship-aware query assembly. The initial engine often discovered too much
join behavior late in execution; the native planning work should make those
fragments explicit before the nodes are called.

---

## Bitmap Execution Engine

The bitmap execution engine is the core architectural focus of Quanta.

Current capabilities include:

- bitmap filtering
- BSI processing
- joins
- aggregate operations
- top-N analytics
- distributed shard execution

### Native Kernels And Materialization Boundaries

The new query engine should prefer bitmap and BSI kernels over materialized row
loops whenever the predicate can be expressed against encoded columnar state.
Same-row field comparisons are the clearest example. A predicate such as:

```sql
l_receiptdate > l_commitdate
```

should eventually execute as:

```text
receiptdate BSI + commitdate BSI -> comparison bitmap -> count/filter
```

rather than:

```text
hydrate receiptdate values + hydrate commitdate values -> row loop compare
```

The historical `core.Projector.Next` path is useful as a compatibility
reference, but it is intentionally not the desired center of the native engine.
It hides
backend retrievals, transport/serialization costs, and row hydration behavior
behind a broad materialization API. Native planner and executor work should
make these boundaries visible:

- bitmap/BSI comparison kernels
- relationship-vector projection kernels
- late materialization points
- transport adapter choice
- residual predicates that truly require hydrated values

This distinction matters for deployment shape. In distributed mode,
serialization across the node boundary is an acceptable cost when it preserves
node isolation and scale-out. In QuantaStream-in-a-Box, the same logical node
operation should be able to use an in-process adapter that avoids network
serialization while preserving the planner/executor contract.

### Join Driver Legality

Current join projection is driven from the structural child/FK-side table. The
projector starts from the selected driver table and can translate row IDs
toward parent tables by following `ParentRelation` BSI links. This makes
child-to-parent projection and grouping practical for joins such as
`orders -> customer` and `lineitem -> orders -> customer`.

The reverse direction, where a parent table such as `orders` drives expansion
into child rows such as `lineitem`, is not a general materialization path yet.
Even if a parent-side predicate produces a smaller candidate set, that table is
not automatically a legal final driver for the current projector. A future
optimizer must therefore separate driver legality from driver cost:

- first identify which candidate drivers can produce correct rows for the
  requested projection, grouping, and aggregate shape
- then rank only legal candidates using bitmap-derived cost signals such as
  reduced cardinality, shard participation, and value distribution
- introduce a separate parent-to-child expansion path before costing parent-side
  drivers for child-side materialization

This is especially visible in TPC-H Q3/Q5, where `orders` may be smaller after
date filtering, but `lineitem` remains the legal driver for current join
materialization.

---

## Consul Integration

Consul is used for:

- node discovery
- cluster coordination
- shard metadata
- service registration

---

## Ingestion

Quanta supports:

- streaming ingestion
- SQL INSERT
- bulk loading
- Parquet export/import workflows

Current ingestion work is focused on:

- simplified local workflows
- TPC-H loading
- streaming demonstrations

### Batch And Stream Ingestion Model

Quanta is intended to support both complete batch-style data sets and
near-real-time streams through the same storage and query model.

For workloads such as TPC-H, the data set may be presented in whole and loaded
as a bounded batch. For streaming workloads, data arrives continuously. In both
cases, Quanta stores incoming records in database-managed bitmap and BSI
structures that represent the current known state of the data set.

The system intentionally does not model streaming ingestion as independent time
windows that lose knowledge of the complete current data set. Time can be part
of the schema and query model, but the storage engine is still maintaining a
database view of accumulated state.

### Selector-Based Stream Routing

Schemas may define selector expressions that identify which table shape an
incoming record belongs to. This is a durable ingestion concept and should not
depend on the historical `qlbridge` expression VM.

The future selector engine should provide a small, deterministic expression
surface for decoded payloads:

- evaluate schema selectors against incoming records
- choose the target table and ingestion session
- preserve parent/child context for nested payloads
- route heterogeneous streams without embedding SQL planner dependencies in the
  ingestion path

Selector evaluation can share expression primitives with the SQL engine where
that is useful, but it should remain an ingestion-facing contract. Its purpose
is dispatch and shape recognition, not query planning.

### Batch Data As Stream Replay

Batch data sets can also be replayed through the streaming ingestion contract.
This gives Quanta a deterministic way to test streaming ingestion without
depending on live external feeds.

TPC-H is a useful candidate for this mode because the same source data can
validate both batch loading and stream replay. Replay modes can include:

- table-by-table replay
- parent/child relationship replay
- time-ordered replay by configured time fields
- rate-limited replay for throughput testing
- deterministic seeded replay for CI and development

The expected end state is that batch loaders and streaming adapters converge on
the same schema selection, session routing, dictionary update, and mutation
writer contracts.

### Streaming Loader Endpoint

`cmd/quantastream-loader` is the first standalone streaming loader process. It
listens for JSON events over HTTP, normalizes them into `IngestEnvelope`
records, evaluates table selectors, and routes accepted records through
`SessionRouter` to the native engine mutation lane.

The initial protocol surface is:

```text
POST /ingest/json
GET  /healthz
```

JSON is the first adapter, not the loader architecture. Additional protocol
adapters should plug into the same normalized envelope boundary.

For `inabox-standard`, the loader connects to the native gRPC endpoint exposed
by a separately running `cmd/quantastream` process:

```bash
go run ./cmd/quantastream-loader \
  -config-dir tpc-h-benchmark/config \
  -native-grpc-addr 127.0.0.1:4100 \
  -listen 127.0.0.1:8088 \
  -tables orders,lineitem
```

`tpc-h-benchmark/tpch-stream-producer` is the first driver for this endpoint. It
generates TPC-H-shaped `orders` and `lineitem` JSON events and posts them to the
loader. Against the full TPC-H schema, dimension and parent tables such as
`customer`, `part`, and `supplier` must already be loaded because normal FK
rules still apply. See `../configuration/STREAMING_LOADER.md` for user-facing
setup and payload details.

### Streaming Loader Routing

Streaming consumers use schema selectors to determine which Quanta table a
record belongs to. A selector lets one incoming stream contain multiple record
shapes or logical table types while still routing each record to the correct
table schema before mutation.

After table selection, the loader uses its configured shard key and rendezvous
hashing to route the record to an internal shard channel. This routing is
independent of upstream stream partition ownership. The purpose is to fan
records out across Quanta session workers while preserving affinity for like
data.

That affinity is important because records with the same loader shard key should
flow through the same session and connection objects. This keeps ingestion
ordering and any session-local state predictable, and avoids sharing a single
session across concurrent workers.

### Session Concurrency Contract

Quanta session objects are intentionally not thread-safe. A session should be
owned by one worker/goroutine at a time, or protected externally by an owning
queue/channel. Components that need ingestion parallelism should create or
reuse multiple sessions and route records to them deterministically rather than
calling one session concurrently from multiple goroutines.

---

# Query Flow

```text
SQL
  ↓
SQL parser/planner
  ↓
Logical query representation
  ↓
Distributed bitmap execution
  ↓
Shard/node aggregation
  ↓
Result assembly
```

---

# QuantaStream-in-a-Box

QuantaStream-in-a-Box (QIAB) provides:

- local 3-node cluster startup
- local MySQL-compatible front-door startup
- integration test environment
- reproducible development workflows

The current implementation uses:

- startup-scripts/start-local.sh
- Ensure_Cluster
- in-process node startup

QIAB is the preferred development and conformance environment. It is not the
intended limit of production topology. Containerized and multi-host deployment
requirements are documented in [`DEPLOYMENT.md`](DEPLOYMENT.md).

---

# Current Architectural Priorities

- startup simplification
- query correctness stabilization
- TPC-H analytical validation
- streaming demo workflows
- SQL support expansion
- ingestion cleanup
