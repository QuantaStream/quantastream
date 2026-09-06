# QuantaStream Architecture Overview

QuantaStream is a bitmap-native analytical database with a MySQL-compatible
SQL front door. It filters compressed sets, executes BSI and relationship-vector
operations, and materializes values when needed for expressions and output.

This overview describes the implementation reviewed on 2026-09-06 against
commit `ebfff0f`. The current product baseline is **single-node**. Distributed
node services and development harnesses also exist, but their presence does not
imply validated multi-node recovery, rebalancing, or rolling upgrades.

## Process And Deployment Boundaries

`cmd/quantastream` starts one service containing the SQL front door, native
query runtime, catalog, and local storage adapter. Its current mode flag is
`inabox-standard`, the compatibility name for the single-node profile.
The default MySQL address is `127.0.0.1:4000`, with database `quanta`.

```text
MySQL clients
      |
      v
quantastream process
  qsmysql wire formats + qsruntime listener/session handling
      |
  qsbridge parsing, planning, and execution contracts
      |
  qsruntime execution and materialization kernels
      |
  shared local-node contracts -> server BitmapIndex / KVStore
      |
  local catalog and persisted bitmap, BSI, dictionary, and backing values

quantastream-loader process
  HTTP JSON -> selectors -> worker-owned sessions
      |
      +-- optional native gRPC endpoint -> quantastream storage services
```

The local query-to-storage path uses in-process adapters, without Consul or
gRPC serialization. A separate loader can use the native node gRPC endpoint,
enabled explicitly with `-native-grpc-port` (for example, `4100`); it is disabled
by default. Enabling that endpoint does not create a multi-node cluster.

The distributed shape separates query processors, loaders, and data nodes.
Nodes own shards and expose bitmap and KV services over gRPC. Consul supplies
discovery, service registration, cluster coordination, and catalog metadata.
`cmd/quantastream-proxy` provides the separate native query front door.

| Profile | Query engine and storage boundary |
| --- | --- |
| `inabox-standard` | One service with an in-process local node; no Consul requirement. |
| `inabox-direct` | SQLRunner hosts the engine and accesses a local data-node cluster through Consul and gRPC. |
| `inabox-local` | A separate MySQL front door accesses local nodes through discovery and gRPC. |

QuantaStream-in-a-Box is not synonymous with the historical local three-node
harness. See [Deployment](DEPLOYMENT.md) and
[Deployment Diagrams](DEPLOYMENT_DIAGRAMS.md) for topology and operational limits.

## Package Responsibilities

| Package | Responsibility |
| --- | --- |
| [`qsbridge`](../qsbridge) | Native parser, planner, catalog/session vocabulary, execution handoff, and engine contracts. Independent of runtime and legacy storage packages. |
| [`qsexpr`](../qsexpr) | Native expression primitives used by engine and ingestion paths. |
| [`qsruntime`](../qsruntime) | SQL execution composition, MySQL listener/session behavior, bitmap and relationship execution, result assembly, and narrow storage/session adapters. |
| [`qsmysql`](../qsmysql) | MySQL packet framing, handshake, command, and result-set wire formats. SQL planning and authentication policy belong above this byte boundary. |
| [`qsinabox`](../qsinabox) | Single-node configuration, local backend mounting, runtime composition, startup recovery, and optional native node endpoint. |
| [`server`](../server), [`shared`](../shared), [`grpc`](../grpc) | BitmapIndex and KVStore implementations, local/remote service contracts, clustering support, and network service definitions. |
| [`core`](../core) | Storage-facing schema, session, mutation, and ingestion machinery reused by native adapters. |
| [`qsloader`](../qsloader) | HTTP ingestion adapter, selector routing, session-worker orchestration, and ingestion statistics. |
| [`qstream-admin-lib`](../qstream-admin-lib) | Administrative implementation, including backup, restore, and WAL inspection. |
| [`sqlrunner`](../sqlrunner) | Executable SQL correctness and compatibility suites. |

The single-node service uses native SQL execution and materialization kernels.
Legacy storage/session dependencies remain behind explicit adapters while
engine contracts are separated from runtime composition. Architecture tests in
[`qsbridge`](../qsbridge/architecture_test.go) and
[`qsruntime`](../qsruntime/architecture_test.go), together with the
[projector quarantine tests](../qsruntime/legacy_projector_quarantine_test.go),
enforce these boundaries.

## Planning And Query Execution

The planner owns the SQL query graph: table instances and aliases, join
relationships and types, per-table predicates, residual expressions, hidden
fields needed for grouping and ordering, and result assembly requirements.
Node-facing work stays close to physical operations on a table's shards:
bitmap and BSI predicates, dictionary lookups, projection reads, and local
aggregate work.

The execution flow is:

1. Decode the MySQL command and apply session, authentication, and access-policy
   handling in the front door.
2. Parse SQL, resolve catalog objects, and plan supported query shapes through
   `qsbridge`.
3. Route execution through `qsruntime`, applying per-table bitmap/BSI filters
   and relationship operations through the selected backend.
4. Compute aggregates and evaluate residual expressions, reading scalar or
   string values where the query requires them.
5. Assemble, order, and limit results as required by the plan, then encode the
   MySQL response.

Native kernels include bitmap filtering, same-row BSI comparisons,
relationship-vector traversal, grouping, aggregation, and late materialization.
For supported encoded fields, a predicate such as
`l_receiptdate > l_commitdate` can produce a comparison bitmap directly from
the two BSIs. This is implemented in the single-node
[same-row comparison adapter](../qsinabox/standard_same_row_comparison.go),
rather than being solely a future optimization.

Materialization remains explicit: some expressions need hydrated values, and
some supported SQL shapes use intermediate rowsets. Request-scoped
[`QueryScratchpad`](../qsruntime/query_scratchpad.go) memoization supports reuse
within execution. Execution inspection and probes expose kernel and fallback
choices; SQL support does not mean every shape runs entirely as bitmap algebra.

### Relationship Joins

Relationship fields encode child-to-parent links in BSIs. The native
[relationship execution contracts](../qsbridge/relationship_execution.go)
describe reduce, expand, semi, anti, and null-extension operations, with
runtime adapters handling vector reads and materialization. Parent-to-child
expansion is part of this vocabulary; the old projector-only description is
no longer an adequate account of join execution.

Driver legality still depends on the requested query shape and executable
kernel. A smaller filtered table is not automatically a legal or faster
materialization driver. Projection, grouping, multiplicity, and outer-join
preservation must remain correct before cost can select among alternatives.
[Reverse relationship artifacts](../qsruntime/relationship_vector_reverse_artifact.go)
also have explicitly opt-in experimental modes; their existence is not a
promise of unrestricted join reordering.

The authoritative SQL contract is [Supported SQL](SUPPORTED_SQL.md), its
SQLRunner suites, and [SQL Boundaries](UNSUPPORTED_SQL.md).

## Storage And Catalog

Schemas describe physical representation as well as logical type:

- Standard bitmaps represent membership and low-cardinality values.
- BSIs represent numeric and timestamp values and relationship vectors.
- StringEnum dictionaries encode categorical strings.
- Backing value storage supplies high-cardinality strings and other values
  needed during materialization.
- Scalar/set multiplicity and time-quantum configuration shape storage and
  query behavior.

`server.BitmapIndex` and `server.KVStore` provide the node storage services.
Single-node mounting stages schema configuration into the data directory and
builds local sessions over `shared.Conn.LocalNodeServices`. Distributed
adapters use the corresponding discovery and transport boundary. Catalog and
dictionary changes require metadata invalidation so subsequent planning and
materialization observe updated definitions and encodings.

See [Schema Design](SCHEMA_DESIGN.md) and the
[Schema Configuration Reference](../configuration/SCHEMA_CONFIG_REFERENCE.md).

## Ingestion

SQL writes, bulk loaders, and streaming adapters feed database-managed bitmap,
BSI, dictionary, and backing-value state. Streaming accumulates the database's
known state; it is not intrinsically a collection of independent time windows.
Schema time fields can still determine physical partitioning and query ranges.

`cmd/quantastream-loader` is a separate process. Its HTTP JSON adapter produces
normalized `IngestEnvelope` records, evaluates schema selectors, and routes
accepted records through `core.SessionRouter` to engine sessions. Native
[selector evaluation](../core/ingest_selector.go) uses `qsexpr` and does not
require the historical qlbridge expression VM.

Router workers own their sessions. Sessions are not safe for concurrent use:
parallel ingestion uses multiple sessions and rendezvous hashing of loader
shard keys to preserve affinity. Loader routing is distinct from upstream
stream partition ownership and from distributed storage-node placement.
Optional physical build routing adds time-quantum-aware routing for eligible
source shapes.

| Endpoint | Role |
| --- | --- |
| `POST /ingest/json` | Accept and enqueue JSON records. Acceptance is not a durable commit. |
| `POST /flush` | Flush through worker queues after records already ahead of the flush markers. |
| `POST /commit` | Flush, then request a backend storage savepoint. |
| `GET /healthz` | Loader readiness. |
| `GET /stats` | Queue, session, processing, flush, commit, and runtime counters. |

The loader supports `standard-native` connections to the optional single-node
gRPC endpoint and `distributed` connections through Consul. Orderly shutdown
commits by default. Batch replay can use the same ingestion interface;
`tpc-h-benchmark/tpch-stream-producer` supplies a development workload, with
normal foreign-key prerequisites still applying.

See [Streaming Loader Configuration](../configuration/STREAMING_LOADER.md) for
payloads, connection settings, routing, and flush/commit semantics.

## Durability And Operations

Single-node data is persisted locally. The optional write-ahead log is enabled
with `-wal-path` or `QUANTASTREAM_WAL_PATH`; an empty path disables it. Its
mutation-intent and commit records, checkpoint handling, and startup replay
form a separate recovery layer over storage persistence.

[`MountStandardProcess`](../qsinabox/standard_process.go) enables WAL recovery
before composing the ready front door. Committed replay records are applied;
pending uncommitted tail records are not. Recovery handles a final torn frame,
while complete corrupt frames fail validation. These mechanisms do not provide
full MySQL rollback or MVCC transaction semantics.

Administrative tooling includes WAL inspection, backup/restore, and support
bundles. Quiesced backup uses a local write barrier while taking a filesystem
snapshot. These are single-node operational mechanisms, not distributed
consensus or automatic failover.

The front door requires an explicit authentication mode: permissive evaluation
or configured static accounts. Optional access-policy files control SQL access.
See [Authentication And Access](../configuration/AUTH_ACCESS.md) and
[Deployment](DEPLOYMENT.md) for operational details and remaining limits.

## Validation And Further Reading

Architecture and unit tests guard package boundaries and kernel behavior.
SQLRunner supplies executable correctness and client-compatibility coverage;
TPC-H suites validate analytical query shapes and benchmark methodology.
Repository CI runs `go test ./...` and the single-node readiness suites.

- [Getting Started](GETTING_STARTED.md): binary evaluation workflow.
- [Quickstart](QUICKSTART.md): source-checkout workflow.
- [MySQL Compatibility](MYSQL-COMPATIBILITY.md): client/protocol coverage.
- [TPC-H](TPCH.md): analytical validation.
- [Glossary](GLOSSARY.md): deployment and engine terminology.

Forward work is tracked in [GitHub Issues](https://github.com/QuantaStream/quantastream/issues).
This overview describes implemented boundaries and their limits rather than a
release checklist or a duplicate roadmap.
