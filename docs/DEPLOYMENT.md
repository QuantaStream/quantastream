# QuantaStream Deployment Guide

## Purpose

This document describes QuantaStream deployment models, operational requirements,
and areas that still require validation.

Quanta-in-a-Box (QIAB) is the preferred development and conformance
environment. It is not the intended limit of production topology.

## Deployment Modes

QuantaStream's go-forward deployment vocabulary should describe topology and
physical boundaries rather than historical harness names. The two durable
concepts, also defined in [GLOSSARY.md](GLOSSARY.md), are:

- **single-node**: one QuantaStream service owns the MySQL-compatible front
  door, query engine, and a local node/storage adapter.
- **distributed**: query processors, loaders, and storage nodes are separable
  services connected through the normal service-discovery and network boundary.

Some current command-line flags, scripts, and SQLRunner suites still use older
compatibility profile names. Treat those names as transitional handles:

- `inabox-standard`: current compatibility name for the single-node product
  profile.
- `inabox-direct`: current SQLRunner compatibility name for a distributed-shape
  development harness where the query engine runs inside the test process and
  talks to a local data-node cluster.
- `inabox-local`: current compatibility name for local distributed validation,
  with a MySQL-compatible front door talking to local nodes through the normal
  service-discovery and gRPC path.
- `distributed`: the multi-host distributed architecture with independently
  deployed query processors, nodes, and loaders.

New prose should prefer **single-node** and **distributed** unless it is
documenting a specific existing flag, script, suite, or compatibility profile.

### `inabox-direct`

`inabox-direct` is primarily a development and conformance mode. Some tooling
may still use direct-mode names because the query engine is hosted by the test
process.

In this mode:

- SQLRunner or a test harness owns the query engine directly.
- The query engine talks to a local data-node cluster.
- Nodes are discovered through Consul.
- Query-engine-to-node traffic still uses gRPC and the shared clustering path.
- Distributed catalog metadata is stored in Consul.
- StringEnum dictionary data is persisted through KVStore.

This mode is valuable because it exercises distributed node behavior while
avoiding the historical qlbridge proxy path. It is not the target user-facing QIAB
experience.

Local workflow:

```bash
cd ~/projects/quantastream/startup-scripts
./start-direct.sh

cd ~/projects/quantastream/sqlrunner
go run . -engine inabox-direct -suite_file sqltests/inabox_direct_smoke.yaml -consul 127.0.0.1:8500
```

Local direct startup skips cross-node synchronization by default. Use
`./start-direct.sh --sync-startup` only when intentionally exercising the
future distributed synchronization path.

### `inabox-standard`

`inabox-standard` is the intended simple QIAB baseline for early adopters,
demos, local development, and single-process benchmark comparisons.

Required shape:

- one QuantaStream process on one host
- the MySQL-compatible front door in process
- the query engine in process
- one lightweight in-process node adapter around the storage/node package
- no Consul requirement
- no gRPC requirement between query engine and node adapter
- catalog metadata readable from local configuration files
- default MySQL-compatible endpoint on port `4000`
- default database name `quanta`
- UTC time handling for generated data and benchmark metadata

The near-term goal is to make `inabox-standard` production-credible for small
deployments, not merely useful for tests and demos. A supported profile should
define durable local storage, restart behavior, backup and restore, health
checks, log rotation, upgrade steps, and operational limits.

This profile deliberately does not validate distributed service discovery,
multi-node recovery, rebalancing, rolling upgrades, or multiple query-front-door
behavior.
Those belong to `inabox-local`, `distributed`, or future operations profiles.

Current implementation status:

- `cmd/quantastream` owns the first executable skeleton for this mode.
- `qsinabox` owns process-level mode/config/readiness planning.
- `shared` owns transport-neutral local-node service contracts and local-node
  observability vocabulary.
- `server` exposes first in-process adapters around `BitmapIndex` and `KVStore`
  unary calls.
- `qsinabox` can mount the in-process node backend, stage local schema
  configuration into the data directory, and build a local `core.SessionPool`
  over `shared.Conn.LocalNodeServices`.
- The mounted execution path can execute flat direct bitmap reads, native
  projection materialization, file-backed `CREATE TABLE`/`DROP TABLE`, and
  simple insert/commit/select flows through local `BitmapIndex` and `KVStore`
  facades without gRPC.
- Local `BitmapIndex` batch mutation, BSI batch set, sequence checkout, and
  local `KVStore` batch put now reuse the existing node write logic through
  in-process stream shims.
- The executable can print mode readiness with `-status` and can construct the
  local backend with `-mount-local-node`.
- Without `-status`, the executable mounts the local backend, composes the
  native SQL runtime, and starts the MySQL-compatible listener.
- `-wal-path` or `QUANTASTREAM_WAL_PATH` enables the first local write-ahead log
  hook for put-row intent, update-row intent, and commit boundary records. When
  enabled, the commit record is appended before the storage commit and a
  checkpoint file is advanced after the storage commit succeeds. Startup now
  validates the WAL/checkpoint pair and reports checkpointed, replayable, and
  pending WAL tail records. Standard-mode startup applies committed replay
  records before the MySQL front door is marked ready and leaves pending tail
  records unapplied. The WAL is disabled when the path is empty.

Skeleton status command:

```bash
cd ~/projects/quantastream
go run ./cmd/quantastream -status
go run ./cmd/quantastream -status -mount-local-node -config-dir configuration -data-dir /tmp/quantastream-standard
go run ./cmd/quantastream -config-dir configuration -data-dir /tmp/quantastream-standard
go run ./cmd/quantastream -config-dir configuration -data-dir /tmp/quantastream-standard -wal-path /tmp/quantastream-standard/storage.wal
```

Convenience startup command:

```bash
cd ~/projects/quantastream
./startup-scripts/start-standard.sh
```

For an already loaded TPC-H standard data directory:

```bash
cd ~/projects/quantastream
QUANTASTREAM_CONFIG_DIR=tpc-h-benchmark/config \
QUANTASTREAM_DATA_DIR=tpc-h-benchmark/local/standard-data \
  ./startup-scripts/start-standard.sh
```

SQLRunner can target that standalone process over the MySQL-compatible socket:

```bash
cd ~/projects/quantastream/sqlrunner
./run-inabox-standard-smoke.sh
./run-inabox-standard-readiness.sh
```

This path validates the product-facing QIAB server process. It is intentionally
different from `inabox-direct`, where SQLRunner embeds the query engine itself.
The default smoke helper starts a temporary `quantastream` process on port
`4400`, stages a small file-backed QA catalog with an empty `CATALOG_OBJECTS`
manifest, and validates CREATE/INSERT/COMMIT/SELECT/DROP over the MySQL socket.
Use `START_SERVER=0` to target an already running process, which defaults back
to port `4000`.

The older read-only TPCH smoke suite can still be selected with
`SUITE=inabox_standard_smoke.yaml` when TPCH data has already been staged.

TPC-H loading has its own direct path. `TPCH_LOAD_MODE=standard` does not start
the MySQL-compatible server; it hosts the lightweight local storage backend
inside the loader process and writes directly into an `inabox-standard` data
directory. After that offline load completes, start `cmd/quantastream` against
the same config and data directories to query the data. Use one worker for
standard-mode TPC-H loads until local multi-session writes are validated; cluster
direct loads can still use multiple workers.
The self-contained QA smoke uses `customers_qa` so the local path exercises
StringEnum dictionary loading, multiplicity-set inserts, default expressions,
and StringLexBSI materialization through the MySQL socket.

The readiness runner starts a fresh temporary `quantastream` process per suite,
copies the SQLRunner schema config, writes a suite-specific `CATALOG_OBJECTS`
manifest, and runs SQLRunner through the product-facing socket path. Set
`RUN_CORE=1` to run the standard smoke plus the core `basic_queries`,
`insert_tests`, and `function_expressions` suites. `RUN_PORTABLE=1` remains a
temporary alias for `RUN_CORE=1`. Set `RUN_EXTENDED=1` to include broader
group-by, join, subquery, multi-table join, and mutation suites. Use
`ALLOW_FAILURES=1` only when intentionally discovering the current gap map.

Known local-node streaming risks:

- `StringSearch.BatchIndex` and `StringSearch.Search`: required for searchable
  text fields.

These are not conceptual blockers to `inabox-standard`; they are implementation
gates where the current gRPC stream-shaped calls need direct local helpers or
local stream shims.

The first mounted backend now covers the essential local read, DDL, insert
write, StringEnum dictionary warmup, StringLexBSI batch materialization, and
default-expression path. UPDATE, DELETE, streaming ingestion, and text-search
paths should be promoted only after the remaining local batch/streaming shims
above are implemented and covered by SQLRunner or compatibility-lab tests.

### `inabox-local`

`inabox-local` is the local distributed-shape profile. As the new network layer
comes online, this is the expected result of running the local harness such as
`startup-scripts/start-local.sh`: the new MySQL-compatible query processor runs locally and
talks to local data nodes through the distributed communication path.

Required shape:

- one local host
- local query processor with the new MySQL-compatible network layer
- local data nodes managed by the development harness
- Consul/service discovery in the path
- gRPC in the query-processor-to-node path
- durable local node data directories
- optional global cache disabled by default until invalidation and consistency
  behavior are mature

This mode validates distributed boundaries without requiring multiple physical
hosts. It is the right local place to test node addressing, service discovery,
cluster startup, gRPC behavior, and distributed catalog assumptions.

Local workflow:

```bash
cd ~/projects/quantastream/startup-scripts
./start-local.sh

cd ~/projects/quantastream/sqlrunner
go run . -engine inabox-local -suite_file sqltests/basic_queries.yaml
go run . -engine inabox-local -suite_file sqltests/inabox_direct_joins.yaml
go run . -engine inabox-local -suite_file sqltests/mutate_tests_body.yaml
```

These suites validate the MySQL wire path through the native front door while still
using the local node cluster.

Local cluster startup skips cross-node synchronization by default. Use
`./start-local.sh --sync-startup` only when intentionally exercising the future
distributed synchronization path.

Schema-change propagation note:

Current Consul-backed schema operations rely on `shared/watch.go` listeners to
notice table create, drop, truncate, and modification events. The historical
admin path introduced fixed multi-second sleeps after drop/truncate so query
front doors and pooled sessions had time to observe the change before node data
was removed. That delay is a distributed watcher-barrier hack, not a durable
correctness mechanism.

`inabox-standard` should not need this delay because schema mutation and cache
invalidation happen in one process. `inabox-local` and `distributed` should
eventually replace fixed sleeps with explicit catalog-version or Consul-index
barriers, direct front-door invalidation, or stale-session rejection/reload.
Track this as V2.0 technical debt for distributed catalog consistency.

### `distributed`

`distributed` is the full multi-host deployment mode.

Expected properties:

- query processors, nodes, and bulk loaders may run on separate hosts
- Consul handles service discovery and distributed metadata coordination unless
  deliberately replaced
- gRPC is the normal query-processor-to-node communication boundary
- node-local or attached durable storage is mapped to stable node identities
- multiple query processors can be deployed where availability requires them
- global caching may be enabled when invalidation, consistency, and observability
  rules are mature
- operational monitoring, backup, recovery, and replacement procedures are
  documented and tested

Longer-term distributed work includes failure recovery, node replacement,
rebalancing, rolling upgrades, backup/restore, and geographically distributed
active/active operation.

### Containerized And Cloud Profiles

Containers and cloud-provider deployment assets are packaging and operations
profiles over the modes above, not separate engine semantics.

The repository may contain component Dockerfiles or research artifacts, but a
complete validated container orchestration topology is not currently documented.
Containers must not be treated as durable storage. Any containerized topology
must preserve node identity and mount external durable data volumes.

Enterprise-grade packaging, cloud-provider integrations, managed deployment
automation, and regional operations may live outside the public/core repository.

## Service Topology

The main runtime services are:

- Consul for discovery and cluster coordination
- QuantaStream data nodes for shards, bitmap data, and BSI data
- MySQL-compatible query front door and processor for the SQL endpoint
- ingestion processes as required by the deployment

Development defaults currently include:

- Consul HTTP API on port `8500`
- MySQL-compatible SQL endpoint on port `4000`

Additional Consul, node, metrics, profiling, and ingestion ports must be
inventoried before publishing a production firewall specification.

## Persistent Storage

QuantaStream data nodes serialize shard data beneath their configured data
directories. That storage is part of the node's durable state.

Non-negotiable requirements:

- Node data directories must be external durable storage.
- Node-local data must not reside only in an ephemeral container layer.
- Each node must mount the storage associated with its identity.
- Restarting a process must preserve its data directory.
- Replacing a container or host must use an explicit recovery or remapping
  procedure.
- Storage permissions and ownership must permit the QuantaStream process to read,
  write, and reload serialized shards.

The current local distributed harness data is stored under:

```text
test/localClusterData/quanta-node-<n>/data
```

This path is useful for local `inabox-local` testing but is not a production
storage recommendation.

## Node Identity

Node identity and durable storage must be managed together.

A node must not accidentally start with:

- another node's data directory
- an empty volume when existing data is expected
- the same writable data directory as another active node

The exact supported procedure for replacing a failed node while retaining or
changing node identity remains to be documented and validated.

## Administrative Addressing

QuantaStream's internal shard ownership uses the stable consistent-hash node name,
not the node's IP address. This is the correct durable identity for data
placement, restart, and storage mapping.

Operational tooling still needs a routable host/process reference for
debugging and targeted administration. In QIAB, `quanta-admin status` naturally
shows `127.0.0.1` for every in-process node. In a multi-host deployment, status
and administrative commands should expose enough information to distinguish:

- stable QuantaStream node identity
- advertised host or IP address
- service port
- data center or placement metadata
- storage mapping

The current `quanta-admin` shutdown support can target all nodes or an
individual node by IP. Multi-host deployment validation should revisit this
interface so targeted shutdown and debugging can use unambiguous node identity
and network address information.

## Consul Requirements

`inabox-local` currently uses a local Consul development agent:

```bash
consul agent -dev
```

Consul development mode is not appropriate for production. `inabox-standard`
should not require Consul.

A production deployment requires:

- a durable, highly available Consul deployment
- appropriate ACL and transport-security configuration
- documented service-registration and health-check behavior
- backup and recovery procedures for Consul state
- startup sequencing that prevents nodes and proxies from assuming an
  incomplete cluster is ready

## Startup and Shutdown

The `inabox-local` and distributed startup order is:

1. Start Consul.
2. Start QuantaStream data nodes.
3. Wait for the nodes to become active and the cluster to become healthy.
4. Start or expose the MySQL-compatible query front door.
5. Start ingestion and client workloads.

Shutdown should stop writes and ingestion before stopping nodes. `inabox-local`
performs node and query-front-door shutdown through its local harness, but production
shutdown and drain semantics require further validation. `inabox-standard`
should provide a simpler single-process lifecycle.

## Restart, Replacement, and Recovery

These are distinct operations:

- **Restart:** the same node identity and durable data directory return.
- **Container replacement:** a new container receives the prior node's identity
  and durable volume.
- **Host replacement:** storage and identity must be deliberately restored or
  reassigned.
- **Node replacement:** a failed node is removed and its shard responsibility
  is recovered or redistributed.

Verified in the local distributed harness:

- a three-node cluster can restart from serialized shard data
- shard counts are restored
- representative SQL results match before and after restart

Not yet formally validated:

- unclean process or host failure
- partial writes during failure
- single-node replacement
- volume restoration on a different host
- rolling restart
- rolling upgrade
- Consul outage and recovery

## Scaling

`inabox-standard` is a single-process, single-node-adapter profile.
`inabox-local` uses a local multi-node development cluster. Production scaling
behavior must be tested independently.

Validation is needed for:

- adding a node
- removing a node
- shard redistribution
- query-front-door scaling
- ingestion behavior during topology changes
- query correctness while the cluster converges

## Backup and Restore

QuantaStream includes an initial local filesystem backup format for offline or
externally quiesced `inabox-standard` data directories. The format copies the
data directory into a provider-neutral snapshot tree and writes `manifest.json`
last. The manifest records the backup format/version, mode, file and directory
counts, byte count, per-file SHA-256 checksums, and the current checkpoint shape.

Create and validate a local backup:

```bash
go run ./quanta-admin backup create \
  --data-dir /path/to/quantastream-data \
  --target file:///path/to/backup

go run ./quanta-admin backup validate \
  --source file:///path/to/backup
```

Restore into an empty data directory:

```bash
go run ./quanta-admin backup restore \
  --source file:///path/to/backup \
  --data-dir /path/to/restored-data
```

This first slice is intentionally an offline/local snapshot contract. A local WAL
primitive and `inabox-standard` enablement switch exist, including checkpoint
metadata for successful commit boundaries and standard-mode startup replay, but
live quiescent backup mode, distributed cluster snapshots, and cloud backup
targets remain separate lifecycle work.

Before production use, the project must establish:

- whether node storage can be snapshotted while nodes are active
- whether writes must be quiesced
- consistency requirements across node volumes and Consul
- restoration ordering
- restoration to original or replacement node identities
- post-restore validation

A backup is not considered valid until a restore test has succeeded.

## Observability

Production deployment should capture:

- structured logs from each data node
- query-front-door logs
- Consul health and membership
- cluster state and active-node count
- shard distribution
- memory consumption
- query errors and latency
- persistence and reload errors
- ingestion lag and failures

`inabox-standard` combines engine and node-adapter logs in one process.
`inabox-local` may combine local harness logs, but component identity should
still be visible. Separate-process deployments should preserve component
identity in every log stream.

## Security

Production security work must include:

- Consul ACLs and transport security
- network exposure and firewall rules
- query-front-door authentication and authorization
- secret distribution and rotation
- least-privilege filesystem permissions
- TLS requirements for client and service communication
- container and host process isolation

The local development defaults are not a production security profile.

### Authentication and Authorization

QuantaStream 1.0 will include a simple MySQL-compatible account and
password authentication model in the public/core repository. The
`quanta-admin` tool will provide account and password administration for
this built-in core auth path.

Enterprise identity integration is a separate adapter surface. OAuth, OIDC,
JWT, SSO, cloud-provider identity systems, and other enterprise-grade
integrations should plug into the same auth/session contract without becoming
hard-coded behavior in the core MySQL protocol path.

The historical Quanta authentication design used OpenID Connect JWT tokens to
gate access to the old query service. In that model, an external identity
provider such as AWS Cognito manages user accounts, issues valid tokens, and
owns user policy and governance.

JWT tokens were expected to be used in two ways:

1. Database-driver connections such as JDBC or Python connectors submit the
   access token as the `userName` value and use an empty `password`.
2. MySQL-compatible tools use a separate token-exchange web endpoint, typically port
   `4001`, to redeem an access token for a temporary MySQL-compatible username
   and password. Those credentials expire according to the access-token TTL.

For AWS Cognito-based deployments, tools such as
[`go-cognito-authy`](https://github.com/RafPe/go-cognito-authy) can help obtain
tokens and manage passwords. Production deployments should follow the chosen
identity provider's documentation rather than treating Cognito as the only
supported provider.

Example token exchange request:

```bash
curl -v -H "Content-Type: application/text" -d "<jwt-access-token>" http://<query-host>:4001/
```

Before this capability can be considered supported, QuantaStream needs validated
configuration, TLS guidance, token validation behavior, authorization/RBAC
semantics, audit logging, secret rotation, and operational tests.

The quarantined authentication and RBAC code should be treated as historical
implementation context rather than the target design. QuantaStream's
MySQL-compatible network protocol is intended to work across a broad range of
standard client drivers and tools, including currently used Node.js, Python,
Java/JDBC, and native Go clients. Authentication work therefore needs to
preserve driver compatibility at the protocol boundary, not only make the
command-line MySQL client happy. The built-in replacement should start from
broad MySQL client compatibility:

- implement the MySQL authentication handshake cleanly enough for standard
  drivers and tools to connect without special-case behavior
- preserve prepared SQL support and add batched insert behavior expected by
  standard drivers
- make `USE <database>`, selected database state, and `database()` semantics
  part of the same session model
- separate authentication, authorization, and session/database selection
  responsibilities
- define stable interfaces for local account/password validation, optional
  identity providers, credential validation, and role lookup
- manage built-in account/password state through `quanta-admin`
- keep enterprise identity integration pluggable, with JWT/OIDC/OAuth-style
  providers as future adapters rather than hard-coded query-front-door behavior
- cache small authorization metadata in memory after loading it from the
  authoritative metadata store

The historical RBAC implementation has been quarantined under `_quarantine/rbac`
for short-term reference and is no longer part of the active core build. Role
concepts may inform the replacement, but the role store, handshake, token
exchange, and MySQL session semantics should be reworked together in the
qsbridge-based MySQL-compatible auth path. Avoid papering over historical
behavior with more hard-coded usernames, passwords, or `database()` responses.

## Deployment Validation

The SQL roadmap and integration suites should be environment-neutral and run
through the MySQL-compatible surface against:

- `inabox-standard`
- `inabox-local`
- containerized distributed profiles
- multi-host `distributed` profiles

The same deterministic schemas, data, and expected results should be used
wherever possible. Deployment-specific suites should additionally validate
restart, replacement, persistence, and topology changes.

## 1.0 Deployment Scope

For 1.0, QuantaStream should be explicit about supported and aspirational
deployment models.

Supported 1.0 target:

- `inabox-standard` as the simple QIAB profile.
- `inabox-local` as the local distributed-shape validation profile.
- Durable node data directories outside ephemeral process or container
  storage.
- Documented clean shutdown, restart, backup, restore, and upgrade workflows.
- Observable health and logs sufficient to diagnose real application issues.
- Clear limits around host failure, node replacement, scaling, and regional
  availability.
- Simple MySQL-compatible account/password authentication is part of the core
  1.0 path, with `quanta-admin` account and password administration.
- The default public auth adapter must live in the public/core repository and
  implement the built-in account/password path behind the MySQL auth/session
  contract.

Post-1.0 targets:

- validated multi-host orchestration
- automated node replacement and recovery
- rolling restart and rolling upgrade
- online scaling and rebalance workflows
- multiple query-front-door deployment patterns
- production packaging around the `quantastream-node` and
  `quantastream-proxy` service entry points beyond the current systemd script
  shape
- cross-region active/active or equivalent regional availability strategy

## Repository Boundary

This repository should remain focused on the open-source engine, QIAB,
conformance workflows, and validated core deployment assets. `inabox-standard`,
`inabox-local`, and distributed deployment should be documented as separate
operational models, not mixed together inside opaque Makefile or script
behavior.

Enterprise-grade packaging, orchestration, identity-provider integrations,
managed deployment automation, and regional operations may eventually live in a
separate repository. Until that split exists, enterprise-oriented artifacts in
this repository should be clearly labeled as aspirational, experimental, or
development-only.

Docker and container orchestration artifacts should be treated as deployment
research and future-state inputs unless a specific topology has validation
coverage. `inabox-standard` does not depend on Docker. Container and
multi-node orchestration may later move to enterprise or operations-focused
repositories once the supported OSS deployment boundary is clear.

Legacy build targets, Docker files, local scripts, and helper utilities should
be inventoried before cleanup. They are not sacred, but they should not be
deleted until their current build, test, development, or deployment role is
understood.

## Current Limitations

- `inabox-standard` shares one process and one host lifecycle.
- `inabox-local` still shares one host but exercises the distributed
  query-front-door-to-node communication path.
- Distributed service wrappers exist for `quantastream-node` and
  `quantastream-proxy`, but multi-host lifecycle validation is still young.
- Current Docker artifacts do not constitute a complete durable deployment.
- Production storage mapping and node replacement are not formally specified.
- Backup and restore are not formally validated.
- Rolling restart and upgrade behavior are not formally validated.
- Production Consul configuration is not documented.
- A complete port and firewall inventory is not documented.

## Operational Validation Backlog

1. Define container volume and node-identity conventions.
2. Build a reproducible multi-container deployment with durable volumes.
3. Verify clean restart and container replacement.
4. Test unclean node termination and recovery.
5. Test adding and removing nodes.
6. Define and test backup and restore.
7. Validate rolling restart and upgrade behavior.
8. Publish the production port, health-check, and security inventory.

The existing deployment architecture image is available at
[`Quanta_Deployment_Architecture.png`](Quanta_Deployment_Architecture.png).
