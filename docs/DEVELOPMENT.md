# QuantaStream Development Guide

This guide is for people working from a source checkout. If you only want to
try QuantaStream, start with [`GETTING_STARTED.md`](GETTING_STARTED.md) and the
published binary bundle instead.

## Requirements

Recommended development environment:

- Linux or WSL2
- Go version from [`go.mod`](../go.mod)
- MySQL command-line client for manual endpoint checks
- MySQL server only when running reference compatibility diffs
- Consul only for direct-cluster or distributed-mode development

The single-node source workflow does not require Consul. Distributed mode and
`inabox-direct` do.

## Repository Map

Common entry points:

- `cmd/quantastream` - single-node engine binary
- `cmd/quantastream-loader` - JSON streaming loader
- `cmd/quantastream-proxy` - distributed MySQL-compatible proxy
- `quanta-admin` - source-tree admin command
- `quanta-admin-lib` - admin implementation used by source and release tools
- `qsbridge` - SQL parsing, planning, metadata, and client exchange layer
- `qsmysql` - MySQL wire protocol adapter
- `qsinabox` - single-node in-a-box runtime wiring
- `qsruntime` - native query execution and rewrite layer
- `shared`, `server`, `core` - bitmap storage, query, and session machinery
- `sqlrunner` - executable SQL behavior and benchmark harness
- `configuration/` - public schema, auth, and loader configuration references
- `startup-scripts/` - source checkout startup helpers

Release bundles rename the admin command to `qstream-admin`. In the source tree
use `go run ./quanta-admin ...`.

## Baseline Checks

The CI baseline is:

```bash
go test ./... -count=1 -timeout=2m
```

Run that before pushing engine or SQL behavior changes. There is no Makefile in
the public repo; use the Go and shell commands directly.

Useful focused checks:

```bash
go test ./qsbridge ./qsmysql ./qsruntime -count=1
go test ./qsinabox ./server ./shared -count=1
go test ./cmd/quantastream ./cmd/quantastream-loader ./quanta-admin-lib -count=1
```

Current raw Go statement coverage can be measured with:

```bash
go test ./... -count=1 -timeout=2m -coverprofile=/tmp/quantastream-cover.out
go tool cover -func=/tmp/quantastream-cover.out | tail -1
```

Remember that Go statement coverage does not include every SQLRunner,
Workbench, AWS, or release-runbook validation path unless those paths run
inside an instrumented `go test` process.

## Build From Source

Build the main binaries explicitly:

```bash
mkdir -p ./bin
go build -o ./bin/quantastream ./cmd/quantastream
go build -o ./bin/qstream-loader ./cmd/quantastream-loader
go build -o ./bin/quantastream-proxy ./cmd/quantastream-proxy
go build -o ./bin/qstream-admin ./quanta-admin
```

For quick iteration it is also fine to run commands with `go run`.

## Local Single-Node Development

The easiest source checkout runtime is the standard single-node process:

```bash
./startup-scripts/start-standard.sh
```

By default it starts the MySQL-compatible endpoint on `127.0.0.1:4000` and the
native gRPC endpoint on `127.0.0.1:4100`. Use environment variables to override
paths and ports when needed:

```bash
QUANTASTREAM_CONFIG_DIR=tpc-h-benchmark/config \
QUANTASTREAM_DATA_DIR=tpc-h-benchmark/local/standard-data-dev \
QUANTASTREAM_BIND=127.0.0.1 \
QUANTASTREAM_MYSQL_PORT=4000 \
QUANTASTREAM_NATIVE_GRPC_BIND=127.0.0.1 \
QUANTASTREAM_NATIVE_GRPC_PORT=4100 \
QUANTASTREAM_DATABASE=quanta \
./startup-scripts/start-standard.sh
```

Verify from another terminal:

```bash
mysql -h 127.0.0.1 -P 4000 -u qstream -D quanta -e 'show tables;'
go run ./quanta-admin doctor local \
  --data-dir tpc-h-benchmark/local/standard-data-dev \
  --config-dir tpc-h-benchmark/config \
  --mysql-addr 127.0.0.1:4000 \
  --native-grpc-addr 127.0.0.1:4100
```

For binary-release users, the equivalent runbook is
[`GETTING_STARTED.md`](GETTING_STARTED.md).

## Direct Cluster And Distributed Development

Use these paths when work touches Consul discovery, distributed proxy behavior,
node membership, routing, reverse artifacts, or cluster lifecycle:

- [`DEPLOYMENT.md`](DEPLOYMENT.md)
- [`DEPLOYMENT_DIAGRAMS.md`](DEPLOYMENT_DIAGRAMS.md)
- [`startup-scripts/README.md`](../startup-scripts/README.md)
- [`startup-scripts/aws/README.md`](../startup-scripts/aws/README.md)

Operational AWS scripts are intentionally kept in the repo because they are the
repeatable benchmark and deployment lane. Do not require distributed mode for
ordinary single-node SQL or client compatibility work.

## SQLRunner

SQLRunner is the executable SQL behavior map. Reusable SQL semantics belong in
YAML suites under `sqlrunner/sqltests`; do not add old line-oriented SQL script
fixtures.

Run a simple single-node suite through the MySQL-compatible endpoint:

```bash
cd sqlrunner
go run . \
  -engine proxy \
  -host 127.0.0.1 \
  -port 4000 \
  -user qstream \
  -db quanta \
  -suite_file sqltests/inabox_standard_smoke.yaml \
  -precise_timing
```

Run a direct-cluster suite when Consul and local cluster nodes are up:

```bash
cd sqlrunner
go run . \
  -engine inabox-direct \
  -consul 127.0.0.1:8500 \
  -suite_file sqltests/inabox_direct_smoke.yaml \
  -precise_timing
```

Run the standard readiness lane used by CI:

```bash
cd sqlrunner
RUN_CORE=1 ./run-inabox-standard-readiness.sh
```

SQLRunner details live in [`sqlrunner/README.md`](../sqlrunner/README.md) and
[`sqlrunner/sqltests/README.md`](../sqlrunner/sqltests/README.md).

## MySQL Compatibility Work

The `mysql_compat_*.yaml` suites are the compatibility lab. They compare a
stock MySQL reference with QuantaStream for supported SQL shapes and track
roadmap gaps with explicit boundary suites.

Typical diff flow:

```bash
cd sqlrunner
MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/test' \
MYSQL_COMPAT_MODE=diff \
TARGET_ENGINE=inabox-standard \
./run-mysql-compat.sh
```

Focused suite example:

```bash
MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/test' \
MYSQL_COMPAT_SUITE=sqltests/mysql_compat_views.yaml \
MYSQL_COMPAT_MODE=diff \
TARGET_ENGINE=inabox-standard \
./run-mysql-compat.sh
```

Boundary suites should usually be run directly against QuantaStream targets so
`XFAIL` cases remain visible without making MySQL reference capture fail.

Compatibility policy and suite organization are documented in
[`MYSQL-COMPATIBILITY.md`](MYSQL-COMPATIBILITY.md),
[`MYSQL_COMPATIBILITY_LAB.md`](MYSQL_COMPATIBILITY_LAB.md), and
[`sqlrunner/sqltests/README.md`](../sqlrunner/sqltests/README.md).

## Schema And Catalog Work

Schemas are part of physical planning in QuantaStream. A field declares both SQL
shape and bitmap-native representation.

Before changing schema behavior, read:

- [`SCHEMA_DESIGN.md`](SCHEMA_DESIGN.md)
- [`../configuration/SCHEMA_CONFIG_REFERENCE.md`](../configuration/SCHEMA_CONFIG_REFERENCE.md)
- [`../configuration/AUTH_ACCESS.md`](../configuration/AUTH_ACCESS.md)

Catalog-backed table and view metadata lives under the configured runtime
configuration directory. Views are tracked under `views/` alongside table schema
metadata. Public release bundles keep schema documentation in `configuration/`
and runnable sample schemas under `samples/.../config`.

When adding schema features, update the reference docs and add SQLRunner or Go
coverage that proves the catalog behavior.

## Loader And Streaming Work

The JSON streaming loader is `cmd/quantastream-loader`. Configuration and
producer examples live in:

- [`JSON_LOADER_TUTORIAL.md`](JSON_LOADER_TUTORIAL.md)
- [`../configuration/STREAMING_LOADER.md`](../configuration/STREAMING_LOADER.md)

Use loader tests and small local runs before scaling to AWS. For performance
work, keep the target table schema, shard key, worker count, batch size, data
set, and row count in the notes or benchmark artifact.

## TPC-H And Benchmarks

TPC-H is the analytical validation and benchmark lane, not the only source of
SQL correctness. General SQL behavior discovered during TPC-H work should be
backfilled into broad SQLRunner suites.

Useful docs:

- [`TPCH.md`](TPCH.md)
- [`TPCH_BENCHMARK_NOTES.md`](TPCH_BENCHMARK_NOTES.md)
- [`BENCHMARK_LAB.md`](BENCHMARK_LAB.md)

Keep public benchmark claims tied to reproducible SQLRunner reports, commit
hashes, data scale, instance type, worker settings, and schema version.

## Code Change Guidelines

- Prefer focused changes that match the existing package boundary.
- Keep SQL behavior changes backed by Go tests, SQLRunner suites, or both.
- Add boundary or `XFAIL` cases for known MySQL-compatible behavior that is not
  implemented yet.
- Do not add broad parser or planner rewrites for a narrow compatibility issue.
- Keep session ownership explicit. `core.Session` is single-owner; parallel
  ingestion should route work to independent session workers.
- Treat cross-query caches as product features, not incidental optimizations.
  Query-local caches are fine when their lifetime and invalidation are obvious.
- Avoid adding AWS or cloud-provider dependencies to core ingestion or query
  paths. Operational scripts can remain cloud-specific.
- Prefer GitHub Issues for public future work instead of embedding long
  roadmap inventories in this document.

## Supported And Unsupported SQL Docs

Keep user-facing SQL docs current:

- [`SUPPORTED_SQL.md`](SUPPORTED_SQL.md)
- [`UNSUPPORTED_SQL.md`](UNSUPPORTED_SQL.md)

The supported doc should describe what works with confidence. The unsupported
doc should list specific unsupported or partial features, not old caveats that
have since been implemented.

## Release Notes For Developers

Release work is separate from ordinary development. Do not roll a new release
for documentation-only corrections unless the docs are packaged into a release
artifact that must be corrected immediately.

For source docs, a normal commit to `main` is enough. For packaged binary
changes, rebuild and publish a new release candidate or release artifact with
checksums.

## Quick Pre-Push Checklist

For most code changes:

```bash
go test ./... -count=1 -timeout=2m
```

For SQL behavior changes, also run the most relevant SQLRunner suite. Examples:

```bash
cd sqlrunner
go run . -engine proxy -host 127.0.0.1 -port 4000 -user qstream -db quanta \
  -suite_file sqltests/mysql_compat_functions.yaml -precise_timing

go run . -engine inabox-direct -consul 127.0.0.1:8500 \
  -suite_file sqltests/mysql_compat_views_boundaries.yaml -compat_report
```

For docs-only changes:

```bash
git diff --check
```

Keep the working tree clean before handing work to another developer or cutting
a release artifact.
