# QuantaStream

QuantaStream is a bitmap-native analytical database engine for SQL over compressed bitmap and BSI-backed data structures. It uses late materialization and relationship-vector joins where they fit the query shape instead of defaulting to row-by-row processing.

The project exposes a MySQL-compatible SQL surface so existing tools and drivers can connect through familiar protocols, while the engine underneath is optimized for analytical filtering, joins, aggregation, categorical distribution analysis, and live-ingest-friendly workloads.

For latest news and information, as well as benchmark results, please visit [quantastream.org](https://quantastream.org).

## Download QuantaStream

The fastest way to try QuantaStream is the Linux AMD64 binary release bundle.
It includes the single-node engine, admin tool, JSON loader, TPC-H loader,
documentation, example service files, and a small TPC-H SF0.01 sample backup.
This path does not require Go. The packaged `configuration/` directory is
documentation and schema reference material; the first runnable sample lives
under `samples/tpch-sf-0.01/`.

Use the source checkout path when you want to build from source, use the
development startup scripts, run on macOS or another platform, or make code
changes. Go must be installed before using the source workflow.

Recommended binary download path:

```bash
QS_VERSION=0.1.1

gh release download "v${QS_VERSION}" \
  --repo QuantaStream/quantastream \
  --pattern "qstream-${QS_VERSION}-linux-amd64.tar.gz" \
  --pattern SHA256SUMS

sha256sum -c SHA256SUMS --ignore-missing
tar -xzf "qstream-${QS_VERSION}-linux-amd64.tar.gz"
cd "qstream-${QS_VERSION}-linux-amd64"
```

The `gh` command downloads the release assets from GitHub. The accompanying
`SHA256SUMS` file verifies that the bundle you unpack is the bundle published
with the release.

If you do not have the GitHub CLI installed, open the
[GitHub Releases](https://github.com/QuantaStream/quantastream/releases) page
in a browser and download both files manually:

- `qstream-0.1.1-linux-amd64.tar.gz`
- `SHA256SUMS`

Then run the same verification and unpack commands:

```bash
QS_VERSION=0.1.1

sha256sum -c SHA256SUMS --ignore-missing
tar -xzf "qstream-${QS_VERSION}-linux-amd64.tar.gz"
cd "qstream-${QS_VERSION}-linux-amd64"
```

For source builds and development scripts:

```bash
git clone git@github.com:QuantaStream/quantastream.git
cd quantastream
go test ./...
```

Start with [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) for the binary
runbook or [docs/QUICKSTART.md](docs/QUICKSTART.md) for the source checkout
workflow.

## Why QuantaStream

Traditional databases usually optimize around row storage, indexes, and tuple-oriented execution. QuantaStream starts from a different assumption: analytical workloads often want to reduce large sets quickly, combine those sets, and materialize only the final values needed by the user.

QuantaStream represents data using bitmap-oriented structures, including Roaring Bitmaps and Bit-Sliced Indexes. This allows the engine to answer many predicates and joins as set operations over compressed data:

- bitmap intersections, unions, and differences for filtering and boolean logic
- BSI operations for numeric, timestamp, and relationship-vector work
- dictionary-backed low-cardinality string handling for categorical data
- late materialization of strings and scalar values after set reduction
- relationship-vector traversal for parent/child joins

The result is an engine that feels accessible through ordinary SQL while taking advantage of bitmap algebra internally.

## Current Status

QuantaStream has prepared a public release centered on a production-credible single-node engine, repeatable correctness suites, and a clear binary getting-started path.

Current release surface:

- a single-node QuantaStream runtime with MySQL-compatible client access
- bitmap-native SQL planning and execution
- descriptor-driven schema configuration
- SQLRunner correctness and compatibility suites
- TPC-H analytical validation suites
- static account and access-policy files for local deployments
- local WAL, backup, restore, and support-bundle tooling
- packaged binary artifacts for first-time users

Forward work is tracked in GitHub Issues instead of duplicated planning lists in the documentation.

## Architecture At A Glance

QuantaStream separates SQL-facing planning from bitmap-native storage and execution.

Core components:

- **SQL and planner layer:** Parses SQL, builds query intent, validates supported shapes, and coordinates execution.
- **Execution runtime:** Applies bitmap, BSI, relationship-vector, aggregate, and materialization kernels.
- **Data nodes:** Own shards and store bitmap/BSI-backed column data and supporting persisted values.
- **Consul metadata and discovery:** Provides cluster coordination, service discovery, and schema metadata storage for distributed deployments.
- **QuantaStream-in-a-Box:** Runs the local development topology in one process while preserving the same conceptual node/query boundaries.

QuantaStream is designed so QuantaStream-in-a-Box can be productive for small deployments and demos, while the same core architecture can grow toward multi-node distributed deployments.

For a deeper architectural overview, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## SQL Surface

QuantaStream supports a focused, documented MySQL-compatible analytical SQL surface. The MySQL protocol makes the engine accessible from familiar tools and drivers, while the supported SQL contract is defined by executable SQLRunner compatibility suites.

The current SQL surface includes:

- projections, filters, ordering, limits, and offsets
- joins over relationship vectors
- grouping and aggregate execution
- function expressions in select lists and predicates
- subquery and membership shapes used by analytical workloads
- views, derived tables, temporary tables, and CTAS materialization
- prepared-statement and metadata paths used by common MySQL clients
- QuantaStream-specific functions such as `topn(...)`
- TPC-H query-shape coverage through SQLRunner suites

Supported behavior is tracked in [docs/SUPPORTED_SQL.md](docs/SUPPORTED_SQL.md). Precise SQL boundaries are tracked in [docs/UNSUPPORTED_SQL.md](docs/UNSUPPORTED_SQL.md).

## Schema Model

QuantaStream schemas are explicit about how data should be represented. A field is not just a SQL type; it also declares the storage representation used by the bitmap engine.

Examples include:

- standard bitmaps for low-cardinality values
- StringEnum dictionaries for categorical strings
- BSI-backed integers, floating point values, and timestamps
- relationship-vector fields for parent/child table traversal
- backing storage for high-cardinality strings
- scalar versus set multiplicity for multi-valued fields

This schema-first design is central to QuantaStream performance. See [docs/SCHEMA_DESIGN.md](docs/SCHEMA_DESIGN.md) for details.

## QuantaStream-in-a-Box

QuantaStream-in-a-Box is the primary single-node development, demo, and validation environment. It runs the local engine shape needed for SQL conformance work and TPC-H experimentation without requiring a distributed deployment.

Recommended binary environment:

- Linux or WSL2
- a MySQL-compatible client, such as `mysql` or MySQL Workbench
- 16 GB RAM minimum, 32 GB recommended for TPC-H experimentation

Go is required only when building or running QuantaStream from source. HashiCorp Consul is required for distributed mode and direct-cluster development, not for the single-node binary runbook.

Start with [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) for the binary runbook or [docs/QUICKSTART.md](docs/QUICKSTART.md) for the source checkout workflow. Deployment assumptions and production-readiness notes are in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Validation And Benchmarks

QuantaStream uses SQLRunner suites to lock down SQL behavior and result correctness. TPC-H is the primary analytical validation suite for query-shape coverage and benchmark discipline.

TPC-H validation notes and benchmark methodology are documented in [docs/TPCH.md](docs/TPCH.md) and [docs/TPCH_BENCHMARK_NOTES.md](docs/TPCH_BENCHMARK_NOTES.md).

## Driver And Tool Compatibility

The goal is broad compatibility with standard MySQL client tooling and network drivers. Covered and actively tested client ecosystems include:

- Go SQL drivers
- Java/JDBC
- Python MySQL connectors
- Node.js MySQL clients
- MySQL command-line tools
- MySQL Workbench

The compatibility plan is documented in [docs/MYSQL-COMPATIBILITY.md](docs/MYSQL-COMPATIBILITY.md).

## Project Tracking

Use [GitHub Issues](https://github.com/QuantaStream/quantastream/issues) for open work, design follow-through, and release tracking. Public docs describe supported behavior, operating procedures, and validation methods.

## License And Notices

QuantaStream is licensed under the Elastic License 2.0. See
[LICENSE.txt](LICENSE.txt).

QuantaStream includes software derived from or inspired by the public
Disney/Quanta project. See [NOTICE](NOTICE) and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for attribution and upstream
license details.

## Documentation

Useful starting points:

- [Quickstart](docs/QUICKSTART.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Schema Design](docs/SCHEMA_DESIGN.md)
- [Supported SQL](docs/SUPPORTED_SQL.md)
- [SQL Boundaries](docs/UNSUPPORTED_SQL.md)
- [TPC-H Validation](docs/TPCH.md)
- [TPC-H Benchmark Notes](docs/TPCH_BENCHMARK_NOTES.md)
- [Deployment](docs/DEPLOYMENT.md)
- [JSON Loader Tutorial](docs/JSON_LOADER_TUTORIAL.md)
- [MySQL Compatibility](docs/MYSQL-COMPATIBILITY.md)
- [Glossary](docs/GLOSSARY.md)
- [Development Notes](docs/DEVELOPMENT.md)

## Contributing

Every engine behavior change should be backed by focused Go tests, SQLRunner coverage, or both. Use GitHub Issues to discuss larger changes before starting implementation.
