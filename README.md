# QuantaStream

QuantaStream is a bitmap-native analytical database engine being prepared for an open-source 1.0 release. It is designed to execute SQL over compressed bitmap and BSI-backed data structures, with late materialization and relationship-vector joins used wherever possible instead of row-by-row processing.

The project exposes a MySQL-compatible SQL surface so existing tools and drivers can connect through familiar protocols, while the engine underneath is optimized for analytical filtering, joins, aggregation, categorical distribution analysis, and live-ingest-friendly workloads.

QuantaStream is currently in private pre-1.0 development. The public-facing documentation in this repository describes the intended core product, current development direction, and the local Quanta-in-a-Box workflow that will anchor the first release.

## Why QuantaStream

Traditional databases usually optimize around row storage, indexes, and tuple-oriented execution. QuantaStream starts from a different assumption: analytical workloads often want to reduce large sets quickly, combine those sets, and materialize only the final values needed by the user.

QuantaStream represents data using bitmap-oriented structures, including Roaring Bitmaps and Bit-Sliced Indexes. This allows the engine to answer many predicates and joins as set operations over compressed data:

- bitmap intersections, unions, and differences for filtering and boolean logic
- BSI operations for numeric, timestamp, and relationship-vector work
- dictionary-backed low-cardinality string handling for categorical data
- late materialization of strings and scalar values after set reduction
- relationship-vector traversal for parent/child joins

The long-term goal is an engine that feels accessible through ordinary SQL while taking advantage of bitmap algebra internally.

## Current Status

QuantaStream is not yet a public 1.0 release. The project is actively consolidating the core engine, SQL planner, execution runtime, schema model, and local deployment story.

Current development focus:

- Quanta-in-a-Box as the default local development and demo environment
- qsbridge-based SQL planning and execution
- MySQL-compatible protocol, session, and authentication work
- SQLRunner-based correctness suites
- TPC-H-oriented analytical validation
- schema documentation for bitmap-native data representation
- cleanup of historical Quanta code paths as the QuantaStream engine stabilizes

The repository is intentionally being simplified so the eventual public release is a complete and useful core product, not a limited community edition.

## Architecture At A Glance

QuantaStream separates SQL-facing planning from bitmap-native storage and execution.

Core components:

- **SQL and planner layer:** Parses SQL, builds query intent, validates supported shapes, and coordinates execution.
- **Execution runtime:** Applies bitmap, BSI, relationship-vector, aggregate, and materialization kernels.
- **Data nodes:** Own shards and store bitmap/BSI-backed column data and supporting persisted values.
- **Consul metadata and discovery:** Provides cluster coordination, service discovery, and schema metadata storage in the current distributed model.
- **Quanta-in-a-Box:** Runs the local development topology in one process while preserving the same conceptual node/query boundaries.

QuantaStream is designed so Quanta-in-a-Box can be productive for small deployments and demos, while the same core architecture can grow toward multi-node distributed deployments.

For a deeper architectural overview, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## SQL Surface

QuantaStream targets a practical analytical SQL subset rather than immediate full MySQL compatibility. The MySQL protocol matters because it makes the engine accessible from familiar tools and drivers; SQL semantics are being expanded deliberately through test coverage.

Current SQL work includes:

- projections, filters, ordering, limits, and offsets
- joins over relationship vectors
- grouping and aggregate execution
- function expressions in select lists and predicates
- subquery and membership shapes used by analytical workloads
- QuantaStream-specific functions such as `topn(...)`
- TPC-H query-shape coverage through SQLRunner suites

Supported behavior is tracked in [docs/SUPPORTED_SQL.md](docs/SUPPORTED_SQL.md). Known gaps are tracked in [docs/UNSUPPORTED_SQL.md](docs/UNSUPPORTED_SQL.md).

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

## Quanta-in-a-Box

Quanta-in-a-Box is the primary near-term development and validation environment. It runs the local cluster shape needed for demos, SQL conformance work, and TPC-H experimentation without requiring a full distributed deployment.

Recommended environment:

- Linux or WSL2
- Go 1.22+
- HashiCorp Consul
- 16 GB RAM minimum, 32 GB recommended for TPC-H experimentation

Start with [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) for the binary runbook or [docs/QUICKSTART.md](docs/QUICKSTART.md) for the source checkout workflow. Deployment assumptions and production-readiness notes are in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Validation And Benchmarks

QuantaStream uses SQLRunner suites to lock down SQL behavior and result correctness. TPC-H is the primary analytical validation roadmap, first for correctness and capability coverage, then for performance work.

The TPC-H roadmap tracks supported query shapes, staged probes, and remaining blockers across Q1-Q22. See [docs/TPCH.md](docs/TPCH.md).

## Driver And Tool Compatibility

The goal is broad compatibility with standard MySQL client tooling and network drivers. Existing and target client ecosystems include:

- Go SQL drivers
- Java/JDBC
- Python MySQL connectors
- Node.js MySQL clients
- MySQL command-line tools

The compatibility plan is documented in [docs/MYSQL-COMPATIBILITY.md](docs/MYSQL-COMPATIBILITY.md).

## Roadmap

### 1.0: Core QuantaStream

Primary goals:

- Quanta-in-a-Box local runtime
- bitmap-native SQL planner and execution runtime
- practical MySQL-compatible SQL endpoint
- documented schema design model
- SQLRunner correctness suites
- TPC-H-oriented demo and validation path
- batch loading and basic live-ingest demonstrations
- basic local backup, restart, and recovery guidance
- contributor-ready documentation and tests

### 2.0: Distributed And Operational Readiness

Planned areas:

- standards-based authentication and authorization integration
- analyzer tooling to infer schema candidates from sample data

### Future: Enterprise And Multi-Region

Longer-term direction:

- cloud-provider deployment integrations
- multi-region and multi-data-center topology support
- active/active high availability and disaster recovery
- advanced workload management and optimizer guidance

## Documentation

Useful starting points:

- [Quickstart](docs/QUICKSTART.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Schema Design](docs/SCHEMA_DESIGN.md)
- [Supported SQL](docs/SUPPORTED_SQL.md)
- [Unsupported SQL](docs/UNSUPPORTED_SQL.md)
- [TPC-H Roadmap](docs/TPCH.md)
- [TPC-H Benchmark Notes](docs/TPCH_BENCHMARK_NOTES.md)
- [Deployment](docs/DEPLOYMENT.md)
- [MySQL Compatibility](docs/MYSQL-COMPATIBILITY.md)
- [Glossary](docs/GLOSSARY.md)
- [Development Notes](docs/DEVELOPMENT.md)

## Contributing

QuantaStream is currently preparing for its public 1.0 release. External contribution workflow will be documented before the repository is opened broadly.

For now, development follows a simple rule: every engine behavior change should be backed by focused Go tests, SQLRunner coverage, or both.
