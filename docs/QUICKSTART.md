# QUICKSTART.md

# QuantaStream Quick Start

For the beginner binary release flow, use
[`GETTING_STARTED.md`](GETTING_STARTED.md). This quick start is the source
checkout workflow.

## Requirements

Recommended environment:

- Linux or WSL2
- Go 1.22+
- Consul

Recommended minimum memory:

- 16 GB RAM minimum
- 32 GB RAM recommended for TPC-H experimentation

---

# Check The Source Build

```bash
go test ./...
```

---

# Start Consul

```bash
consul agent -dev -client=0.0.0.0
```

---

# Start QuantaStream-In-A-Box

```bash
cd startup-scripts
./start-standard.sh
```

This starts:

- MySQL-compatible query front door
- in-process local storage adapter
- file-backed local catalog/data directories

---

# Verify Cluster Status

```bash
go run ./qstream-admin status
```

Expected:

- Cluster UP
- 3 active nodes
- shard allocation visible

---

# Run SQLRunner Demo

```bash
cd sqlrunner
go run . \
  -suite_file sqltests/joins_sql.yaml \
  -host 127.0.0.1 \
  -user qstream \
  -db quanta \
  -port 4000
```

This performs:

- schema creation
- test data loading
- SQL execution
- complete result-set validation

---

# Connect with MySQL Client

```bash
mysql -h 127.0.0.1 -P 4000 -u root
```

---

# Current Focus

Current project priorities:

- QuantaStream-in-a-Box stabilization
- TPC-H demo coverage
- Streaming ingestion demonstrations
- SQL correctness improvements
- OSS onboarding simplification

This quick start describes the local QIAB development environment. Container
and multi-host operational requirements are documented in
[`DEPLOYMENT.md`](DEPLOYMENT.md).

Release bundles use `./bin/qstream-admin` for the same admin commands; see
[`GETTING_STARTED.md`](GETTING_STARTED.md) for the binary-first runbook.
