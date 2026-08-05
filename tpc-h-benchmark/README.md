# TPC-H Benchmark Helpers

This directory contains Quanta schemas and local helper scripts for TPC-H
validation.

The official TPC-H dbgen/qgen tools and generated `.tbl` data files are not
checked into this repository. Place or unpack the TPC-H dbgen kit locally, then
use the helper scripts below.

## Build dbgen

```bash
cd tpc-h-benchmark
./build-dbgen.sh ~/TPC-H\ V3.0.1/dbgen
```

The script verifies that `dbgen` exists or attempts to build it with `make`.

## Generate Data

```bash
cd tpc-h-benchmark
./generate-data.sh ~/TPC-H\ V3.0.1/dbgen 0.01
```

This generates `.tbl` files under:

```text
tpc-h-benchmark/local/data/sf-0.01
```

Generated data is intentionally ignored by git.

## Load Data Directly Into A Cluster

Start the Consul-backed local cluster first, create the TPC-H tables, then run:

```bash
cd tpc-h-benchmark
./create-tpch.sh
./tpch-direct.sh local/data/sf-0.01 3 1000
```

Arguments to `tpch-direct.sh` are:

```text
tpch-direct.sh <tpch-data-dir> [workers] [batch-size]
```

The direct loader uses the same record envelope as the Kinesis producer but
routes records directly into Quanta sessions through `core.SessionRouter`.

The existing cluster direct path remains the default for `tpch-direct.sh`:

```bash
TPCH_LOAD_MODE=cluster ./tpch-direct.sh local/data/sf-0.01 3 1000
```

In cluster mode, the loader process does not host storage itself. It connects to
the running Consul-backed local cluster and writes through the normal session
path.

## Load Data Into Inabox-Standard Storage

`inabox-standard` can load TPC-H data into a local data directory without a
Consul cluster or gRPC node hop. This is an offline/in-process load: do not run
the standalone `quantastream` server against the same data directory while the
loader is writing.

This loader mode is intentionally a little different from the SQLRunner
`inabox-direct` mode:

- SQLRunner `inabox-direct` hosts the query engine inside SQLRunner and talks to
  local clustered nodes.
- TPC-H `TPCH_LOAD_MODE=standard` hosts the lightweight local storage backend
  inside the loader process and writes directly to an `inabox-standard` data
  directory.
- The loader does not start the MySQL server. After the load finishes, start
  `cmd/quantastream` against the same config and data directories to query the
  data over the MySQL-compatible endpoint.

```bash
cd tpc-h-benchmark
rm -rf local/standard-data
TPCH_LOAD_MODE=standard \
TPCH_STANDARD_CONFIG_DIR=config \
TPCH_STANDARD_DATA_DIR=local/standard-data \
  ./tpch-direct.sh local/data/sf-0.01 1 1000
```

Use one worker for `inabox-standard` loads for now. Cluster-direct loads can use
multiple workers; the local standard storage path is intentionally serialized
until the multi-session write path is validated.

After the load completes, start `inabox-standard` against the same schema and
data directories:

```bash
cd ..
go run ./cmd/quantastream \
  -config-dir tpc-h-benchmark/config \
  -data-dir tpc-h-benchmark/local/standard-data \
  -bind 127.0.0.1 \
  -mysql-port 4000 \
  -database quanta
```

To run the full offline load, start a temporary `quantastream` process, compare
table counts to the `.tbl` files, and run the TPC-H smoke suite:

```bash
cd tpc-h-benchmark
./run-inabox-standard-tpch.sh local/data/sf-0.01 1 1000
```

The helper uses port `4400` by default so it does not collide with a local
cluster on `4000`. Useful environment variables:

```bash
PORT=4000                         # MySQL-compatible validation port
TPCH_STANDARD_DATA_DIR=local/tpch  # target inabox-standard data directory
RUN_LOAD=0                        # validate an already-loaded data directory
RUN_COUNTS=0                      # skip table-count validation
RUN_SUITE=0                       # skip SQLRunner suite validation
SUITE=sqltests/tpch_queries.yaml   # run a different TPC-H SQLRunner suite
CASE=tpch_queries.q7.030_supplier_line_order_shipdate_count
VERBOSE=1                         # print SQLRunner case SQL and timings
DUMP_ACTUAL=1                     # print rows for failing query cases
SLOW_THRESHOLD=10s                # summarize cases at or above this duration
KEEP_SERVER=1                     # leave the temporary server running
```

For an already-loaded `inabox-standard` data directory, use the same helper to
run the formal TPC-H roadmap suite through an isolated temporary server:

```bash
cd tpc-h-benchmark
RUN_LOAD=0 RUN_COUNTS=0 \
SUITE=sqltests/tpch_queries.yaml \
VERBOSE=1 \
  ./run-inabox-standard-tpch.sh
```

Prefer this helper over raw SQLRunner commands for `inabox-standard` TPC-H
validation. A raw command pointed at port `4000` targets whatever local harness
is already listening there, which can accidentally test a cluster or data root
that does not match the intended `inabox-standard` fixture.

The schema lifecycle scripts can use either the historical admin path or SQL
DDL through the query engine:

```bash
TPCH_DDL_MODE=admin ./create-tpch.sh
TPCH_DDL_MODE=sql QUANTA_PORT=4000 ./create-tpch.sh
```

## Load Data Into Stock MySQL

Use `load-mysql-tpch.sh` to load the same generated `.tbl` files into a
caller-managed MySQL instance for compatibility and benchmark comparison work:

```bash
cd tpc-h-benchmark
MYSQL_HOST=127.0.0.1 \
MYSQL_PORT=3306 \
MYSQL_USER=root \
MYSQL_PASSWORD='secret' \
MYSQL_DATABASE=tpch \
  ./load-mysql-tpch.sh local/data/sf-0.01
```

The loader creates the standard TPC-H tables, loads data with
`LOAD DATA LOCAL INFILE`, creates indexes, and validates counts against the
`.tbl` files. The MySQL client and server must both allow local infile loading.
If the server reports `local_infile=OFF`, enable it before loading.

`MYSQL_INDEX_PROFILE` controls the MySQL index posture:

```bash
MYSQL_INDEX_PROFILE=benchmark  # default: primary keys plus common join/filter indexes
MYSQL_INDEX_PROFILE=pk         # primary keys only
MYSQL_INDEX_PROFILE=none       # no indexes
```

The selected profile is benchmark evidence, so record it beside any timing
claim. After loading MySQL and starting a matching QuantaStream target, compare
the two systems through SQLRunner:

```bash
cd ../sqlrunner
MYSQL_DSN='root:secret@tcp(127.0.0.1:3306)/tpch' \
TARGET_ENGINE=inabox-standard \
TARGET_HOST=127.0.0.1 \
TARGET_PORT=4000 \
SUITE_FILE=../tpc-h-benchmark/sqltests/tpch_queries.yaml \
BENCHMARK_RUNS=3 \
  ./run-mysql-benchmark-compare.sh
```

## Native Ingest Micro-Benchmark

The native ingest benchmark mounts a temporary `inabox-standard` process,
connects through the standard native gRPC loader lane, routes deterministic
nested order/lineitem envelopes, and emits a JSON report with throughput plus
PutRow/flush profile totals:

```bash
cd tpc-h-benchmark
ORDERS=100 \
LINEITEMS=4 \
SHARDS=1 \
RUNS=3 \
PROFILE=standard-native-tpch-ingest \
  ./run-native-ingest-benchmark.sh
```

Set `PRIMARY_KEY_SHADOW=bsi` to keep KV primary-key resolution authoritative
while shadow-validating the in-memory BSI resolver path during the run. Shadow
mismatches fail the benchmark and the JSON report records shadow comparison
counts.

Reports and logs are written under
`tpc-h-benchmark/local/ingest-benchmarks/<timestamp>/` by default. Override
`BENCHMARK_OUTPUT_DIR`, `BENCHMARK_REPORT`, or `LOG_FILE` when comparing named
before/after runs.

Compare two native ingest JSON reports with:

```bash
cd tpc-h-benchmark
./run-native-ingest-compare.sh \
  local/ingest-benchmarks/before/standard-native-tpch-ingest.json \
  local/ingest-benchmarks/after/standard-native-tpch-ingest.json \
  local/ingest-benchmarks/comparison.md
```

## Validate Loaded Data

TPC-H SQL roadmap suites live under `tpc-h-benchmark/sqltests` so the benchmark
schemas, data helpers, and validation goals stay together. SQLRunner remains the
generic execution engine.

Run the smoke suite after creating and loading the SF 0.01 fixture:

```bash
cd ../sqlrunner
go run . \
  -suite_file ../tpc-h-benchmark/sqltests/tpch_smoke.yaml \
  -host 127.0.0.1 \
  -user MOLIG004 \
  -db quanta \
  -port 4000
```

Run the formal query roadmap suite with:

```bash
go run . \
  -suite_file ../tpc-h-benchmark/sqltests/tpch_queries.yaml \
  -host 127.0.0.1 \
  -user MOLIG004 \
  -db quanta \
  -port 4000
```


## End-To-End Benchmark Run

Use `run-tpch-benchmark.sh` to capture a full local benchmark cycle:

```bash
cd tpc-h-benchmark
./run-tpch-benchmark.sh local/data/sf-0.01 3 1000 5
```

The script runs:

1. `drop-tpch.sh`
2. `create-tpch.sh`
3. `tpch-direct.sh`
4. `tpch_smoke.yaml`
5. `tpch_profile.yaml`

Arguments are:

```text
run-tpch-benchmark.sh [tpch-data-dir] [workers] [batch-size] [profile-runs]
```

The smoke suite runs once by default. Override with `TPCH_SMOKE_RUNS`.
The exact default suites target the SF 0.01 fixture. For larger scale factors,
use the scale-friendly suites:

```bash
TPCH_SMOKE_SUITE=sqltests/tpch_smoke_scale.yaml \
TPCH_PROFILE_SUITE=sqltests/tpch_profile_scale.yaml \
  ./run-tpch-benchmark.sh local/data/sf-0.05 3 1000 3
```

For already-prepared clusters, individual setup phases can be skipped:

```bash
TPCH_SKIP_DROP=1 TPCH_SKIP_CREATE=1 ./run-tpch-benchmark.sh local/data/sf-0.01 3 1000 5
```

For an already-loaded larger data set, skip the destructive and load phases and
rerun only validation/profile suites:

```bash
TPCH_SKIP_DROP=1 TPCH_SKIP_CREATE=1 TPCH_SKIP_LOAD=1 \
TPCH_SMOKE_SUITE=sqltests/tpch_smoke_scale.yaml \
TPCH_PROFILE_SUITE=sqltests/tpch_profile_scale.yaml \
  ./run-tpch-benchmark.sh local/data/sf-0.05 3 1000 3
```

The wrapper writes a top-level timestamped log under
`tpc-h-benchmark/local/logs` and preserves the nested load, smoke, and profile
logs generated by the existing helper scripts. It also captures `JOIN_DRIVER`
diagnostic lines emitted during the benchmark window from
`startup-scripts/.local/logs/start-local.log` into a matching
`join-driver-YYYYMMDD-HHMMSS.log` file when those lines are present. Override
the source cluster log with `START_LOCAL_LOG=/path/to/start-local.log`.


## Capture A Baseline

Use `run-tpch-suite.sh` to run a suite repeatedly and capture a timestamped log:

```bash
cd tpc-h-benchmark
./run-tpch-suite.sh 3
```

The helper passes SQLRunner `-precise_timing`, so captured logs include
millisecond or microsecond case durations instead of collapsing subsecond cases
to `<1s>`.

Set `QUANTA_ENGINE` to capture the same suite through a specific SQLRunner
engine. This is useful for apples-to-apples profiling across execution modes:

```bash
QUANTA_ENGINE=inabox-direct ./run-tpch-suite.sh 1 sqltests/tpch_profile.yaml
QUANTA_ENGINE=inabox-standard QUANTA_PORT=4400 ./run-tpch-suite.sh 1 sqltests/tpch_profile.yaml
```

Set `CASES` to run one or more exact roadmap case IDs without cloning a suite:

```bash
CASES=tpch_q12_profile.020_shipmode_receiptdate_group_count \
  QUANTA_ENGINE=inabox-standard QUANTA_PORT=4400 \
  ./run-tpch-suite.sh 3 sqltests/tpch_q12_profile.yaml
```

Compare two or more captured suite logs, including logs with known FAIL/XFAIL
results, with:

```bash
./compare-tpch-suite.py \
  local/logs/tpch_profile-inabox-direct-1x-*.log \
  local/logs/tpch_profile-inabox-standard-1x-*.log
```

This comparison is a profiling flashlight rather than a correctness gate. Use
the JSON benchmark report mode for green, repeatable benchmark artifacts.

To run and compare modes in one pass, use:

```bash
CASES=tpch_q12_profile.020_shipmode_receiptdate_group_count \
  MODES="inabox-direct inabox-standard" \
  ./run-tpch-mode-compare.sh 1 sqltests/tpch_q12_profile.yaml
```

`inabox-standard` is started automatically against
`TPCH_STANDARD_DATA_DIR`/`TPCH_STANDARD_CONFIG_DIR`. Other modes assume their
normal environment is already running, for example the local cluster for
`inabox-direct` or `inabox-local`. The wrapper continues through known profile
suite failures by default so timing comparisons can still be generated; set
`EXIT_ON_FAILURE=1` when using only green suites.

The default suite is `sqltests/tpch_queries.yaml`. Logs are written under:

```text
tpc-h-benchmark/local/logs
```

Connection settings can be overridden with environment variables:

```bash
QUANTA_HOST=127.0.0.1 QUANTA_PORT=4000 QUANTA_USER=MOLIG004 QUANTA_DB=quanta ./run-tpch-suite.sh 3
```

An alternate suite can be supplied as the second argument:

```bash
./run-tpch-suite.sh 5 sqltests/tpch_smoke.yaml
```


The focused profile suite contains the current Q3/Q5 hotspot queries:

```bash
./run-tpch-suite.sh 5 sqltests/tpch_profile.yaml
```

Optional profile experiments live in `sqltests/tpch_profile_experiments.yaml`.
That suite is intentionally separate from the normal scale profile so benchmark
runs stay focused. It includes key-grouping comparators that keep the same join
graph and filters but group by numeric relationship keys instead of display
labels where possible.


Summarize a captured baseline log with:

```bash
./summarize-tpch-suite.py local/logs/tpch_queries-3x-YYYYMMDD-HHMMSS.log
```

The summary reports per-query run count, min, median, p95, max, and observed
statuses, sorted by median runtime.

Summarize captured join-driver diagnostics with:

```bash
./summarize-join-driver.py local/logs/join-driver-YYYYMMDD-HHMMSS.log
```

The join-driver summary reports selected drivers, post-reduction found-set
sizes, an approximate legality hint for known benchmark schemas, and cases where
a smaller parent-side table exists but would require a parent-to-child expansion
path before it can become a legal execution driver.
Summarize captured projector timing diagnostics with:

```bash
./summarize-projector-timing.py local/logs/projector-timing-YYYYMMDD-HHMMSS.log
```

The projector timing summary groups `PROJECTOR_TIMING` lines by table and field
set, then reports calls, total time, median, p95, max, average found-set size,
and cache hit/miss counts. Projector-local relationship and child payload BSI
reuse are expected to remove repeated `lineitem` relationship/payload projection
groups from the focused SF 0.05 profile; shared cross-query fragment caching is
deferred to the proxy-cache roadmap.
