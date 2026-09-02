# Streaming Loader Configuration

`quantastream-loader` is the high-throughput ingest front door for batch-style
replay and streaming adapters. It is a separate process from `quantastream`.
The loader receives source events, normalizes them into QuantaStream ingest
envelopes, evaluates table selectors, and writes through the native mutation
lane.

The first protocol adapter is HTTP JSON. Additional adapters should plug into
the same normalized envelope boundary rather than bypassing loader routing.

## Process Roles

`quantastream`
: Owns the engine, catalog, storage, MySQL-compatible query endpoint, and native
  node gRPC endpoint.

`quantastream-loader`
: Listens for incoming ingest events and forwards accepted records to the engine
  through the native loader connection.

`tpc-h-benchmark/tpch-stream-producer`
: Development workload generator that produces TPC-H-shaped `orders` and
  `lineitem` JSON events.

## Start Inabox-Standard With Native gRPC

Start the engine with both MySQL and native loader endpoints enabled:

```bash
cd ~/projects/quantastream
go run ./cmd/quantastream \
  -config-dir tpc-h-benchmark/config \
  -data-dir tpc-h-benchmark/local/standard-data \
  -bind 127.0.0.1 \
  -mysql-port 4000 \
  -native-grpc-port 4100 \
  -pprof-bind 127.0.0.1:6060 \
  -database quanta
```

The MySQL endpoint serves SQL clients. The native gRPC endpoint is used by
high-throughput loaders.

## Start The Loader

```bash
cd ~/projects/quantastream
go run ./cmd/quantastream-loader \
  -config-dir tpc-h-benchmark/config \
  -native-grpc-addr 127.0.0.1:4100 \
  -listen 127.0.0.1:8088 \
  -pprof-bind 127.0.0.1:6061 \
  -tables orders,lineitem
```

Important flags:

| Flag | Default | Description |
| --- | --- | --- |
| `-listen` | `127.0.0.1:8088` | HTTP listen address for protocol adapters. |
| `-config-dir` | `configuration` | Catalog/schema directory used for selector and table metadata. |
| `-database` | `quanta` | Schema/database name used when reading catalog objects. |
| `-tables` | all active/discovered selector tables | Comma-separated table allowlist. |
| `-connection-mode` | `standard-native` | Engine connection mode: `standard-native` or `distributed`. |
| `-native-grpc-addr` | `127.0.0.1:4100` | Native gRPC endpoint for `standard-native`. |
| `-consul-addr` | `127.0.0.1:8500` | Consul endpoint for `distributed`. |
| `-workers` | `1` | Session-router worker count. |
| `-channel-size` | `100000` | Buffered channel size per router worker. |
| `-flush-interval` | `1s` | Idle flush interval for router-owned sessions. |
| `-commit-on-close` | `true` | Drain loader sessions and commit the native/distributed backend during orderly shutdown. |
| `-default-source` | `json-http` | Source used when a JSON event omits `source`. |
| `-physical-build-routing` | `false` | Optional time-quantum build-shard routing for safe source shapes. |
| `-pprof-bind` | disabled | Optional pprof listen address. Use a different port than the engine. |

Use `GET /healthz` to confirm the loader is ready:

```bash
curl http://127.0.0.1:8088/healthz
```

Use `GET /stats` to inspect live ingest pressure and timing summaries:

```bash
curl -s http://127.0.0.1:8088/stats | jq .
```

The stats response includes a `pipeline` block with the lifecycle counters most
useful during a bulk load:

| Field | Meaning |
| --- | --- |
| `accepted` | Records accepted by the HTTP adapter and enqueued for router workers. |
| `failed` | Records rejected before enqueue. |
| `processed` | Records that completed the worker-owned `PutRow` path. |
| `flushed` | Worker-owned session buffers flushed to the engine. |
| `committed` | Explicit loader commit requests that completed successfully. |
| `pending_queued` | Records and control messages still queued in router worker channels. |
| `open_sessions` | Router-owned engine sessions currently open. |

The full response also includes router queue depth by worker, per-table PutRow
counters, flush counters, drain counters, derived rates, coarse Go runtime
memory/goroutine counters, and whether commit-on-close is enabled for the
running loader.

## Flush And Commit

`POST /ingest/json` reports that records were accepted into the loader. Accepted
records are not guaranteed to be flushed or committed yet. Router workers own
their sessions, so explicit flush requests are routed through the same worker
queues and run after any accepted records already ahead of the flush marker.

Use `POST /flush` to push accepted records through worker-owned buffers without
stopping the loader:

```bash
curl -fsS -X POST http://127.0.0.1:8088/flush | jq .
```

Use `POST /commit` to flush first and then ask the backend to persist the
current storage savepoint:

```bash
curl -fsS -X POST http://127.0.0.1:8088/commit | jq .
```

`-commit-on-close=true` still commits during orderly loader shutdown. The
explicit `/commit` endpoint is the preferred boundary for long-running loaders
that need a durable checkpoint while the process keeps accepting traffic.

When `-pprof-bind` is enabled, capture profiles while a producer run is active:

```bash
go tool pprof -top http://127.0.0.1:6060/debug/pprof/profile?seconds=30
go tool pprof -top http://127.0.0.1:6061/debug/pprof/profile?seconds=30
```

Use the engine profile to inspect server-side mutation/persistence cost. Use
the loader profile to inspect JSON decode, selector routing, PutRow staging,
native gRPC batching, and router pressure.

## JSON Ingest Endpoint

The initial endpoint is:

```text
POST /ingest/json
```

The endpoint accepts:

- one raw payload object;
- one normalized envelope object;
- an array of objects;
- an object containing an `events` or `records` array.

Raw payload example:

```json
{
  "type": "orders",
  "data": {
    "o_orderkey": 100000001,
    "o_custkey": 1,
    "o_orderstatus": "O",
    "o_totalprice": 100.00,
    "o_orderdate": "1995-03-15",
    "o_orderpriority": "1-URGENT",
    "o_clerk": "Clerk#000000001",
    "o_shippriority": 0,
    "o_comment": "streaming order"
  }
}
```

Normalized envelope example:

```json
{
  "mode": "stream",
  "event_id": "tpch.orders.100000001",
  "source": "tpch-stream-producer",
  "event_time": "1995-03-15T12:00:00Z",
  "source_offset": "orders:1",
  "shard_key": "tpch.order.100000001",
  "payload": {
    "type": "orders",
    "data": {
      "o_orderkey": 100000001,
      "o_custkey": 1,
      "o_orderstatus": "O",
      "o_totalprice": 100.00,
      "o_orderdate": "1995-03-15",
      "o_orderpriority": "1-URGENT",
      "o_clerk": "Clerk#000000001",
      "o_shippriority": 0,
      "o_comment": "streaming order"
    }
  }
}
```

Batch request example:

```json
{
  "events": [
    {
      "mode": "stream",
      "event_id": "tpch.orders.100000001",
      "source": "tpch-stream-producer",
      "shard_key": "tpch.order.100000001",
      "payload": {
        "type": "orders",
        "data": {
          "o_orderkey": 100000001,
          "o_custkey": 1,
          "o_orderstatus": "O",
          "o_totalprice": 100.00,
          "o_orderdate": "1995-03-15",
          "o_orderpriority": "1-URGENT",
          "o_clerk": "Clerk#000000001",
          "o_shippriority": 0,
          "o_comment": "streaming order"
        }
      }
    }
  ]
}
```

The response reports accepted records, failed records, and compact route
details:

```json
{
  "accepted": 1,
  "failed": 0,
  "routes": [
    {
      "index": 0,
      "table": "orders",
      "event_id": "tpch.orders.100000001",
      "shard_mode": "explicit"
    }
  ]
}
```

## Nested Streaming Events

Some streaming sources naturally arrive as one parent event with nested child
arrays. For example, an order event may include the line items for that order.
QuantaStream can ingest that shape when the parent schema declares a
`ChildRelation` and the child schema declares the corresponding
`ParentRelation`.

### Choosing Flat Or Nested Events

Choose the input shape that matches the source boundary:

- Use a nested envelope when a live source naturally emits one complete
  aggregate event, such as an order and its lineitems. The loader can resolve
  the enclosing parent relationship within one session without a separate
  committed-parent lookup.
- Use flat records for table-oriented batch migration, independent change
  events, or sources where parent and child records arrive separately. Load and
  commit referenced parents before their children; use a shared `shard_key`
  when related streaming records should stay on the same session worker.
- Do not nest arbitrary relational graphs solely to avoid load ordering. Foreign
  keys outside the enclosing parent-child edge still require their referenced
  rows to exist and be visible.

In short, batch migration preserves relational table boundaries; streaming
ingestion preserves natural event boundaries. The loader supports both.

Parent schema excerpt:

```yaml
tableName: orders
primaryKey: o_orderkey
selector: type="orders"
attributes:
- fieldName: o_orderkey
  sourceName: /data/o_orderkey
  mappingStrategy: IntBSI
  type: Integer
  columnID: true
- sourceName: /data/lineitems
  mappingStrategy: ChildRelation
  childTable: lineitem
```

Child schema excerpt:

```yaml
tableName: lineitem
primaryKey: l_orderkey+l_linenumber
selector: type="lineitem"
attributes:
- fieldName: l_orderkey
  sourceName: l_orderkey
  mappingStrategy: ParentRelation
  foreignKey: orders
  type: Integer
- fieldName: l_linenumber
  sourceName: l_linenumber
  mappingStrategy: IntBSI
  type: Integer
- fieldName: l_quantity
  sourceName: l_quantity
  mappingStrategy: IntBSI
  type: Integer
```

Nested event example:

```json
{
  "mode": "stream",
  "event_id": "tpch.orders.100000001",
  "source": "tpch-stream-producer",
  "shard_key": "tpch.order.100000001",
  "payload": {
    "type": "orders",
    "data": {
      "o_orderkey": 100000001,
      "o_orderstatus": "O",
      "lineitems": [
        {
          "l_orderkey": 100000001,
          "l_linenumber": 1,
          "l_quantity": 3
        },
        {
          "l_orderkey": 100000001,
          "l_linenumber": 2,
          "l_quantity": 5
        }
      ]
    }
  }
}
```

The loader routes the top-level event to the parent table. During `PutRow`,
QuantaStream assigns or resolves the parent row ID, expands each child object
from the configured child array, and maps the child's relationship back to the
current parent row within the same session. That avoids a separate committed
parent lookup for the enclosing parent-child edge.

Nested ingestion is a streaming-envelope feature, not a general replacement for
batch relational load ordering. Additional child foreign keys that do not point
to the enclosing parent still require the referenced parent rows to exist and
be visible to the loader.

## TPC-H Stream Producer

The TPC-H producer is a development driver for the loader endpoint:

```bash
cd ~/projects/quantastream
go run ./tpc-h-benchmark/tpch-stream-producer \
  -target http://127.0.0.1:8088/ingest/json \
  -orders 10 \
  -lineitems 4 \
  -batch-size 25
```

It emits flat `orders` and `lineitem` records that match the real TPC-H schema.
Each order and its lineitems use the same `shard_key` so the loader sends that
group through the same session worker.

When using the full TPC-H schema, load parent dimension tables first:

- `customer`
- `part`
- `supplier`

The producer defaults to key ranges that fit the SF 0.01 parent tables:

| Flag | Default | Description |
| --- | --- | --- |
| `-orders` | `10` | Number of synthetic orders. |
| `-lineitems` | `4` | Lineitems per order. |
| `-batch-size` | `25` | Events per HTTP POST. |
| `-base-order-key` | `100000001` | First generated order key. |
| `-customer-count` | `1500` | Existing customer key domain size. |
| `-part-count` | `2000` | Existing part key domain size. |
| `-supplier-count` | `100` | Existing supplier key domain size. |
| `-started-at` | `1995-03-15T12:00:00Z` | First event timestamp. |
| `-interval` | `0` | Optional sleep interval between generated orders. |

## Current Boundaries

- JSON is the first adapter; it is not the only intended protocol.
- MySQL `INSERT` remains the low-volume/control lane.
- Durable final-boundary deduplication is not complete yet.
- `standard-native` is the normal inabox-standard loader connection.
- `distributed` mode uses the Consul-backed connection path and will need more
  deployment validation.
- Physical build routing is opt-in because some streaming shapes need logical
  order affinity more than time-quantum placement.
