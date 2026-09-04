
## How to start a cluster locally

`start-local.sh` starts the Consul-backed local distributed-shape harness:
local nodes plus the MySQL query front door.

```sh
./start-local.sh
```

The script starts the bundled single-node Consul helper automatically unless
`QUANTASTREAM_SKIP_CONSUL_START=1` is set. Consul uses durable state under
`.local/consul/data`. This keeps local schema metadata available across Consul
process restarts and WSL restarts, as long as `.local` is not deleted.

You can still start Consul explicitly:

```sh
./start-consul.sh
```

Local cluster startup defaults to fast local iteration: cross-node startup
synchronization is skipped and nodes are marked active against trusted local
data.

```sh
./start-local.sh
```

This sets `QUANTA_DEV_SKIP_SYNC=1` when the variable is not already specified.
The full startup sync path is retained for future distributed-mode work and can
be exercised explicitly:

```sh
./start-local.sh --sync-startup
```

Nodes do not take autonomous timed bitmap savepoints. Mutations live in memory
immediately, and durability is established by explicit commit/checkpoint calls
or orderly shutdown.

The tradeoff is intentional: an in-flight load that has not reached an explicit
savepoint may need to be restarted after a node crash.

## How to start direct mode locally

`start-direct.sh` starts the same Consul-backed local nodes, but skips the MySQL
query front door. Use it when SQLRunner owns the query engine in `inabox-direct`
mode.

```sh
./start-direct.sh
```

Then run direct-mode SQLRunner suites from `../sqlrunner`.

Or, use a debugger or `go run .` directly.

Optional: delete the files in `../test/localClusterData` when you want to clear
local node data. If you also delete `.local/consul/data`, recreate schemas before
starting a cluster against existing data files.

## How to start distributed mode on hosts

Distributed mode separates the data-node processes from the MySQL-compatible
front door. Each QS server runs `quantastream-node` against its local Consul
agent and data directory. The bench runner runs `quantastream-proxy`, which
discovers the nodes through Consul and listens for MySQL clients.

On each QS server:

```sh
cd ~/quantastream
sudo -E ./startup-scripts/install-distributed-node-service.sh
```

Useful node overrides:

```sh
QUANTASTREAM_NODE_HASH_KEY=qs-server-1 \
QUANTASTREAM_DATA_DIR=/home/ubuntu/quantastream/tpc-h-benchmark/local/distributed-data \
QUANTASTREAM_NODE_PORT=4400 \
  sudo -E ./startup-scripts/install-distributed-node-service.sh
```

On the bench runner:

```sh
cd ~/quantastream
sudo -E ./startup-scripts/install-distributed-proxy-service.sh
```

Useful proxy overrides:

```sh
QUANTASTREAM_BIND=0.0.0.0 \
QUANTASTREAM_MYSQL_PORT=4000 \
QUANTASTREAM_CONSUL_ENDPOINT=127.0.0.1:8500 \
QUANTASTREAM_NODE_PORT=4400 \
QUANTASTREAM_SCHEMA_DIR=/home/ubuntu/quantastream/tpc-h-benchmark/config \
  sudo -E ./startup-scripts/install-distributed-proxy-service.sh
```

The installed services are named `quantastream-node` and `quantastream-proxy`.
They are enabled through systemd and start after `consul.service`, so stopping
and starting AWS instances should rejoin the Consul cluster and remount the
QuantaStream processes automatically.

Distributed node services default to `QUANTASTREAM_SKIP_NODE_SYNC=1`. This
marks nodes active from their local store on startup and disables the legacy
peer push sync path. For benchmark clusters, load data only after the full node
set is online.

For correctness benchmarks, do not expose the same fully loaded single-node data
directory from multiple cloned QS servers unless replication is the explicit test
case. Until cluster scale-out sync/rebalance is designed and validated, start
distributed nodes with empty per-node storage separate from standalone
`standard-data`, bring the full target cluster online, and then load through the
distributed path. Online scale-out from an already loaded node is a future
2.0-class capability, not a supported benchmark setup.

AWS helper scripts for deploys, health checks, schema sync, distributed loads,
and read-only benchmark runs live in `startup-scripts/aws/`.

## How to start standard mode locally

`start-standard.sh` starts the standalone QIAB product path: one
`quantastream` process with the local node backend and MySQL front door.

```sh
QUANTASTREAM_CONFIG_DIR=tpc-h-benchmark/config \
QUANTASTREAM_DATA_DIR=tpc-h-benchmark/local/standard-data \
QUANTASTREAM_AUTH_MODE=permissive \
  ./start-standard.sh
```

This explicit permissive mode is limited to a loopback-bound local evaluation.
Use `QUANTASTREAM_AUTH_MODE=static` with configured credentials for any
networked environment.

The MySQL front door is the compatibility/control lane. For high-throughput
loaders, standard mode can also expose the native bitmap/KV gRPC node surface
on a separate port:

```sh
QUANTASTREAM_CONFIG_DIR=tpc-h-benchmark/config \
QUANTASTREAM_DATA_DIR=tpc-h-benchmark/local/standard-data \
QUANTASTREAM_NATIVE_GRPC_PORT=4100 \
QUANTASTREAM_AUTH_MODE=permissive \
  ./start-standard.sh
```
