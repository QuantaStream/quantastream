
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

For fast local code iteration, you can skip cross-node startup synchronization:

```sh
./start-local.sh --dev-fast-start
```

This sets `QUANTA_DEV_SKIP_SYNC=1`, marks nodes active without the normal sync
push/verification phase, and logs a warning. Use it only with trusted local data;
do not use it for QIAB or production-like runs.

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
./start-direct.sh --dev-fast-start
```

Then run direct-mode SQLRunner suites from `../sqlrunner`.

Or, use a debugger or `go run .` directly.

Optional: delete the files in `../test/localClusterData` when you want to clear
local node data. If you also delete `.local/consul/data`, recreate schemas before
starting a cluster against existing data files.

## How to start standard mode locally

`start-standard.sh` starts the standalone QIAB product path: one
`quantastream` process with the local node backend and MySQL front door.

```sh
QUANTASTREAM_CONFIG_DIR=tpc-h-benchmark/config \
QUANTASTREAM_DATA_DIR=tpc-h-benchmark/local/standard-data \
  ./start-standard.sh
```

The MySQL front door is the compatibility/control lane. For high-throughput
loaders, standard mode can also expose the native bitmap/KV gRPC node surface
on a separate port:

```sh
QUANTASTREAM_CONFIG_DIR=tpc-h-benchmark/config \
QUANTASTREAM_DATA_DIR=tpc-h-benchmark/local/standard-data \
QUANTASTREAM_NATIVE_GRPC_PORT=4100 \
  ./start-standard.sh
```
