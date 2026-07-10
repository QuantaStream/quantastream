
## How to start a cluster locally

Start Consul first:

```sh
./start-consul.sh
```

The script runs a single-node Consul server with durable state under
`.local/consul/data`. This keeps local schema metadata available across Consul
process restarts and WSL restarts, as long as `.local` is not deleted.

Then start the local cluster:

```sh
./start-local.sh
```

For fast local code iteration, you can skip cross-node startup synchronization:

```sh
./start-local.sh --dev-fast-start
```

This sets `QUANTA_DEV_SKIP_SYNC=1`, marks nodes active without the normal sync
push/verification phase, and logs a warning. Use it only with trusted local data;
do not use it for QIAB or production-like runs.

Or, use a debugger or `go run .` directly.

Optional: delete the files in `../test/localClusterData` when you want to clear
local node data. If you also delete `.local/consul/data`, recreate schemas before
starting a cluster against existing data files.
