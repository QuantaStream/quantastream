#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
cd "$repo_root"

PROXY_BIN="${QUANTASTREAM_PROXY_BIN:-$repo_root/bin/quantastream-proxy}"
BIND_ADDRESS="${QUANTASTREAM_BIND:-0.0.0.0}"
MYSQL_PORT="${QUANTASTREAM_MYSQL_PORT:-4000}"
CONSUL_ENDPOINT="${QUANTASTREAM_CONSUL_ENDPOINT:-127.0.0.1:8500}"
NODE_PORT="${QUANTASTREAM_NODE_PORT:-4400}"
SCHEMA_DIR="${QUANTASTREAM_SCHEMA_DIR:-$repo_root/tpc-h-benchmark/config}"
DATABASE="${QUANTASTREAM_DATABASE:-quanta}"
RUNTIME_PROBES="${QUANTASTREAM_RUNTIME_PROBES:-false}"
SESSION_POOL_SIZE="${QUANTASTREAM_SESSION_POOL_SIZE:-0}"
PPROF_BIND="${QUANTASTREAM_PPROF_BIND:-}"

usage() {
  cat <<'EOF'
Usage: ./start-distributed-proxy.sh

Starts the MySQL-compatible proxy over a Consul-discovered distributed
QuantaStream cluster.

Environment:
  QUANTASTREAM_PROXY_BIN         quantastream-proxy binary path.
  QUANTASTREAM_BIND              MySQL bind address. Defaults to 0.0.0.0.
  QUANTASTREAM_MYSQL_PORT        MySQL listen port. Defaults to 4000.
  QUANTASTREAM_CONSUL_ENDPOINT   Local Consul agent endpoint. Defaults to 127.0.0.1:8500.
  QUANTASTREAM_NODE_PORT         Quanta data-node service port. Defaults to 4400.
  QUANTASTREAM_SCHEMA_DIR        Schema/catalog directory. Defaults to tpc-h-benchmark/config.
  QUANTASTREAM_DATABASE          Default database/schema. Defaults to quanta.
  QUANTASTREAM_RUNTIME_PROBES    Set to true to log runtime execution probes.
  QUANTASTREAM_SESSION_POOL_SIZE Direct runtime session pool size. Defaults to runtime default.
  QUANTASTREAM_PPROF_BIND        Optional pprof bind address.
EOF
}

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -x "$PROXY_BIN" ]]; then
  echo "quantastream-proxy binary is not executable: $PROXY_BIN" >&2
  echo "Run startup-scripts/install-distributed-proxy-service.sh first." >&2
  exit 2
fi

echo "Starting quantastream-proxy"
echo "mysql=${BIND_ADDRESS}:${MYSQL_PORT}"
echo "consul=${CONSUL_ENDPOINT}"
echo "node_port=${NODE_PORT}"
echo "schema_dir=${SCHEMA_DIR}"
echo "database=${DATABASE}"
echo "runtime_probes=${RUNTIME_PROBES}"

exec "$PROXY_BIN" \
  -bind "$BIND_ADDRESS" \
  -mysql-port "$MYSQL_PORT" \
  -consul "$CONSUL_ENDPOINT" \
  -node-port "$NODE_PORT" \
  -schema-dir "$SCHEMA_DIR" \
  -database "$DATABASE" \
  -session-pool-size "$SESSION_POOL_SIZE" \
  -runtime-probes="$RUNTIME_PROBES" \
  -pprof-bind "$PPROF_BIND"
