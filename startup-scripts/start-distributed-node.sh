#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
cd "$repo_root"

NODE_BIN="${QUANTASTREAM_NODE_BIN:-$repo_root/bin/quantastream-node}"
NODE_HASH_KEY="${QUANTASTREAM_NODE_HASH_KEY:-$(hostname -s)}"
DATA_DIR="${QUANTASTREAM_DATA_DIR:-$repo_root/tpc-h-benchmark/local/distributed-data}"
BIND_ADDRESS="${QUANTASTREAM_NODE_BIND:-0.0.0.0}"
NODE_PORT="${QUANTASTREAM_NODE_PORT:-4400}"
CONSUL_ENDPOINT="${QUANTASTREAM_CONSUL_ENDPOINT:-127.0.0.1:8500}"
ENVIRONMENT="${QUANTASTREAM_ENV:-PROD}"
LOG_LEVEL="${QUANTASTREAM_LOG_LEVEL:-INFO}"
PPROF="${QUANTASTREAM_PPROF:-false}"
SKIP_NODE_SYNC="${QUANTASTREAM_SKIP_NODE_SYNC:-${QUANTA_DEV_SKIP_SYNC:-1}}"
CONSUL_HEALTH_CHECK_PROFILE="${QUANTASTREAM_CONSUL_HEALTH_CHECK_PROFILE:-}"
CONSUL_HEALTH_CHECK_INTERVAL="${QUANTASTREAM_CONSUL_HEALTH_CHECK_INTERVAL:-}"
CONSUL_HEALTH_CHECK_TIMEOUT="${QUANTASTREAM_CONSUL_HEALTH_CHECK_TIMEOUT:-}"
CONSUL_HEALTH_CHECK_FAILURES_BEFORE_CRITICAL="${QUANTASTREAM_CONSUL_HEALTH_CHECK_FAILURES_BEFORE_CRITICAL:-}"
CONSUL_HEALTH_CHECK_DEREGISTER_AFTER="${QUANTASTREAM_CONSUL_HEALTH_CHECK_DEREGISTER_AFTER:-}"

usage() {
  cat <<'EOF'
Usage: ./start-distributed-node.sh

Starts one distributed QuantaStream data node. The first four node inputs are
positional arguments, so this wrapper turns service-friendly environment
variables into the expected process form.

Environment:
  QUANTASTREAM_NODE_BIN          quantastream-node binary path.
  QUANTASTREAM_NODE_HASH_KEY     Consistent-hash key. Defaults to hostname -s.
  QUANTASTREAM_DATA_DIR          Data directory. Defaults to tpc-h-benchmark/local/distributed-data.
  QUANTASTREAM_NODE_BIND         Node bind address. Defaults to 0.0.0.0.
  QUANTASTREAM_NODE_PORT         Node service port. Defaults to 4400.
  QUANTASTREAM_CONSUL_ENDPOINT   Local Consul agent endpoint. Defaults to 127.0.0.1:8500.
  QUANTASTREAM_ENV               Logging environment label. Defaults to PROD.
  QUANTASTREAM_LOG_LEVEL         ERROR, WARN, INFO, or DEBUG. Defaults to INFO.
  QUANTASTREAM_PPROF             Start pprof/prom listener. Defaults to false.
  QUANTASTREAM_SKIP_NODE_SYNC    Skip legacy peer sync on startup and mark node active. Defaults to 1.
  QUANTASTREAM_CONSUL_HEALTH_CHECK_PROFILE
                                  Consul health-check profile. Empty/production
                                  uses fast failure detection. Use bulk-load
                                  for large benchmark loads.
  QUANTASTREAM_CONSUL_HEALTH_CHECK_INTERVAL
                                  Consul gRPC health-check interval.
  QUANTASTREAM_CONSUL_HEALTH_CHECK_TIMEOUT
                                  Consul gRPC health-check timeout.
  QUANTASTREAM_CONSUL_HEALTH_CHECK_FAILURES_BEFORE_CRITICAL
                                  Consecutive health failures before critical.
  QUANTASTREAM_CONSUL_HEALTH_CHECK_DEREGISTER_AFTER
                                  Time critical before service deregistration.
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

if [[ ! -x "$NODE_BIN" ]]; then
  echo "quantastream-node binary is not executable: $NODE_BIN" >&2
  echo "Run startup-scripts/install-distributed-node-service.sh first." >&2
  exit 2
fi

echo "Starting quantastream-node"
echo "hash_key=${NODE_HASH_KEY}"
echo "data_dir=${DATA_DIR}"
echo "node=${BIND_ADDRESS}:${NODE_PORT}"
echo "consul=${CONSUL_ENDPOINT}"
echo "skip_node_sync=${SKIP_NODE_SYNC}"
echo "consul_health_check_profile=${CONSUL_HEALTH_CHECK_PROFILE:-production}"
echo "consul_health_check_interval=${CONSUL_HEALTH_CHECK_INTERVAL:-node-default}"
echo "consul_health_check_timeout=${CONSUL_HEALTH_CHECK_TIMEOUT:-node-default}"
echo "consul_health_check_failures_before_critical=${CONSUL_HEALTH_CHECK_FAILURES_BEFORE_CRITICAL:-node-default}"
echo "consul_health_check_deregister_after=${CONSUL_HEALTH_CHECK_DEREGISTER_AFTER:-node-default}"

export QUANTASTREAM_SKIP_NODE_SYNC="$SKIP_NODE_SYNC"
export QUANTASTREAM_CONSUL_HEALTH_CHECK_PROFILE="$CONSUL_HEALTH_CHECK_PROFILE"
export QUANTASTREAM_CONSUL_HEALTH_CHECK_INTERVAL="$CONSUL_HEALTH_CHECK_INTERVAL"
export QUANTASTREAM_CONSUL_HEALTH_CHECK_TIMEOUT="$CONSUL_HEALTH_CHECK_TIMEOUT"
export QUANTASTREAM_CONSUL_HEALTH_CHECK_FAILURES_BEFORE_CRITICAL="$CONSUL_HEALTH_CHECK_FAILURES_BEFORE_CRITICAL"
export QUANTASTREAM_CONSUL_HEALTH_CHECK_DEREGISTER_AFTER="$CONSUL_HEALTH_CHECK_DEREGISTER_AFTER"

exec "$NODE_BIN" \
  "$NODE_HASH_KEY" \
  "$DATA_DIR" \
  "$BIND_ADDRESS" \
  "$NODE_PORT" \
  --consul-endpoint "$CONSUL_ENDPOINT" \
  --env "$ENVIRONMENT" \
  --log-level "$LOG_LEVEL" \
  --pprof "$PPROF"
