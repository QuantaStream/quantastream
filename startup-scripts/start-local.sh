#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
log_dir="$script_dir/.local/logs"
log_file="$log_dir/start-local.log"

mkdir -p "$log_dir"
cd "$script_dir"

usage() {
  cat <<'EOF'
Usage: ./start-local.sh [--sync-startup] [--dev-fast-start] [--nodes-only]

Options:
  --sync-startup    Run cross-node startup synchronization. This preserves the
                    future distributed-mode path, but is not the local default.
  --dev-fast-start  Keep the default local behavior: skip cross-node startup
                    synchronization. This option is retained as a compatibility
                    alias for existing commands.
  --nodes-only      Set QUANTASTREAM_NODES_ONLY=1 to start local Consul-backed
                    nodes without starting the MySQL query proxy.

Environment:
  QUANTASTREAM_SKIP_CONSUL_START=1  Do not start the bundled local Consul helper.
EOF
}

export QUANTA_DEV_SKIP_SYNC="${QUANTA_DEV_SKIP_SYNC:-1}"

for arg in "$@"; do
  case "$arg" in
    --dev-fast-start|--skip-sync)
      export QUANTA_DEV_SKIP_SYNC=1
      ;;
    --sync-startup|--no-dev-fast-start)
      export QUANTA_DEV_SKIP_SYNC=0
      ;;
    --nodes-only)
      export QUANTASTREAM_NODES_ONLY=1
      ;;
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

if [[ ! "${QUANTASTREAM_SKIP_CONSUL_START:-}" =~ ^(1|true|TRUE|yes|YES|on|ON)$ ]]; then
  ./start-consul.sh
fi

echo "Starting local cluster; logging to $log_file"
if [[ "${QUANTA_DEV_SKIP_SYNC:-}" =~ ^(1|true|TRUE|yes|YES|on|ON)$ ]]; then
  echo "Dev fast start enabled; startup synchronization will be skipped."
else
  echo "Startup synchronization enabled."
fi
if [[ "${QUANTASTREAM_NODES_ONLY:-}" =~ ^(1|true|TRUE|yes|YES|on|ON)$ ]]; then
  echo "Starting nodes only; the MySQL query proxy on port 4000 will not be started."
fi
go run . > >(tee "$log_file") 2>&1
