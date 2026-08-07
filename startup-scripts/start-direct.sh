#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

usage() {
  cat <<'EOF'
Usage: ./start-direct.sh [--sync-startup] [--dev-fast-start]

Starts the Consul-backed local data-node cluster without the MySQL query front
door. This is the expected companion process for SQLRunner inabox-direct mode,
where SQLRunner hosts the query engine in process and talks directly to the
local nodes.

Options:
  --sync-startup    Run cross-node startup synchronization. This preserves the
                    future distributed-mode path, but is not the local default.
  --dev-fast-start  Keep the default local behavior: skip cross-node startup
                    synchronization. This option is retained as a compatibility
                    alias for existing commands.

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

exec ./start-local.sh --nodes-only
