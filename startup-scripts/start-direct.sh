#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

usage() {
  cat <<'EOF'
Usage: ./start-direct.sh [--dev-fast-start] [--bulk-load-savepoint]

Starts the Consul-backed local data-node cluster without the MySQL query front
door. This is the expected companion process for SQLRunner inabox-direct mode,
where SQLRunner hosts the query engine in process and talks directly to the
local nodes.

Options:
  --dev-fast-start  Skip cross-node startup sync. Use only with trusted local data.
  --bulk-load-savepoint
                    Defer background bitmap savepoints until explicit
                    commit/shutdown. Useful for trusted bulk-load runs.

Environment:
  QUANTASTREAM_SKIP_CONSUL_START=1  Do not start the bundled local Consul helper.
  QUANTASTREAM_DEFER_BACKGROUND_PERSIST=1
                    Defer non-forced background bitmap savepoints.
EOF
}

for arg in "$@"; do
  case "$arg" in
    --dev-fast-start|--skip-sync)
      export QUANTA_DEV_SKIP_SYNC=1
      ;;
    --bulk-load-savepoint|--defer-background-persist)
      export QUANTASTREAM_DEFER_BACKGROUND_PERSIST=1
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
