#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
log_dir="$script_dir/.local/logs"
log_file="$log_dir/start-local.log"

mkdir -p "$log_dir"
cd "$script_dir"

usage() {
  cat <<'EOF'
Usage: ./start-local.sh [--dev-fast-start]

Options:
  --dev-fast-start  Set QUANTA_DEV_SKIP_SYNC=1 to skip cross-node startup sync.
                    Use only for local development with trusted local data.
EOF
}

for arg in "$@"; do
  case "$arg" in
    --dev-fast-start|--skip-sync)
      export QUANTA_DEV_SKIP_SYNC=1
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

echo "Starting local cluster; logging to $log_file"
if [[ "${QUANTA_DEV_SKIP_SYNC:-}" =~ ^(1|true|TRUE|yes|YES|on|ON)$ ]]; then
  echo "WARNING: QUANTA_DEV_SKIP_SYNC=${QUANTA_DEV_SKIP_SYNC}; startup synchronization will be skipped."
fi
go run . > >(tee "$log_file") 2>&1
