#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$script_dir/lib.sh"

usage() {
  cat <<'EOF'
Usage: ./startup-scripts/aws/load-tpch-distributed.sh [--skip-schema] [--skip-health]

Loads the configured TPC-H data set into the full distributed cluster. The
cluster should start empty and all target nodes should already be joined.
EOF
}

skip_schema=0
skip_health=0
for arg in "$@"; do
  case "$arg" in
    --skip-schema)
      skip_schema=1
      ;;
    --skip-health)
      skip_health=1
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

qs_load_env

if (( ! skip_health )); then
  qs_wait_for_cluster "$QS_CLUSTER_WAIT_SECONDS" >/dev/null
fi
if (( ! skip_schema )); then
  "$script_dir/sync-schema.sh"
fi

echo "Loading TPC-H data_dir=$QS_TPCH_DATA_DIR workers=$QS_TPCH_LOAD_WORKERS batch_size=$QS_TPCH_LOAD_BATCH_SIZE cluster_size=$QS_CLUSTER_SIZE"
(
  cd "$qs_repo_root/tpc-h-benchmark"
  ADMIN_CONSUL_ADDR="$QS_CONSUL_ENDPOINT" \
  ADMIN_PORT="$QS_NODE_PORT" \
  WAIT_SECONDS=180 \
  TPCH_LOAD_MODE=cluster \
  TPCH_CLUSTER_SIZE="$QS_CLUSTER_SIZE" \
  ./tpch-direct.sh "$QS_TPCH_DATA_DIR" "$QS_TPCH_LOAD_WORKERS" "$QS_TPCH_LOAD_BATCH_SIZE"
)
