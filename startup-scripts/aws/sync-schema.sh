#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$script_dir/lib.sh"

TABLES=(
  region
  nation
  customer
  part
  supplier
  partsupp
  orders
  lineitem
)

confirm=1
for arg in "$@"; do
  case "$arg" in
    --dry-run)
      confirm=0
      ;;
    -h|--help)
      cat <<'EOF'
Usage: ./startup-scripts/aws/sync-schema.sh [--dry-run]

Creates or updates the TPC-H schemas in Consul through quanta-admin. By default
passes --confirm so additive schema/artifact changes deploy in place.
EOF
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      exit 2
      ;;
  esac
done

qs_load_env

for table in "${TABLES[@]}"; do
  args=(run ./quanta-admin --consul-addr "$QS_CONSUL_ENDPOINT" --port "$QS_NODE_PORT" create "$table" --schema-dir "$QS_SCHEMA_DIR")
  if (( confirm )); then
    args+=(--confirm)
  fi
  echo "Syncing schema table=$table"
  go -C "$qs_repo_root" "${args[@]}"
done
