#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$script_dir/lib.sh"

wait_seconds=0
for arg in "$@"; do
  case "$arg" in
    --wait)
      wait_seconds=120
      ;;
    --wait=*)
      wait_seconds="${arg#--wait=}"
      ;;
    -h|--help)
      cat <<'EOF'
Usage: ./startup-scripts/aws/cluster-health.sh [--wait[=seconds]]

Shows Consul membership, configured cluster size, Quanta admin status, and
systemd state for the distributed proxy and nodes.
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

echo "== Consul members =="
consul members || true

echo
echo "== Quanta cluster target =="
go -C "$qs_repo_root" run ./quanta-admin \
  --consul-addr "$QS_CONSUL_ENDPOINT" \
  --port "$QS_NODE_PORT" \
  config --key cluster-size-target || true

echo
echo "== Quanta cluster status =="
if [[ "$wait_seconds" != "0" ]]; then
  qs_wait_for_cluster "$wait_seconds"
else
  qs_admin_status || true
fi

echo
echo "== Proxy service =="
systemctl --no-pager --full status quantastream-proxy || true

node_service_status() {
  local index="$1"
  local host="$2"
  local node_key="$3"
  echo
  echo "== Node service: $node_key ($host) =="
  qs_remote_bash "$host" "systemctl --no-pager --full status quantastream-node || true"
}

qs_each_node node_service_status
