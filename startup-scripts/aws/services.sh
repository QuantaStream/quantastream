#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$script_dir/lib.sh"

usage() {
  cat <<'EOF'
Usage: ./startup-scripts/aws/services.sh <start|stop|restart|status|logs> [--proxy-only|--nodes-only] [--lines=n]

Controls or inspects the distributed proxy and node systemd services across the
configured AWS fleet.
EOF
}

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  usage
  exit 0
fi

qs_load_env

action="$1"
shift
include_proxy=1
include_nodes=1
lines=120

for arg in "$@"; do
  case "$arg" in
    --proxy-only)
      include_nodes=0
      ;;
    --nodes-only)
      include_proxy=0
      ;;
    --lines=*)
      lines="${arg#--lines=}"
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

run_systemctl() {
  local service="$1"
  case "$action" in
    start|stop|restart)
      sudo systemctl "$action" "$service"
      ;;
    status)
      systemctl --no-pager --full status "$service" || true
      ;;
    logs)
      journalctl -u "$service" -n "$lines" --no-pager || true
      ;;
    *)
      echo "Unknown action: $action" >&2
      usage >&2
      exit 2
      ;;
  esac
}

if (( include_proxy )); then
  echo "== proxy: quantastream-proxy =="
  run_systemctl quantastream-proxy
fi

node_service_action() {
  local index="$1"
  local host="$2"
  local node_key="$3"
  echo
  echo "== node: $node_key ($host) =="
  case "$action" in
    start|stop|restart)
      qs_remote_bash "$host" "sudo systemctl '$action' quantastream-node"
      ;;
    status)
      qs_remote_bash "$host" "systemctl --no-pager --full status quantastream-node || true"
      ;;
    logs)
      qs_remote_bash "$host" "journalctl -u quantastream-node -n '$lines' --no-pager || true"
      ;;
  esac
}

if (( include_nodes )); then
  qs_each_node node_service_action
fi
