#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$script_dir/lib.sh"

usage() {
  cat <<'EOF'
Usage: ./startup-scripts/aws/reset-distributed-data.sh --confirm [--no-start] [--skip-health]

Stops the distributed proxy and node services, removes the configured
distributed node data directory contents on every configured node, then starts
the services and waits for an empty GREEN cluster.

Options:
  --confirm      required; acknowledge destructive deletion of node data.
  --no-start     stop and clear data, but leave services stopped.
  --skip-health  skip the final GREEN cluster wait.
EOF
}

confirmed=0
start_after=1
check_health=1

for arg in "$@"; do
  case "$arg" in
    --confirm)
      confirmed=1
      ;;
    --no-start)
      start_after=0
      check_health=0
      ;;
    --skip-health)
      check_health=0
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

if (( ! confirmed )); then
  usage >&2
  echo >&2
  echo "Refusing to delete distributed data without --confirm." >&2
  exit 2
fi

validate_data_dir() {
  local data_dir="$1"
  case "$data_dir" in
    ""|"/"|"/home"|"/home/"|"/home/ubuntu"|"/home/ubuntu/"|"/tmp"|"/tmp/")
      echo "Unsafe QS_NODE_DATA_DIR: $data_dir" >&2
      exit 2
      ;;
  esac
  if [[ "$data_dir" != */distributed-data && "$data_dir" != */distributed-data/ ]]; then
    echo "Refusing to reset non-distributed data dir: $data_dir" >&2
    echo "Set QS_NODE_DATA_DIR to a path ending in distributed-data." >&2
    exit 2
  fi
}

validate_data_dir "$QS_NODE_DATA_DIR"

echo "Resetting distributed data"
echo "data_dir=$QS_NODE_DATA_DIR"
echo "nodes=${QS_NODE_HOSTS[*]}"
echo

echo "Stopping proxy"
sudo systemctl stop quantastream-proxy || true

stop_and_clear_node() {
  local index="$1"
  local host="$2"
  local node_key="$3"

  echo
  echo "== reset node: $node_key ($host) =="
  qs_remote_bash "$host" "
    set -Eeuo pipefail
    data_dir='$QS_NODE_DATA_DIR'
    case \"\$data_dir\" in
      ''|'/'|'/home'|'/home/'|'/home/ubuntu'|'/home/ubuntu/'|'/tmp'|'/tmp/')
        echo \"Unsafe data_dir=\$data_dir\" >&2
        exit 2
        ;;
    esac
    if [[ \"\$data_dir\" != */distributed-data && \"\$data_dir\" != */distributed-data/ ]]; then
      echo \"Refusing to reset non-distributed data dir: \$data_dir\" >&2
      exit 2
    fi
    sudo systemctl stop quantastream-node || true
    mkdir -p \"\$data_dir\"
    shopt -s dotglob nullglob
    rm -rf -- \"\$data_dir\"/*
    echo \"cleared \$data_dir\"
  "
}

qs_each_node stop_and_clear_node

if (( ! start_after )); then
  echo
  echo "Services left stopped (--no-start)."
  exit 0
fi

start_node() {
  local index="$1"
  local host="$2"
  local node_key="$3"

  echo
  echo "== start node: $node_key ($host) =="
  qs_remote_bash "$host" "sudo systemctl start quantastream-node"
}

qs_each_node start_node

echo
echo "Starting proxy"
sudo systemctl start quantastream-proxy

echo "Setting cluster-size-target=$QS_CLUSTER_SIZE"
qs_set_cluster_size

if (( check_health )); then
  echo "Waiting for distributed cluster"
  qs_wait_for_cluster 180
fi
