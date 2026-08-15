#!/usr/bin/env bash

set -Eeuo pipefail

qs_aws_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
qs_repo_root="$(cd -- "$qs_aws_script_dir/../.." && pwd)"

qs_load_env() {
  local env_file="${QS_AWS_ENV:-$qs_aws_script_dir/quantastream-aws.env}"
  if [[ -f "$env_file" ]]; then
    # shellcheck source=/dev/null
    source "$env_file"
  elif [[ -f "$qs_aws_script_dir/quantastream-aws.env.example" ]]; then
    echo "Using defaults from $qs_aws_script_dir/quantastream-aws.env.example" >&2
    # shellcheck source=/dev/null
    source "$qs_aws_script_dir/quantastream-aws.env.example"
  else
    echo "Missing AWS env file: $env_file" >&2
    exit 2
  fi

  QS_REPO_DIR="${QS_REPO_DIR:-/home/ubuntu/quantastream}"
  QS_SSH_USER="${QS_SSH_USER:-ubuntu}"
  QS_SSH_OPTS="${QS_SSH_OPTS:--o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10}"
  QS_SSH_IDENTITY_FILE="${QS_SSH_IDENTITY_FILE:-}"
  QS_SSH_BASTION_HOST="${QS_SSH_BASTION_HOST:-}"
  QS_SSH_BASTION_USER="${QS_SSH_BASTION_USER:-$QS_SSH_USER}"
  QS_CONSUL_ENDPOINT="${QS_CONSUL_ENDPOINT:-127.0.0.1:8500}"
  QS_NODE_CONSUL_ENDPOINT="${QS_NODE_CONSUL_ENDPOINT:-127.0.0.1:8500}"
  QS_NODE_REQUIRE_LOCAL_CONSUL="${QS_NODE_REQUIRE_LOCAL_CONSUL:-auto}"
  QS_CLUSTER_SIZE="${QS_CLUSTER_SIZE:-${#QS_NODE_HOSTS[@]}}"
  QS_NODE_PORT="${QS_NODE_PORT:-4400}"
  QS_PROXY_MYSQL_PORT="${QS_PROXY_MYSQL_PORT:-4000}"
  QS_NODE_DATA_DIR="${QS_NODE_DATA_DIR:-$QS_REPO_DIR/tpc-h-benchmark/local/standard-data}"
  QS_SKIP_NODE_SYNC="${QS_SKIP_NODE_SYNC:-1}"
  QS_GRAPH_EQUALITY_ROLE_SEED="${QS_GRAPH_EQUALITY_ROLE_SEED:-0}"
  QS_SCHEMA_DIR="${QS_SCHEMA_DIR:-$QS_REPO_DIR/tpc-h-benchmark/config}"
  QS_DATABASE="${QS_DATABASE:-quanta}"
  QS_TPCH_DATA_DIR="${QS_TPCH_DATA_DIR:-$QS_REPO_DIR/tpc-h-benchmark/local/data/sf-0.05}"
  QS_TPCH_LOAD_WORKERS="${QS_TPCH_LOAD_WORKERS:-3}"
  QS_TPCH_LOAD_BATCH_SIZE="${QS_TPCH_LOAD_BATCH_SIZE:-1000}"
  QS_BENCHMARK_RUNS="${QS_BENCHMARK_RUNS:-3}"
  QS_BENCHMARK_WARMUP="${QS_BENCHMARK_WARMUP:-1}"

  if [[ "${#QS_NODE_HOSTS[@]}" -eq 0 ]]; then
    echo "QS_NODE_HOSTS is empty" >&2
    exit 2
  fi
  if [[ "${#QS_NODE_KEYS[@]}" -ne "${#QS_NODE_HOSTS[@]}" ]]; then
    echo "QS_NODE_KEYS must have the same length as QS_NODE_HOSTS" >&2
    exit 2
  fi
}

qs_bool() {
  [[ "${1:-}" =~ ^(1|true|TRUE|yes|YES|on|ON)$ ]]
}

qs_repo_cmd() {
  printf 'cd %q && %s' "$QS_REPO_DIR" "$1"
}

qs_ssh() {
  local host="$1"
  shift
  local ssh_args=()
  if [[ -n "$QS_SSH_OPTS" ]]; then
    # QS_SSH_OPTS is intentionally split into ssh argv words.
    # shellcheck disable=SC2206
    ssh_args+=($QS_SSH_OPTS)
  fi
  if [[ -n "$QS_SSH_IDENTITY_FILE" ]]; then
    ssh_args+=(-i "$QS_SSH_IDENTITY_FILE")
  fi
  if [[ -n "$QS_SSH_BASTION_HOST" && "$host" != "$QS_SSH_BASTION_HOST" && "$host" != "localhost" && "$host" != "127.0.0.1" ]]; then
    local bastion="$QS_SSH_BASTION_HOST"
    if [[ "$bastion" != *@* ]]; then
      bastion="$QS_SSH_BASTION_USER@$bastion"
    fi
    ssh_args+=(-J "$bastion")
  fi
  # QS_SSH_OPTS is intentionally split into ssh argv words.
  ssh "${ssh_args[@]}" "$QS_SSH_USER@$host" "$@"
}

qs_remote_bash() {
  local host="$1"
  shift
  qs_ssh "$host" "bash -lc $(printf '%q' "$*")"
}

qs_each_node() {
  local fn="$1"
  shift
  local i
  for i in "${!QS_NODE_HOSTS[@]}"; do
    "$fn" "$i" "${QS_NODE_HOSTS[$i]}" "${QS_NODE_KEYS[$i]}" "$@"
  done
}

qs_set_cluster_size() {
  go -C "$qs_repo_root" run ./quanta-admin \
    --consul-addr "$QS_CONSUL_ENDPOINT" \
    --port "$QS_NODE_PORT" \
    config --key cluster-size-target --value "$QS_CLUSTER_SIZE"
}

qs_admin_status() {
  go -C "$qs_repo_root" run ./quanta-admin \
    --consul-addr "$QS_CONSUL_ENDPOINT" \
    --port "$QS_NODE_PORT" \
    status
}

qs_wait_for_cluster() {
  local wait_seconds="${1:-120}"
  local deadline=$(( $(date +%s) + wait_seconds ))
  local status
  while true; do
    status="$(qs_admin_status 2>&1 || true)"
    if grep -q "Cluster State = GREEN, Active nodes = $QS_CLUSTER_SIZE, Target Cluster Size = $QS_CLUSTER_SIZE" <<<"$status"; then
      echo "$status"
      return 0
    fi
    if (( $(date +%s) >= deadline )); then
      echo "Timed out waiting for GREEN cluster:" >&2
      echo "$status" >&2
      return 1
    fi
    sleep 2
  done
}
