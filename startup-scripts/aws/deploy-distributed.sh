#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$script_dir/lib.sh"

usage() {
  cat <<'EOF'
Usage: ./startup-scripts/aws/deploy-distributed.sh [--no-pull] [--no-start] [--proxy-only|--nodes-only]

Deploys distributed QuantaStream services for the configured AWS fleet.
Run this from bench-runner after editing quantastream-aws.env.

Options:
  --no-pull    skip git pull --ff-only on bench-runner and each QS node.
  --no-start   install/enable services, but do not restart them now.
  --proxy-only deploy/restart only quantastream-proxy on bench-runner.
  --nodes-only deploy/restart only quantastream-node on QS nodes.
EOF
}

do_pull=1
enable_now=1
deploy_proxy=1
deploy_nodes=1
for arg in "$@"; do
  case "$arg" in
    --pull)
      do_pull=1
      ;;
    --no-pull)
      do_pull=0
      ;;
    --no-start)
      enable_now=0
      ;;
    --proxy-only)
      deploy_proxy=1
      deploy_nodes=0
      ;;
    --nodes-only)
      deploy_proxy=0
      deploy_nodes=1
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

if (( do_pull )); then
  echo "Updating bench repo"
  git -C "$qs_repo_root" pull --ff-only
fi

deploy_node() {
  local index="$1"
  local host="$2"
  local node_key="$3"

  echo "Deploying node $node_key on $host"
  local update_cmd=""
  if (( do_pull )); then
    update_cmd="git pull --ff-only && "
  fi
  qs_remote_bash "$host" "cd '$QS_REPO_DIR' && ${update_cmd}QUANTASTREAM_NODE_HASH_KEY='$node_key' QUANTASTREAM_DATA_DIR='$QS_NODE_DATA_DIR' QUANTASTREAM_NODE_PORT='$QS_NODE_PORT' QUANTASTREAM_CONSUL_ENDPOINT='$QS_NODE_CONSUL_ENDPOINT' QUANTASTREAM_REQUIRE_LOCAL_CONSUL='$QS_NODE_REQUIRE_LOCAL_CONSUL' QUANTASTREAM_SKIP_NODE_SYNC='$QS_SKIP_NODE_SYNC' QUANTASTREAM_REVERSE_ARTIFACT_WARM_MEMBERSHIP_TIMEOUT='$QS_REVERSE_ARTIFACT_WARM_MEMBERSHIP_TIMEOUT' ENABLE_NOW='$enable_now' sudo -E ./startup-scripts/install-distributed-node-service.sh"
}

if (( deploy_nodes )); then
  qs_each_node deploy_node
fi

if (( deploy_proxy )); then
  echo "Deploying proxy on bench-runner"
  QUANTASTREAM_BIND=0.0.0.0 \
  QUANTASTREAM_MYSQL_PORT="$QS_PROXY_MYSQL_PORT" \
  QUANTASTREAM_CONSUL_ENDPOINT="$QS_CONSUL_ENDPOINT" \
  QUANTASTREAM_NODE_PORT="$QS_NODE_PORT" \
  QUANTASTREAM_SCHEMA_DIR="$QS_SCHEMA_DIR" \
  QUANTASTREAM_DATABASE="$QS_DATABASE" \
  QUANTASTREAM_GRAPH_EQUALITY_ROLE_SEED="$QS_GRAPH_EQUALITY_ROLE_SEED" \
  ENABLE_NOW="$enable_now" \
    sudo -E "$qs_repo_root/startup-scripts/install-distributed-proxy-service.sh"
fi

echo "Setting cluster-size-target=$QS_CLUSTER_SIZE"
qs_set_cluster_size

if (( enable_now && deploy_nodes )); then
  echo "Waiting for distributed cluster"
  qs_wait_for_cluster 180
fi
