#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$script_dir/lib.sh"

usage() {
  cat <<'EOF'
Usage: ./startup-scripts/aws/git-pull-nodes.sh [--include-bench]

Runs git pull --ff-only on the configured QS data nodes. Add --include-bench to
also update the local bench-runner checkout.
EOF
}

include_bench=0
for arg in "$@"; do
  case "$arg" in
    --include-bench)
      include_bench=1
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

if (( include_bench )); then
  echo "== bench-runner =="
  git -C "$qs_repo_root" pull --ff-only
  git -C "$qs_repo_root" status --short
fi

pull_node() {
  local index="$1"
  local host="$2"
  local node_key="$3"
  echo
  echo "== node: $node_key ($host) =="
  qs_remote_bash "$host" "hostname; git -C '$QS_REPO_DIR' pull --ff-only; git -C '$QS_REPO_DIR' status --short"
}

qs_each_node pull_node
