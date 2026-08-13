#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$script_dir/lib.sh"

suite_file=""
profile=""
report=""
while [[ $# -gt 0 ]]; do
  arg="$1"
  shift
  case "$arg" in
    --suite=*)
      suite_file="${arg#--suite=}"
      ;;
    --suite)
      suite_file="${1:?--suite requires a value}"
      shift
      ;;
    --profile=*)
      profile="${arg#--profile=}"
      ;;
    --profile)
      profile="${1:?--profile requires a value}"
      shift
      ;;
    --report=*)
      report="${arg#--report=}"
      ;;
    --report)
      report="${1:?--report requires a value}"
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: ./startup-scripts/aws/run-readonly-benchmark.sh [--profile=name] [--report=/tmp/file.json] [--suite=path]

Runs the compact read-only SQLRunner suite against the distributed cluster via
Consul direct mode.
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

suite_file="${suite_file:-${QS_READONLY_SUITE:-../tpc-h-benchmark/sqltests/tpch_benchmark_readonly.yaml}}"
profile="${profile:-${QS_BENCHMARK_PROFILE:-local-cluster-sf005-readonly-$(date +%Y%m%d-%H%M%S)}}"
report="${report:-${QS_BENCHMARK_REPORT:-/tmp/${profile}.json}}"

exec "$script_dir/run-sqlrunner.sh" \
  --suite="$suite_file" \
  --profile="$profile" \
  --report="$report" \
  --warmup="$QS_BENCHMARK_WARMUP" \
  --runs="$QS_BENCHMARK_RUNS"
