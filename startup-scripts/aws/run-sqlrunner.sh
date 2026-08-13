#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$script_dir/lib.sh"

suite_file="../tpc-h-benchmark/sqltests/tpch_profile_scale.yaml"
case_id=""
profile="aws-distributed-$(date +%Y%m%d-%H%M%S)"
report="/tmp/${profile}.json"
explicit_runs=""
warmup=0
summary=0

usage() {
  cat <<'EOF'
Usage: ./startup-scripts/aws/run-sqlrunner.sh [options]

Runs SQLRunner against the distributed cluster through Consul direct mode.

Options:
  --suite=path       SQLRunner suite path. Defaults to tpch_profile_scale.yaml.
  --case=id          Optional single case id.
  --profile=name     Benchmark profile name.
  --report=path      Benchmark report path. Defaults to /tmp/<profile>.json.
  --runs=n           Measured runs. Defaults to QS_BENCHMARK_RUNS.
  --warmup=n         Warmup runs. Defaults to 0.
  --summary          Print benchmark summary after the run.
EOF
}

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
    --case=*)
      case_id="${arg#--case=}"
      ;;
    --case)
      case_id="${1:?--case requires a value}"
      shift
      ;;
    --profile=*)
      profile="${arg#--profile=}"
      report="/tmp/${profile}.json"
      ;;
    --profile)
      profile="${1:?--profile requires a value}"
      report="/tmp/${profile}.json"
      shift
      ;;
    --report=*)
      report="${arg#--report=}"
      ;;
    --report)
      report="${1:?--report requires a value}"
      shift
      ;;
    --runs=*)
      explicit_runs="${arg#--runs=}"
      ;;
    --runs)
      explicit_runs="${1:?--runs requires a value}"
      shift
      ;;
    --warmup=*)
      warmup="${arg#--warmup=}"
      ;;
    --warmup)
      warmup="${1:?--warmup requires a value}"
      shift
      ;;
    --summary)
      summary=1
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
runs="${explicit_runs:-$QS_BENCHMARK_RUNS}"

qs_wait_for_cluster 120 >/dev/null

args=(
  run .
  -engine inabox-direct
  -suite_file "$suite_file"
  -consul "$QS_CONSUL_ENDPOINT"
  -precise_timing
  -capture_profile
  -benchmark_report "$report"
  -benchmark_runs "$runs"
  -benchmark_profile "$profile"
)
if [[ -n "$case_id" ]]; then
  args+=(-case "$case_id")
fi
if [[ "$warmup" != "0" ]]; then
  args+=(-benchmark_warmup "$warmup")
fi

cd "$qs_repo_root/sqlrunner"
go "${args[@]}"

if (( summary )); then
  go run . -benchmark_summary "$report"
fi
