#!/usr/bin/env bash
set -euo pipefail

SUITE_FILE="${SUITE_FILE:-sqltests/inabox_direct_tpch_kernels.yaml}"
ENGINE="${ENGINE:-inabox-direct}"
BENCHMARK_PROFILE="${BENCHMARK_PROFILE:-developer-local}"
BENCHMARK_REPORT="${BENCHMARK_REPORT:-expected/local/benchmark-$(date -u +%Y%m%dT%H%M%SZ).json}"
BENCHMARK_WARMUP="${BENCHMARK_WARMUP:-0}"
BENCHMARK_RUNS="${BENCHMARK_RUNS:-3}"
BENCHMARK_METADATA="${BENCHMARK_METADATA:-}"

mkdir -p "$(dirname "$BENCHMARK_REPORT")"

args=(
  -engine "$ENGINE"
  -suite_file "$SUITE_FILE"
  -benchmark_profile "$BENCHMARK_PROFILE"
  -benchmark_warmup "$BENCHMARK_WARMUP"
  -benchmark_runs "$BENCHMARK_RUNS"
  -benchmark_report "$BENCHMARK_REPORT"
)

if [[ -n "$BENCHMARK_METADATA" ]]; then
  args+=( -benchmark_metadata "$BENCHMARK_METADATA" )
fi

exec go run . "${args[@]}" "$@"
