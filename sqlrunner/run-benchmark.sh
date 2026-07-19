#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/benchmark_metadata.sh"

SUITE_FILE="${SUITE_FILE:-../tpc-h-benchmark/sqltests/tpch_benchmark_readonly.yaml}"
ENGINE="${ENGINE:-inabox-direct}"
BENCHMARK_PROFILE="${BENCHMARK_PROFILE:-developer-local}"
BENCHMARK_REPORT="${BENCHMARK_REPORT:-expected/local/benchmark-$(date -u +%Y%m%dT%H%M%SZ).json}"
BENCHMARK_WARMUP="${BENCHMARK_WARMUP:-0}"
BENCHMARK_RUNS="${BENCHMARK_RUNS:-3}"
BENCHMARK_METADATA="$(benchmark_metadata_join "$(benchmark_base_metadata "$SUITE_FILE" "$ENGINE")" "${BENCHMARK_METADATA:-}")"

cd "$SCRIPT_DIR"
mkdir -p "$(dirname "$BENCHMARK_REPORT")"

args=(
  -engine "$ENGINE"
  -suite_file "$SUITE_FILE"
  -benchmark_profile "$BENCHMARK_PROFILE"
  -benchmark_warmup "$BENCHMARK_WARMUP"
  -benchmark_runs "$BENCHMARK_RUNS"
  -benchmark_report "$BENCHMARK_REPORT"
  -benchmark_metadata "$BENCHMARK_METADATA"
)

exec go run . "${args[@]}" "$@"
