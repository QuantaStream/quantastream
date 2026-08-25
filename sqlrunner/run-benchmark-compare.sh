#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/benchmark_metadata.sh"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"

SUITE_FILE="${SUITE_FILE:-../tpc-h-benchmark/sqltests/tpch_benchmark_readonly.yaml}"
ENGINES="${ENGINES:-inabox-direct inabox-standard}"
BENCHMARK_PROFILE="${BENCHMARK_PROFILE:-developer-local}"
BENCHMARK_WARMUP="${BENCHMARK_WARMUP:-0}"
BENCHMARK_RUNS="${BENCHMARK_RUNS:-3}"
BENCHMARK_LIMIT="${BENCHMARK_LIMIT:-20}"
BENCHMARK_METADATA="${BENCHMARK_METADATA:-}"
BENCHMARK_OUTPUT_DIR="${BENCHMARK_OUTPUT_DIR:-${SCRIPT_DIR}/expected/local/benchmarks/${RUN_ID}}"

HOST="${QUANTA_HOST:-127.0.0.1}"
PORT="${QUANTA_PORT:-4000}"
USER="${QUANTA_USER:-qstream}"
DB="${QUANTA_DB:-quanta}"
CONSUL="${QUANTA_CONSUL:-127.0.0.1:8500}"

mkdir -p "$BENCHMARK_OUTPUT_DIR"

reports=()
for engine in $ENGINES; do
  safe_engine="$(printf '%s' "$engine" | tr -c '[:alnum:]_.-' '-')"
  report="${BENCHMARK_OUTPUT_DIR}/${safe_engine}.json"
  args=(
    -engine "$engine"
    -suite_file "$SUITE_FILE"
    -host "$HOST"
    -port "$PORT"
    -user "$USER"
    -db "$DB"
    -consul "$CONSUL"
    -benchmark_profile "$BENCHMARK_PROFILE"
    -benchmark_warmup "$BENCHMARK_WARMUP"
    -benchmark_runs "$BENCHMARK_RUNS"
    -benchmark_report "$report"
    -precise_timing
  )
  metadata="$(benchmark_metadata_join "$(benchmark_base_metadata "$SUITE_FILE" "$engine" "$HOST" "$PORT")" "$BENCHMARK_METADATA")"
  args+=( -benchmark_metadata "$metadata" )

  echo "===== benchmark engine=${engine} report=${report} ====="
  (cd "$SCRIPT_DIR" && go run . "${args[@]}")
  reports+=( "$report" )
done

compare_arg="$(IFS=,; echo "${reports[*]}")"
summary="${BENCHMARK_OUTPUT_DIR}/comparison.md"
(cd "$SCRIPT_DIR" && go run . -benchmark_compare "$compare_arg" -benchmark_limit "$BENCHMARK_LIMIT") | tee "$summary"
echo "benchmark comparison written to ${summary}"
