#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"

ORDERS="${ORDERS:-1000}"
LINEITEMS="${LINEITEMS:-4}"
SHARDS="${SHARDS:-1}"
RUNS="${RUNS:-3}"
BASE_PROFILE="${BASE_PROFILE:-pk-verify-existing}"
TARGET_PROFILE="${TARGET_PROFILE:-pk-assume-new}"
BENCHMARK_OUTPUT_DIR="${BENCHMARK_OUTPUT_DIR:-${SCRIPT_DIR}/local/ingest-benchmarks/pk-mode-compare-${RUN_ID}}"

absolute_path() {
  local path="$1"
  if [[ "${path}" == /* ]]; then
    printf '%s' "${path}"
  else
    printf '%s/%s' "${SCRIPT_DIR}" "${path}"
  fi
}

BENCHMARK_OUTPUT_DIR="$(absolute_path "${BENCHMARK_OUTPUT_DIR}")"
mkdir -p "${BENCHMARK_OUTPUT_DIR}"

BASELINE_REPORT="${BENCHMARK_OUTPUT_DIR}/${BASE_PROFILE}.json"
TARGET_REPORT="${BENCHMARK_OUTPUT_DIR}/${TARGET_PROFILE}.json"
COMPARISON_REPORT="${BENCHMARK_OUTPUT_DIR}/comparison.md"

echo "Native ingest primary-key mode comparison"
echo "timestamp_utc=${RUN_ID}"
echo "orders=${ORDERS}"
echo "lineitems_per_order=${LINEITEMS}"
echo "shards=${SHARDS}"
echo "runs=${RUNS}"
echo "output_dir=${BENCHMARK_OUTPUT_DIR}"
echo

ORDERS="${ORDERS}" \
LINEITEMS="${LINEITEMS}" \
SHARDS="${SHARDS}" \
RUNS="${RUNS}" \
PROFILE="${BASE_PROFILE}" \
PRIMARY_KEY_MODE=verify_existing \
BENCHMARK_OUTPUT_DIR="${BENCHMARK_OUTPUT_DIR}" \
BENCHMARK_REPORT="${BASELINE_REPORT}" \
  "${SCRIPT_DIR}/run-native-ingest-benchmark.sh" "$@"

echo

ORDERS="${ORDERS}" \
LINEITEMS="${LINEITEMS}" \
SHARDS="${SHARDS}" \
RUNS="${RUNS}" \
PROFILE="${TARGET_PROFILE}" \
PRIMARY_KEY_MODE=assume_new \
BENCHMARK_OUTPUT_DIR="${BENCHMARK_OUTPUT_DIR}" \
BENCHMARK_REPORT="${TARGET_REPORT}" \
  "${SCRIPT_DIR}/run-native-ingest-benchmark.sh" "$@"

echo

"${SCRIPT_DIR}/run-native-ingest-compare.sh" \
  "${BASELINE_REPORT}" \
  "${TARGET_REPORT}" \
  "${COMPARISON_REPORT}"
