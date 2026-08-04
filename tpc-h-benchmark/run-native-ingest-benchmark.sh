#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"

ORDERS="${ORDERS:-100}"
LINEITEMS="${LINEITEMS:-4}"
SHARDS="${SHARDS:-1}"
RUNS="${RUNS:-3}"
PROFILE="${PROFILE:-standard-native-tpch-ingest}"
BENCHMARK_OUTPUT_DIR="${BENCHMARK_OUTPUT_DIR:-${SCRIPT_DIR}/local/ingest-benchmarks/${RUN_ID}}"

absolute_path() {
  local path="$1"
  if [[ "${path}" == /* ]]; then
    printf '%s' "${path}"
  else
    printf '%s/%s' "${SCRIPT_DIR}" "${path}"
  fi
}

positive_int() {
  local name="$1"
  local value="$2"
  if ! [[ "${value}" =~ ^[0-9]+$ ]] || [[ "${value}" -lt 1 ]]; then
    echo "${name} must be a positive integer: ${value}" >&2
    exit 2
  fi
}

safe_label() {
  printf '%s' "$1" | tr -c '[:alnum:]_.-' '-'
}

positive_int ORDERS "${ORDERS}"
positive_int LINEITEMS "${LINEITEMS}"
positive_int SHARDS "${SHARDS}"
positive_int RUNS "${RUNS}"

BENCHMARK_OUTPUT_DIR="$(absolute_path "${BENCHMARK_OUTPUT_DIR}")"
mkdir -p "${BENCHMARK_OUTPUT_DIR}"
SAFE_PROFILE="$(safe_label "${PROFILE}")"
BENCHMARK_REPORT="${BENCHMARK_REPORT:-${BENCHMARK_OUTPUT_DIR}/${SAFE_PROFILE}.json}"
LOG_FILE="${LOG_FILE:-${BENCHMARK_OUTPUT_DIR}/${SAFE_PROFILE}.log}"
BENCHMARK_REPORT="$(absolute_path "${BENCHMARK_REPORT}")"
LOG_FILE="$(absolute_path "${LOG_FILE}")"
mkdir -p "$(dirname "${LOG_FILE}")"

exec > >(tee "${LOG_FILE}") 2>&1

echo "Native ingest benchmark"
echo "timestamp_utc=${RUN_ID}"
echo "repo=${REPO_ROOT}"
echo "profile=${PROFILE}"
echo "orders=${ORDERS}"
echo "lineitems_per_order=${LINEITEMS}"
echo "shards=${SHARDS}"
echo "runs=${RUNS}"
echo "report=${BENCHMARK_REPORT}"
echo "log=${LOG_FILE}"
if [[ -n "${GOWORK:-}" ]]; then
  echo "gowork=${GOWORK}"
fi
if git -C "${REPO_ROOT}" rev-parse --short HEAD >/dev/null 2>&1; then
  echo "git_commit=$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
  echo "git_branch=$(git -C "${REPO_ROOT}" branch --show-current)"
fi
echo

cd "${REPO_ROOT}"
QUANTASTREAM_TPCH_INGEST_BENCH_ORDERS="${ORDERS}" \
QUANTASTREAM_TPCH_INGEST_BENCH_LINEITEMS="${LINEITEMS}" \
QUANTASTREAM_TPCH_INGEST_BENCH_SHARDS="${SHARDS}" \
QUANTASTREAM_TPCH_INGEST_BENCH_PROFILE="${PROFILE}" \
QUANTASTREAM_TPCH_INGEST_BENCH_REPORT="${BENCHMARK_REPORT}" \
  go test ./qsinabox \
    -run '^$' \
    -bench '^BenchmarkStandardProcessNativeGRPCRouterTPCHNestedIngest$' \
    -benchtime="${RUNS}x" \
    -count=1 \
    "$@"

echo
echo "native ingest benchmark complete report=${BENCHMARK_REPORT} log=${LOG_FILE}"
