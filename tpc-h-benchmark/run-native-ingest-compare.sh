#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"

BASELINE="${1:-${BASELINE:-}}"
TARGET="${2:-${TARGET:-}}"
OUTPUT="${3:-${OUTPUT:-}}"
BENCHMARK_OUTPUT_DIR="${BENCHMARK_OUTPUT_DIR:-${SCRIPT_DIR}/local/ingest-benchmarks/${RUN_ID}}"

absolute_path() {
  local path="$1"
  if [[ "${path}" == /* ]]; then
    printf '%s' "${path}"
  else
    printf '%s/%s' "${SCRIPT_DIR}" "${path}"
  fi
}

if [[ -z "${BASELINE}" || -z "${TARGET}" ]]; then
  echo "usage: $0 <baseline-report.json> <target-report.json> [comparison.md]" >&2
  echo "or set BASELINE=... TARGET=... [OUTPUT=...]" >&2
  exit 2
fi

BASELINE="$(absolute_path "${BASELINE}")"
TARGET="$(absolute_path "${TARGET}")"

if [[ -z "${OUTPUT}" ]]; then
  BENCHMARK_OUTPUT_DIR="$(absolute_path "${BENCHMARK_OUTPUT_DIR}")"
  mkdir -p "${BENCHMARK_OUTPUT_DIR}"
  OUTPUT="${BENCHMARK_OUTPUT_DIR}/comparison.md"
else
  OUTPUT="$(absolute_path "${OUTPUT}")"
fi

echo "Native ingest benchmark comparison"
echo "timestamp_utc=${RUN_ID}"
echo "repo=${REPO_ROOT}"
echo "baseline=${BASELINE}"
echo "target=${TARGET}"
echo "output=${OUTPUT}"
echo

cd "${REPO_ROOT}"
go run ./cmd/ingest-benchmark-compare \
  -baseline "${BASELINE}" \
  -target "${TARGET}" \
  -out "${OUTPUT}"

echo
cat "${OUTPUT}"
echo
echo "native ingest comparison written to ${OUTPUT}"
