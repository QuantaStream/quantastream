#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ./run-profile-capture.sh
  CASE=tpch_profile.q5.formal_revenue ./run-profile-capture.sh

Environment:
  SUITE             TPC-H SQLRunner profile suite. Defaults to sqltests/tpch_profile.yaml.
  CASE              Optional exact SQLRunner case id.
  ENGINE            QuantaStream socket engine. Defaults to inabox-standard.
  HOST              QuantaStream host. Defaults to QUANTA_HOST or 127.0.0.1.
  PORT              QuantaStream port. Defaults to QUANTA_PORT or 4000.
  USER              QuantaStream user. Defaults to QUANTA_USER or qstream.
  PASSWORD          QuantaStream password. Defaults to QUANTA_PASSWORD or empty.
  DB                QuantaStream database. Defaults to QUANTA_DB or quanta.
  CONSUL            Consul address for engines that need it. Defaults to QUANTA_CONSUL or 127.0.0.1:8500.
  LOG_DIR           Output log directory. Defaults to tpc-h-benchmark/local/logs.
  REPORT_DIR        Output JSON profile report directory. Defaults to tpc-h-benchmark/local/profile-reports.
  REPORT_FILE       Optional exact JSON profile report path.
  BENCHMARK_PROFILE Profile label recorded in the JSON report. Defaults to profile-capture.
  BENCHMARK_WARMUP  Warm-up suite runs before measured profile capture. Defaults to 0.
  BENCHMARK_RUNS    Measured suite runs recorded in the JSON report. Defaults to 1.
  BENCHMARK_METADATA
                    Optional comma-separated key=value metadata recorded in the JSON report.
  VERBOSE           Set to 0 to suppress verbose SQL/profile output. Defaults to 1.
  SLOW_THRESHOLD    Optional SQLRunner slow-case summary threshold, such as 2s.
  GOWORK            Optional Go workspace overlay inherited by go run.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQLRUNNER_DIR="${REPO_ROOT}/sqlrunner"

SUITE="${SUITE:-sqltests/tpch_profile.yaml}"
CASE="${CASE:-}"
ENGINE="${ENGINE:-inabox-standard}"
HOST="${HOST:-${QUANTA_HOST:-127.0.0.1}}"
PORT="${PORT:-${QUANTA_PORT:-4000}}"
USER="${USER:-${QUANTA_USER:-qstream}}"
PASSWORD="${PASSWORD:-${QUANTA_PASSWORD:-}}"
DB="${DB:-${QUANTA_DB:-quanta}}"
CONSUL="${CONSUL:-${QUANTA_CONSUL:-127.0.0.1:8500}}"
LOG_DIR="${LOG_DIR:-${SCRIPT_DIR}/local/logs}"
REPORT_DIR="${REPORT_DIR:-${SCRIPT_DIR}/local/profile-reports}"
BENCHMARK_PROFILE="${BENCHMARK_PROFILE:-profile-capture}"
BENCHMARK_WARMUP="${BENCHMARK_WARMUP:-0}"
BENCHMARK_RUNS="${BENCHMARK_RUNS:-1}"
BENCHMARK_METADATA="${BENCHMARK_METADATA:-}"
VERBOSE="${VERBOSE:-1}"
SLOW_THRESHOLD="${SLOW_THRESHOLD:-}"

if [[ "${SUITE}" = /* ]]; then
  SUITE_ARG="${SUITE}"
  SUITE_NAME="$(basename "${SUITE}" .yaml)"
else
  SUITE_ARG="../tpc-h-benchmark/${SUITE}"
  SUITE_NAME="$(basename "${SUITE}" .yaml)"
fi

mkdir -p "${LOG_DIR}"
mkdir -p "${REPORT_DIR}"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
ENGINE_LABEL="$(printf '%s' "${ENGINE}" | tr -c '[:alnum:]_.-' '-')"
CASE_LABEL="all"
if [[ -n "${CASE}" ]]; then
  CASE_LABEL="$(printf '%s' "${CASE}" | tr -c '[:alnum:]_.-' '-')"
fi
LOG_FILE="${LOG_DIR}/${SUITE_NAME}-${ENGINE_LABEL}-${CASE_LABEL}-profile-${STAMP}.log"
REPORT_FILE="${REPORT_FILE:-${REPORT_DIR}/${SUITE_NAME}-${ENGINE_LABEL}-${CASE_LABEL}-profile-${STAMP}.json}"

args=(
  -engine "${ENGINE}"
  -suite_file "${SUITE_ARG}"
  -host "${HOST}"
  -port "${PORT}"
  -user "${USER}"
  -password "${PASSWORD}"
  -db "${DB}"
  -consul "${CONSUL}"
  -capture_profile
  -precise_timing
  -benchmark_report "${REPORT_FILE}"
  -benchmark_warmup "${BENCHMARK_WARMUP}"
  -benchmark_runs "${BENCHMARK_RUNS}"
  -benchmark_profile "${BENCHMARK_PROFILE}"
)

if [[ "${VERBOSE}" != "0" ]]; then
  args+=(-verbose)
fi
if [[ -n "${CASE}" ]]; then
  args+=(-case "${CASE}")
fi
if [[ -n "${SLOW_THRESHOLD}" ]]; then
  args+=(-slow_threshold "${SLOW_THRESHOLD}")
fi
if [[ -n "${BENCHMARK_METADATA}" ]]; then
  args+=(-benchmark_metadata "${BENCHMARK_METADATA}")
fi

{
  echo "TPC-H profile capture"
  echo "timestamp_utc=${STAMP}"
  echo "repo=${REPO_ROOT}"
  echo "suite=${SUITE}"
  echo "case=${CASE:-all}"
  echo "engine=${ENGINE}"
  echo "host=${HOST}"
  echo "port=${PORT}"
  echo "db=${DB}"
  echo "benchmark_warmup=${BENCHMARK_WARMUP}"
  echo "benchmark_runs=${BENCHMARK_RUNS}"
  echo "report=${REPORT_FILE}"
  if [[ -n "${GOWORK:-}" ]]; then
    echo "gowork=${GOWORK}"
  fi
  if git -C "${REPO_ROOT}" rev-parse --short HEAD >/dev/null 2>&1; then
    echo "git_commit=$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
    echo "git_branch=$(git -C "${REPO_ROOT}" branch --show-current)"
  fi
  echo
} | tee "${LOG_FILE}"

(cd "${SQLRUNNER_DIR}" && go run . "${args[@]}") 2>&1 | tee -a "${LOG_FILE}"

echo "profile capture log=${LOG_FILE}" | tee -a "${LOG_FILE}"
echo "profile capture report=${REPORT_FILE}" | tee -a "${LOG_FILE}"
