#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${SCRIPT_DIR}/local/logs"

RUNS="${1:-1}"
SUITE_FILE="${2:-sqltests/tpch_profile.yaml}"
MODES="${MODES:-inabox-direct inabox-standard}"
CASES="${CASES:-}"

HOST="${QUANTA_HOST:-127.0.0.1}"
USER="${QUANTA_USER:-qstream}"
DB="${QUANTA_DB:-quanta}"
DEFAULT_PORT="${QUANTA_PORT:-4000}"
STANDARD_PORT="${STANDARD_PORT:-4400}"
STANDARD_CONFIG_DIR="${TPCH_STANDARD_CONFIG_DIR:-${SCRIPT_DIR}/config}"
STANDARD_DATA_DIR="${TPCH_STANDARD_DATA_DIR:-${SCRIPT_DIR}/local/standard-data}"
START_STANDARD="${START_STANDARD:-1}"
KEEP_STANDARD="${KEEP_STANDARD:-0}"
EXIT_ON_FAILURE="${EXIT_ON_FAILURE:-0}"
STANDARD_READY_TIMEOUT_SECONDS="${STANDARD_READY_TIMEOUT_SECONDS:-240}"
STANDARD_PID=""
STANDARD_LOG=""

if ! [[ "${RUNS}" =~ ^[0-9]+$ ]] || [[ "${RUNS}" -lt 1 ]]; then
  echo "usage: $0 [runs] [suite-file]" >&2
  echo "runs must be a positive integer" >&2
  exit 2
fi

mkdir -p "${LOG_DIR}"

suite_name() {
  basename "$1" .yaml
}

latest_suite_log() {
  local suite="$1"
  local runs="$2"
  local mode="$3"
  local name label case_label
  name="$(suite_name "${suite}")"
  label="$(printf '%s' "${mode}" | tr -c '[:alnum:]_.-' '-')"
  case_label="all"
  if [[ -n "${CASES}" ]]; then
    case_label="$(printf '%s' "${CASES}" | tr ', ' '__' | tr -c '[:alnum:]_.-' '-')"
  fi
  ls -t "${LOG_DIR}/${name}-${label}-${case_label}-${runs}x-"*.log 2>/dev/null | head -1 || true
}

mode_list_contains() {
  local needle="$1"
  local mode
  for mode in ${MODES}; do
    if [[ "${mode}" == "${needle}" ]]; then
      return 0
    fi
  done
  return 1
}

wait_for_standard() {
  local i attempts
  attempts=$((STANDARD_READY_TIMEOUT_SECONDS * 4))
  for i in $(seq 1 "${attempts}"); do
    if [[ -n "${STANDARD_PID}" ]] && ! kill -0 "${STANDARD_PID}" >/dev/null 2>&1; then
      echo "inabox-standard server exited before readiness" >&2
      cat "${STANDARD_LOG}" >&2 || true
      exit 1
    fi
    if command -v mysqladmin >/dev/null 2>&1; then
      if mysqladmin --connect-timeout=1 -h "${HOST}" -P "${STANDARD_PORT}" -u "${USER}" ping >/dev/null 2>&1; then
        return 0
      fi
    elif (echo >/dev/tcp/"${HOST}"/"${STANDARD_PORT}") >/dev/null 2>&1; then
      sleep 0.25
      return 0
    fi
    sleep 0.25
  done
  echo "inabox-standard server did not become ready on ${HOST}:${STANDARD_PORT}" >&2
  cat "${STANDARD_LOG}" >&2 || true
  exit 1
}

start_standard_server() {
  if [[ "${START_STANDARD}" != "1" ]] || ! mode_list_contains inabox-standard; then
    return 0
  fi
  if [[ ! -d "${STANDARD_DATA_DIR}" ]]; then
    echo "standard data directory not found: ${STANDARD_DATA_DIR}" >&2
    echo "load data first with run-inabox-standard-tpch.sh" >&2
    exit 2
  fi
  local stamp
  stamp="$(date -u +%Y%m%d-%H%M%S)"
  STANDARD_LOG="${LOG_DIR}/tpch-mode-compare-standard-server-${stamp}.log"
  echo "starting inabox-standard server target=${HOST}:${STANDARD_PORT} log=${STANDARD_LOG}"
  (
    cd "${REPO_ROOT}"
    go run ./cmd/quantastream \
      -auth-mode permissive \
      -config-dir "${STANDARD_CONFIG_DIR}" \
      -data-dir "${STANDARD_DATA_DIR}" \
      -bind "${HOST}" \
      -mysql-port "${STANDARD_PORT}" \
      -database "${DB}"
  ) >"${STANDARD_LOG}" 2>&1 &
  STANDARD_PID="$!"
  wait_for_standard
}

cleanup() {
  if [[ "${KEEP_STANDARD}" == "1" ]]; then
    if [[ -n "${STANDARD_PID}" ]]; then
      echo "preserved inabox-standard server pid=${STANDARD_PID}"
    fi
    return 0
  fi
  if [[ -n "${STANDARD_PID}" ]] && kill -0 "${STANDARD_PID}" >/dev/null 2>&1; then
    kill "${STANDARD_PID}" >/dev/null 2>&1 || true
    wait "${STANDARD_PID}" >/dev/null 2>&1 || true
  fi
  return 0
}

on_exit() {
  local status="$?"
  trap - EXIT
  cleanup || true
  exit "${status}"
}
trap on_exit EXIT

run_mode() {
  local mode="$1"
  local port="${DEFAULT_PORT}"
  if [[ "${mode}" == "inabox-standard" ]]; then
    port="${STANDARD_PORT}"
  fi

  echo
  echo "===== TPCH profile mode=${mode} suite=${SUITE_FILE} runs=${RUNS} ====="
  (
    cd "${SCRIPT_DIR}" &&
      QUANTA_ENGINE="${mode}" \
      QUANTA_HOST="${HOST}" \
      QUANTA_PORT="${port}" \
      QUANTA_USER="${USER}" \
      QUANTA_DB="${DB}" \
      CASES="${CASES}" \
      ./run-tpch-suite.sh "${RUNS}" "${SUITE_FILE}"
  )
}

echo "TPC-H mode comparison"
echo "repo=${REPO_ROOT}"
echo "suite=${SUITE_FILE}"
echo "cases=${CASES:-all}"
echo "runs=${RUNS}"
echo "modes=${MODES}"
echo "host=${HOST}"
echo "default_port=${DEFAULT_PORT}"
echo "standard_port=${STANDARD_PORT}"
echo "standard_ready_timeout_seconds=${STANDARD_READY_TIMEOUT_SECONDS}"
echo

start_standard_server

logs=()
failures=0
for mode in ${MODES}; do
  before="$(latest_suite_log "${SUITE_FILE}" "${RUNS}" "${mode}")"
  run_mode "${mode}"
  status="$?"
  after="$(latest_suite_log "${SUITE_FILE}" "${RUNS}" "${mode}")"
  if [[ -z "${after}" || "${after}" == "${before}" ]]; then
    echo "mode ${mode} did not produce a new suite log" >&2
    failures=$((failures + 1))
    continue
  fi
  logs+=("${after}")
  echo "mode=${mode} status=${status} log=${after}"
  if [[ "${status}" -ne 0 ]]; then
    failures=$((failures + 1))
  fi
done

if [[ "${#logs[@]}" -ge 2 ]]; then
  echo
  "${SCRIPT_DIR}/compare-tpch-suite.py" "${logs[@]}"
else
  echo "not enough logs to compare" >&2
fi

if [[ "${EXIT_ON_FAILURE}" == "1" && "${failures}" -ne 0 ]]; then
  exit 1
fi
