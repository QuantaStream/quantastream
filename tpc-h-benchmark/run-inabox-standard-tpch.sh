#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQLRUNNER_DIR="${REPO_ROOT}/sqlrunner"

DATA_DIR="${1:-local/data/sf-0.01}"
WORKERS="${2:-1}"
BATCH_SIZE="${3:-1000}"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-4400}"
SQL_USER="${SQL_USER:-${QUANTA_USER:-MOLIG004}}"
DB="${DB:-${QUANTA_DB:-quanta}}"
CONFIG_DIR="${TPCH_STANDARD_CONFIG_DIR:-${SCRIPT_DIR}/config}"
STANDARD_DATA_DIR="${TPCH_STANDARD_DATA_DIR:-${SCRIPT_DIR}/local/standard-data}"
LOG_DIR="${LOG_DIR:-${SCRIPT_DIR}/local/logs}"
RUN_LOAD="${RUN_LOAD:-1}"
RUN_COUNTS="${RUN_COUNTS:-1}"
RUN_SMOKE="${RUN_SMOKE:-1}"
CLEAN_DATA="${CLEAN_DATA:-1}"
START_SERVER="${START_SERVER:-1}"
KEEP_SERVER="${KEEP_SERVER:-0}"
SMOKE_SUITE="${SMOKE_SUITE:-sqltests/tpch_smoke.yaml}"
SERVER_PID=""

TABLES=(
  region
  nation
  customer
  part
  supplier
  partsupp
  orders
  lineitem
)

if [[ "${DATA_DIR}" != /* ]]; then
  DATA_DIR="${SCRIPT_DIR}/${DATA_DIR}"
fi
if [[ "${CONFIG_DIR}" != /* ]]; then
  CONFIG_DIR="${SCRIPT_DIR}/${CONFIG_DIR}"
fi
if [[ "${STANDARD_DATA_DIR}" != /* ]]; then
  STANDARD_DATA_DIR="${SCRIPT_DIR}/${STANDARD_DATA_DIR}"
fi

mkdir -p "${LOG_DIR}"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
LOG_FILE="${LOG_DIR}/inabox-standard-tpch-${STAMP}.log"
SERVER_LOG="${LOG_DIR}/inabox-standard-tpch-server-${STAMP}.log"
exec > >(tee "${LOG_FILE}") 2>&1

cleanup() {
  if [[ "${KEEP_SERVER}" == "1" ]]; then
    if [[ -n "${SERVER_PID}" ]]; then
      echo "preserved quantastream process pid=${SERVER_PID}"
    fi
    return
  fi
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
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

wait_for_server() {
  for _ in $(seq 1 120); do
    if [[ -n "${SERVER_PID}" ]] && ! kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
      echo "quantastream exited before readiness"
      cat "${SERVER_LOG}" || true
      exit 1
    fi
    if command -v mysqladmin >/dev/null 2>&1; then
      if mysqladmin --connect-timeout=1 -h "${HOST}" -P "${PORT}" -u "${SQL_USER}" ping >/dev/null 2>&1; then
        return 0
      fi
    elif (echo >/dev/tcp/"${HOST}"/"${PORT}") >/dev/null 2>&1; then
      sleep 0.25
      return 0
    fi
    sleep 0.25
  done
  echo "quantastream did not become ready on ${HOST}:${PORT}"
  cat "${SERVER_LOG}" || true
  exit 1
}

run_step() {
  local name="$1"
  shift
  local start_epoch end_epoch status elapsed
  start_epoch="$(date +%s)"
  echo "===== ${name} start $(date -u +%Y-%m-%dT%H:%M:%SZ) ====="
  "$@"
  status="$?"
  end_epoch="$(date +%s)"
  elapsed="$((end_epoch - start_epoch))"
  echo "===== ${name} end status=${status} elapsed=${elapsed}s ====="
  echo
  if [[ "${status}" -ne 0 ]]; then
    exit "${status}"
  fi
}

validate_counts() {
  local failures=0
  for table in "${TABLES[@]}"; do
    local expected actual
    expected="$(wc -l < "${DATA_DIR}/${table}.tbl" | tr -d '[:space:]')"
    actual="$(
      mysql -N -B \
        -h "${HOST}" \
        -P "${PORT}" \
        -u "${SQL_USER}" \
        -D "${DB}" \
        -e "select count(*) from ${table};"
    )"
    actual="$(echo "${actual}" | tail -n 1 | tr -d '[:space:]')"
    if [[ "${actual}" == "${expected}" ]]; then
      echo "PASS count ${table} expected=${expected} actual=${actual}"
    else
      echo "FAIL count ${table} expected=${expected} actual=${actual}" >&2
      failures=$((failures + 1))
    fi
  done
  if [[ "${failures}" -ne 0 ]]; then
    return 1
  fi
}

echo "TPC-H inabox-standard load and validation"
echo "timestamp_utc=${STAMP}"
echo "repo=${REPO_ROOT}"
echo "data_dir=${DATA_DIR}"
echo "config_dir=${CONFIG_DIR}"
echo "standard_data_dir=${STANDARD_DATA_DIR}"
echo "target=${HOST}:${PORT}"
echo "db=${DB}"
echo "user=${SQL_USER}"
echo "workers=${WORKERS}"
echo "batch_size=${BATCH_SIZE}"
echo "run_load=${RUN_LOAD}"
echo "run_counts=${RUN_COUNTS}"
echo "run_smoke=${RUN_SMOKE}"
echo "clean_data=${CLEAN_DATA}"
echo "start_server=${START_SERVER}"
echo "keep_server=${KEEP_SERVER}"
if git -C "${REPO_ROOT}" rev-parse --short HEAD >/dev/null 2>&1; then
  echo "git_commit=$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
  echo "git_branch=$(git -C "${REPO_ROOT}" branch --show-current)"
fi
echo "log=${LOG_FILE}"
echo "server_log=${SERVER_LOG}"
echo

if [[ ! -d "${DATA_DIR}" ]]; then
  echo "TPC-H data directory not found: ${DATA_DIR}" >&2
  echo "usage: $0 [tpch-data-dir] [workers] [batch-size]" >&2
  echo "generate data first, for example: ./generate-data.sh /path/to/dbgen 0.01" >&2
  exit 2
fi

if [[ "${RUN_LOAD}" == "1" && "${WORKERS}" != "1" && "${ALLOW_STANDARD_PARALLEL_LOAD:-0}" != "1" ]]; then
  echo "inabox-standard TPC-H load currently requires workers=1" >&2
  echo "parallel standard loads are disabled until local storage concurrency is validated" >&2
  echo "set ALLOW_STANDARD_PARALLEL_LOAD=1 only for investigation" >&2
  exit 2
fi

if [[ "${RUN_LOAD}" == "1" ]]; then
  if [[ "${CLEAN_DATA}" == "1" ]]; then
    run_step "clean_standard_data" rm -rf "${STANDARD_DATA_DIR}"
  fi
  run_step "standard_direct_load" env \
    TPCH_LOAD_MODE=standard \
    TPCH_STANDARD_CONFIG_DIR="${CONFIG_DIR}" \
    TPCH_STANDARD_DATA_DIR="${STANDARD_DATA_DIR}" \
    TPCH_STANDARD_DB="${DB}" \
    "${SCRIPT_DIR}/tpch-direct.sh" "${DATA_DIR}" "${WORKERS}" "${BATCH_SIZE}"
fi

if [[ "${START_SERVER}" == "1" ]]; then
  echo "starting quantastream server target=${HOST}:${PORT}"
  (
    cd "${REPO_ROOT}"
    go run ./cmd/quantastream \
      -config-dir "${CONFIG_DIR}" \
      -data-dir "${STANDARD_DATA_DIR}" \
      -bind "${HOST}" \
      -mysql-port "${PORT}" \
      -database "${DB}"
  ) >"${SERVER_LOG}" 2>&1 &
  SERVER_PID="$!"
  wait_for_server
else
  echo "using already running quantastream server target=${HOST}:${PORT}"
fi

if [[ "${RUN_COUNTS}" == "1" ]]; then
  run_step "validate_counts" validate_counts
fi

if [[ "${RUN_SMOKE}" == "1" ]]; then
  run_step "smoke_suite" bash -lc "
    cd '${SQLRUNNER_DIR}' &&
    go run . \
      -engine inabox-standard \
      -suite_file '../tpc-h-benchmark/${SMOKE_SUITE}' \
      -host '${HOST}' \
      -user '${SQL_USER}' \
      -db '${DB}' \
      -port '${PORT}'
  "
fi

echo "TPC-H inabox-standard validation complete"
