#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

cd "${script_dir}"

HOST="${HOST:-127.0.0.1}"
START_SERVER="${START_SERVER:-1}"
if [[ -z "${PORT:-}" ]]; then
  if [[ "${START_SERVER}" == "1" ]]; then
    PORT="4400"
  else
    PORT="4000"
  fi
fi
SQL_USER="${SQL_USER:-MOLIG004}"
DB="${DB:-quanta}"
SUITE="${SUITE:-inabox_standard_qa_smoke.yaml}"
CASE="${CASE:-}"
CLEANUP="${CLEANUP:-1}"
READY_ATTEMPTS="${READY_ATTEMPTS:-160}"
SERVER_PID=""
WORK_DIR=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${CLEANUP}" == "1" ]] && [[ -n "${WORK_DIR}" ]] && [[ -d "${WORK_DIR}" ]]; then
    rm -rf "${WORK_DIR}"
  elif [[ -n "${WORK_DIR}" ]]; then
    echo "preserved smoke work directory: ${WORK_DIR}"
  fi
}
trap cleanup EXIT

wait_for_server() {
  local log_file="$1"
  for _ in $(seq 1 "${READY_ATTEMPTS}"); do
    if [[ -n "${SERVER_PID}" ]] && ! kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
      echo "quantastream exited before readiness"
      cat "${log_file}" || true
      exit 1
    fi
    if (echo >/dev/tcp/"${HOST}"/"${PORT}") >/dev/null 2>&1; then
      sleep 0.25
      return 0
    fi
    if command -v mysqladmin >/dev/null 2>&1; then
      if mysqladmin --connect-timeout=1 -h "${HOST}" -P "${PORT}" -u "${SQL_USER}" ping >/dev/null 2>&1; then
        return 0
      fi
    fi
    sleep 0.25
  done
  echo "quantastream did not become ready on ${HOST}:${PORT}"
  cat "${log_file}" || true
  exit 1
}

echo
echo "===== inabox-standard ${SUITE} ====="
echo "target=${HOST}:${PORT} db=${DB} user=${SQL_USER}"

if [[ "${START_SERVER}" == "1" ]]; then
  WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/quantastream-standard-smoke.XXXXXX")"
  runtime_config="${WORK_DIR}/config"
  runtime_data="${WORK_DIR}/data"
  log_file="${WORK_DIR}/quantastream.log"

  mkdir -p "${runtime_config}"
  cp -R "${script_dir}/config/customers_qa" "${runtime_config}/customers_qa"
  printf "objects: []\n" > "${runtime_config}/CATALOG_OBJECTS"

  echo "starting temporary quantastream process"
  echo "work_dir=${WORK_DIR}"
  (
    cd "${repo_root}"
    go run ./cmd/quantastream \
      -config-dir "${runtime_config}" \
      -data-dir "${runtime_data}" \
      -bind "${HOST}" \
      -mysql-port "${PORT}" \
      -database "${DB}"
  ) >"${log_file}" 2>&1 &
  SERVER_PID="$!"
  wait_for_server "${log_file}"
else
  echo "using already running quantastream process"
fi

args=(
  -engine inabox-standard
  -suite_file "sqltests/${SUITE}"
  -host "${HOST}"
  -user "${SQL_USER}"
  -db "${DB}"
  -port "${PORT}"
)

if [[ -n "${CASE}" ]]; then
  args+=(-case "${CASE}")
fi

go run . "${args[@]}"
