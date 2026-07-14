#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

cd "${script_dir}"

HOST="${HOST:-127.0.0.1}"
BASE_PORT="${PORT:-4400}"
SQL_USER="${SQL_USER:-MOLIG004}"
DB="${DB:-quanta}"
CLEANUP="${CLEANUP:-1}"
RUN_PORTABLE="${RUN_PORTABLE:-0}"
RUN_EXTENDED="${RUN_EXTENDED:-0}"
ALLOW_FAILURES="${ALLOW_FAILURES:-0}"
SLOW_THRESHOLD="${SLOW_THRESHOLD:-10s}"
SUITE_INDEX=0
SERVER_PID=""
WORK_DIR=""
PASSED_SUITES=()
FAILED_SUITES=()

cleanup_server() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  SERVER_PID=""
}

cleanup_work_dir() {
  if [[ "${CLEANUP}" == "1" ]] && [[ -n "${WORK_DIR}" ]] && [[ -d "${WORK_DIR}" ]]; then
    rm -rf "${WORK_DIR}"
  elif [[ -n "${WORK_DIR}" ]]; then
    echo "preserved readiness work directory: ${WORK_DIR}"
  fi
  WORK_DIR=""
}

cleanup() {
  cleanup_server
  cleanup_work_dir
}
trap cleanup EXIT

write_catalog_manifest() {
  local config_dir="$1"
  shift || true
  if [[ "$#" -eq 0 ]]; then
    printf "objects: []\n" > "${config_dir}/CATALOG_OBJECTS"
    return
  fi
  {
    printf "objects:\n"
    for table in "$@"; do
      printf "  - schema_name: %s\n" "${DB}"
      printf "    table_name: %s\n" "${table}"
      printf "    object_type: TABLE\n"
    done
  } > "${config_dir}/CATALOG_OBJECTS"
}

wait_for_server() {
  local port="$1"
  local log_file="$2"
  for _ in $(seq 1 80); do
    if [[ -n "${SERVER_PID}" ]] && ! kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
      echo "quantastream exited before readiness"
      cat "${log_file}" || true
      exit 1
    fi
    if command -v mysqladmin >/dev/null 2>&1; then
      if mysqladmin --connect-timeout=1 -h "${HOST}" -P "${port}" -u "${SQL_USER}" ping >/dev/null 2>&1; then
        return 0
      fi
    elif (echo >/dev/tcp/"${HOST}"/"${port}") >/dev/null 2>&1; then
      sleep 0.25
      return 0
    fi
    sleep 0.25
  done
  echo "quantastream did not become ready on ${HOST}:${port}"
  cat "${log_file}" || true
  exit 1
}

run_suite() {
  local suite="$1"
  shift || true
  local port=$((BASE_PORT + SUITE_INDEX))
  SUITE_INDEX=$((SUITE_INDEX + 1))

  cleanup_server
  cleanup_work_dir

  WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/quantastream-standard-readiness.XXXXXX")"
  local runtime_config="${WORK_DIR}/config"
  local runtime_data="${WORK_DIR}/data"
  local log_file="${WORK_DIR}/quantastream.log"

  mkdir -p "${runtime_config}"
  cp -R "${script_dir}/config/." "${runtime_config}/"
  write_catalog_manifest "${runtime_config}" "$@"

  echo
  echo "===== inabox-standard ${suite} ====="
  echo "target=${HOST}:${port} db=${DB} user=${SQL_USER} active_tables=${*:-<none>}"
  (
    cd "${repo_root}"
    go run ./cmd/quantastream \
      -config-dir "${runtime_config}" \
      -data-dir "${runtime_data}" \
      -bind "${HOST}" \
      -mysql-port "${port}" \
      -database "${DB}"
  ) >"${log_file}" 2>&1 &
  SERVER_PID="$!"
  wait_for_server "${port}" "${log_file}"

  set +e
  go run . \
    -engine inabox-standard \
    -suite_file "sqltests/${suite}" \
    -host "${HOST}" \
    -user "${SQL_USER}" \
    -db "${DB}" \
    -port "${port}" \
    -slow_threshold "${SLOW_THRESHOLD}" \
    2>&1
  local status="$?"
  set -e

  cleanup_server
  cleanup_work_dir

  if [[ "${status}" -eq 0 ]]; then
    PASSED_SUITES+=("${suite}")
    return 0
  fi
  FAILED_SUITES+=("${suite}")
  if [[ "${ALLOW_FAILURES}" == "1" ]]; then
    return 0
  fi
  return "${status}"
}

run_suite inabox_standard_qa_smoke.yaml

if [[ "${RUN_PORTABLE}" == "1" ]]; then
  run_suite basic_queries.yaml orders_qa customers_qa
  run_suite insert_tests.yaml orders_qa customers_qa
  run_suite function_expressions.yaml deliveries_qa lineitems_qa orders_qa customers_qa
else
  echo
  echo "===== inabox-standard portable suites skipped ====="
  echo "Set RUN_PORTABLE=1 to run basic_queries, insert_tests, and function_expressions."
  echo "Use ALLOW_FAILURES=1 for discovery while these suites are being promoted."
fi

if [[ "${RUN_EXTENDED}" == "1" ]]; then
  run_suite group_by.yaml deliveries_qa lineitems_qa orders_qa customers_qa
  run_suite joins_sql.yaml orders_qa customers_qa
  run_suite join_group_by.yaml deliveries_qa lineitems_qa orders_qa customers_qa
  run_suite subqueries.yaml deliveries_qa lineitems_qa orders_qa customers_qa
  run_suite multi_table_joins.yaml deliveries_qa lineitems_qa orders_qa customers_qa
  run_suite mutate_tests_body.yaml orders_qa customers_qa
else
  echo
  echo "===== inabox-standard extended suites skipped ====="
  echo "Set RUN_EXTENDED=1 to run group_by, joins_sql, join_group_by, subqueries, multi_table_joins, and mutate_tests_body."
fi

echo
echo "===== inabox-standard readiness summary ====="
printf "passed:"
for suite in "${PASSED_SUITES[@]}"; do
  printf " %s" "${suite}"
done
printf "\n"
if [[ "${#FAILED_SUITES[@]}" -gt 0 ]]; then
  printf "failed:"
  for suite in "${FAILED_SUITES[@]}"; do
    printf " %s" "${suite}"
  done
  printf "\n"
  if [[ "${ALLOW_FAILURES}" != "1" ]]; then
    exit 1
  fi
fi
