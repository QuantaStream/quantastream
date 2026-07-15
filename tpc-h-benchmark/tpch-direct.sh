#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

DATA_DIR=${1:-local/data/sf-0.01}
WORKERS=${2:-3}
BATCH_SIZE=${3:-1000}
ADMIN_BIN=${ADMIN_BIN:-./quanta-admin}
WAIT_SECONDS=${WAIT_SECONDS:-120}
LOG_DIR=${LOG_DIR:-local/logs}
LOG_FILE=${LOG_FILE:-"${LOG_DIR}/tpch-direct-$(date +%Y%m%d-%H%M%S).log"}
TPCH_LOAD_MODE=${TPCH_LOAD_MODE:-cluster}
TPCH_STANDARD_CONFIG_DIR=${TPCH_STANDARD_CONFIG_DIR:-config}
TPCH_STANDARD_DATA_DIR=${TPCH_STANDARD_DATA_DIR:-local/standard-data}
TPCH_STANDARD_DB=${TPCH_STANDARD_DB:-quanta}

if [[ ! -d "${DATA_DIR}" ]]; then
  echo "TPC-H data directory not found: ${DATA_DIR}" >&2
  echo "usage: $0 [tpch-data-dir] [workers] [batch-size]" >&2
  echo "generate data first, for example: ./generate-data.sh ~/TPC-H\\ V3.0.1/dbgen 0.01" >&2
  exit 1
fi

mkdir -p "${LOG_DIR}"
exec > >(tee "${LOG_FILE}") 2>&1

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

start_epoch=$(date +%s)
echo "TPC-H direct load starting mode=${TPCH_LOAD_MODE} data_dir=${DATA_DIR} workers=${WORKERS} batch_size=${BATCH_SIZE}"
echo "TPC-H direct load log=${LOG_FILE}"
if [[ "${TPCH_LOAD_MODE}" == "standard" ]]; then
  echo "TPC-H standard load config_dir=${TPCH_STANDARD_CONFIG_DIR} data_dir=${TPCH_STANDARD_DATA_DIR} db=${TPCH_STANDARD_DB}"
fi

wait_for_cluster() {
  if [[ ! -x "${ADMIN_BIN}" ]]; then
    echo "quanta-admin not found or not executable: ${ADMIN_BIN}" >&2
    exit 1
  fi

  local deadline=$(( $(date +%s) + WAIT_SECONDS ))
  local status
  while true; do
    status="$("${ADMIN_BIN}" status 2>&1 || true)"
    if grep -q "Cluster State = GREEN, Active nodes = 3, Target Cluster Size = 3" <<<"${status}"; then
      return
    fi
    if (( $(date +%s) >= deadline )); then
      echo "Timed out waiting for GREEN cluster:" >&2
      echo "${status}" >&2
      exit 1
    fi
    sleep 2
  done
}

if [[ "${TPCH_LOAD_MODE}" == "cluster" ]]; then
  wait_for_cluster
elif [[ "${TPCH_LOAD_MODE}" != "standard" ]]; then
  echo "unsupported TPCH_LOAD_MODE=${TPCH_LOAD_MODE}; expected cluster or standard" >&2
  exit 2
fi

for table in "${TABLES[@]}"; do
  table_start=$(date +%s)
  if [[ "${TPCH_LOAD_MODE}" == "cluster" ]]; then
    wait_for_cluster
  fi
  expected_rows=$(wc -l < "${DATA_DIR}/${table}.tbl")
  echo "Loading ${table} expected_rows=${expected_rows}..."
  if [[ "${TPCH_LOAD_MODE}" == "standard" ]]; then
    go run . \
      --direct \
      --direct-mode standard \
      --config-dir "${TPCH_STANDARD_CONFIG_DIR}" \
      --data-dir "${TPCH_STANDARD_DATA_DIR}" \
      --database "${TPCH_STANDARD_DB}" \
      --workers "${WORKERS}" \
      --batch-size "${BATCH_SIZE}" \
      "${DATA_DIR}" "${table}"
  else
    go run . --direct --workers "${WORKERS}" --batch-size "${BATCH_SIZE}" "${DATA_DIR}" "${table}"
  fi
  table_end=$(date +%s)
  echo "Loaded ${table} elapsed=$((table_end - table_start))s"
done

end_epoch=$(date +%s)
echo "TPC-H direct load complete total_elapsed=$((end_epoch - start_epoch))s"
