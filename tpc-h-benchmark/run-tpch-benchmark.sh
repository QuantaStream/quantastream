#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DATA_DIR="${1:-local/data/sf-0.01}"
WORKERS="${2:-3}"
BATCH_SIZE="${3:-1000}"
PROFILE_RUNS="${4:-${TPCH_PROFILE_RUNS:-5}}"
SMOKE_RUNS="${TPCH_SMOKE_RUNS:-1}"
SMOKE_SUITE="${TPCH_SMOKE_SUITE:-sqltests/tpch_smoke.yaml}"
PROFILE_SUITE="${TPCH_PROFILE_SUITE:-sqltests/tpch_profile.yaml}"

LOG_DIR="${SCRIPT_DIR}/local/logs"
mkdir -p "${LOG_DIR}"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
LOG_FILE="${LOG_DIR}/tpch-benchmark-${STAMP}.log"
START_LOCAL_LOG="${START_LOCAL_LOG:-${REPO_ROOT}/startup-scripts/.local/logs/start-local.log}"
JOIN_DRIVER_LOG="${LOG_DIR}/join-driver-${STAMP}.log"
PROJECTOR_TIMING_LOG="${LOG_DIR}/projector-timing-${STAMP}.log"
START_LOCAL_LINE=0
if [[ -f "${START_LOCAL_LOG}" ]]; then
  START_LOCAL_LINE="$(wc -l < "${START_LOCAL_LOG}")"
fi

exec > >(tee "${LOG_FILE}") 2>&1

echo "TPC-H end-to-end benchmark"
echo "timestamp_utc=${STAMP}"
echo "repo=${REPO_ROOT}"
echo "data_dir=${DATA_DIR}"
echo "workers=${WORKERS}"
echo "batch_size=${BATCH_SIZE}"
echo "smoke_runs=${SMOKE_RUNS}"
echo "profile_runs=${PROFILE_RUNS}"
echo "smoke_suite=${SMOKE_SUITE}"
echo "profile_suite=${PROFILE_SUITE}"
echo "skip_drop=${TPCH_SKIP_DROP:-0}"
echo "skip_create=${TPCH_SKIP_CREATE:-0}"
echo "skip_load=${TPCH_SKIP_LOAD:-0}"
if git -C "${REPO_ROOT}" rev-parse --short HEAD >/dev/null 2>&1; then
  echo "git_commit=$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
  echo "git_branch=$(git -C "${REPO_ROOT}" branch --show-current)"
fi
echo "top_level_log=${LOG_FILE}"
echo "start_local_log=${START_LOCAL_LOG}"
echo "join_driver_log=${JOIN_DRIVER_LOG}"
echo "projector_timing_log=${PROJECTOR_TIMING_LOG}"
echo

if [[ ! -d "${SCRIPT_DIR}/${DATA_DIR}" && ! -d "${DATA_DIR}" ]]; then
  echo "TPC-H data directory not found: ${DATA_DIR}" >&2
  echo "usage: $0 [tpch-data-dir] [workers] [batch-size] [profile-runs]" >&2
  echo "generate data first, for example: ./generate-data.sh /path/to/dbgen 0.01" >&2
  exit 2
fi

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
    echo "TPC-H benchmark failed during ${name}; log=${LOG_FILE}" >&2
    exit "${status}"
  fi
}

cd "${SCRIPT_DIR}" || exit 1

if [[ "${TPCH_SKIP_DROP:-0}" == "1" ]]; then
  echo "===== drop skipped TPCH_SKIP_DROP=1 ====="
  echo
else
  run_step "drop" ./drop-tpch.sh
fi

if [[ "${TPCH_SKIP_CREATE:-0}" == "1" ]]; then
  echo "===== create skipped TPCH_SKIP_CREATE=1 ====="
  echo
else
  run_step "create" ./create-tpch.sh
fi

if [[ "${TPCH_SKIP_LOAD:-0}" == "1" ]]; then
  echo "===== direct_load skipped TPCH_SKIP_LOAD=1 ====="
  echo
else
  run_step "direct_load" ./tpch-direct.sh "${DATA_DIR}" "${WORKERS}" "${BATCH_SIZE}"
fi
run_step "smoke_suite" ./run-tpch-suite.sh "${SMOKE_RUNS}" "${SMOKE_SUITE}"
run_step "profile_suite" ./run-tpch-suite.sh "${PROFILE_RUNS}" "${PROFILE_SUITE}"

profile_name="$(basename "${PROFILE_SUITE}" .yaml)"
smoke_name="$(basename "${SMOKE_SUITE}" .yaml)"
latest_profile_log="$(ls -t "${LOG_DIR}"/"${profile_name}"-"${PROFILE_RUNS}"x-*.log 2>/dev/null | head -1 || true)"
if [[ -n "${latest_profile_log}" ]]; then
  echo "===== profile summary ${latest_profile_log} ====="
  python3 ./summarize-tpch-suite.py "${latest_profile_log}"
  echo
fi

latest_smoke_log="$(ls -t "${LOG_DIR}"/"${smoke_name}"-"${SMOKE_RUNS}"x-*.log 2>/dev/null | head -1 || true)"
if [[ -n "${latest_smoke_log}" ]]; then
  echo "smoke_log=${latest_smoke_log}"
fi
if [[ -n "${latest_profile_log}" ]]; then
  echo "profile_log=${latest_profile_log}"
fi
if [[ -f "${START_LOCAL_LOG}" ]]; then
  tail -n +"$((START_LOCAL_LINE + 1))" "${START_LOCAL_LOG}" | grep "JOIN_DRIVER" > "${JOIN_DRIVER_LOG}" || true
  if [[ -s "${JOIN_DRIVER_LOG}" ]]; then
    echo "join_driver_log=${JOIN_DRIVER_LOG}"
    echo
    echo "===== join driver summary ${JOIN_DRIVER_LOG} ====="
    python3 ./summarize-join-driver.py "${JOIN_DRIVER_LOG}"
    echo
  else
    rm -f "${JOIN_DRIVER_LOG}"
  fi

  tail -n +"$((START_LOCAL_LINE + 1))" "${START_LOCAL_LOG}" | grep "PROJECTOR_TIMING" > "${PROJECTOR_TIMING_LOG}" || true
  if [[ -s "${PROJECTOR_TIMING_LOG}" ]]; then
    echo "projector_timing_log=${PROJECTOR_TIMING_LOG}"
    echo
    echo "===== projector timing summary ${PROJECTOR_TIMING_LOG} ====="
    python3 ./summarize-projector-timing.py "${PROJECTOR_TIMING_LOG}"
    echo
  else
    rm -f "${PROJECTOR_TIMING_LOG}"
  fi
fi
echo "TPC-H end-to-end benchmark complete log=${LOG_FILE}"
