#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQLRUNNER_DIR="${REPO_ROOT}/sqlrunner"

RUNS="${1:-3}"
SUITE_FILE="${2:-sqltests/tpch_queries.yaml}"

HOST="${QUANTA_HOST:-127.0.0.1}"
PORT="${QUANTA_PORT:-4000}"
USER="${QUANTA_USER:-MOLIG004}"
DB="${QUANTA_DB:-quanta}"

if ! [[ "${RUNS}" =~ ^[0-9]+$ ]] || [ "${RUNS}" -lt 1 ]; then
  echo "usage: $0 [runs] [suite-file]" >&2
  echo "runs must be a positive integer" >&2
  exit 2
fi

if [[ "${SUITE_FILE}" = /* ]]; then
  SUITE_ARG="${SUITE_FILE}"
  SUITE_NAME="$(basename "${SUITE_FILE}" .yaml)"
else
  SUITE_ARG="../tpc-h-benchmark/${SUITE_FILE}"
  SUITE_NAME="$(basename "${SUITE_FILE}" .yaml)"
fi

LOG_DIR="${SCRIPT_DIR}/local/logs"
mkdir -p "${LOG_DIR}"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
LOG_FILE="${LOG_DIR}/${SUITE_NAME}-${RUNS}x-${STAMP}.log"

{
  echo "TPC-H suite baseline"
  echo "timestamp_utc=${STAMP}"
  echo "repo=${REPO_ROOT}"
  echo "suite=${SUITE_FILE}"
  echo "runs=${RUNS}"
  echo "host=${HOST}"
  echo "port=${PORT}"
  echo "user=${USER}"
  echo "db=${DB}"
  if git -C "${REPO_ROOT}" rev-parse --short HEAD >/dev/null 2>&1; then
    echo "git_commit=$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
    echo "git_branch=$(git -C "${REPO_ROOT}" branch --show-current)"
  fi
  echo
} | tee "${LOG_FILE}"

failures=0
for run in $(seq 1 "${RUNS}"); do
  start_epoch="$(date +%s)"
  echo "===== run ${run}/${RUNS} start $(date -u +%Y-%m-%dT%H:%M:%SZ) =====" | tee -a "${LOG_FILE}"

  (
    cd "${SQLRUNNER_DIR}" &&
      go run . \
        -suite_file "${SUITE_ARG}" \
        -host "${HOST}" \
        -user "${USER}" \
        -db "${DB}" \
        -port "${PORT}"
  ) 2>&1 | tee -a "${LOG_FILE}"
  status="${PIPESTATUS[0]}"

  end_epoch="$(date +%s)"
  elapsed="$((end_epoch - start_epoch))"
  echo "===== run ${run}/${RUNS} end status=${status} elapsed=${elapsed}s =====" | tee -a "${LOG_FILE}"
  echo | tee -a "${LOG_FILE}"

  if [ "${status}" -ne 0 ]; then
    failures=$((failures + 1))
  fi
done

echo "TPC-H suite baseline complete failures=${failures} log=${LOG_FILE}" | tee -a "${LOG_FILE}"
if [ "${failures}" -ne 0 ]; then
  exit 1
fi
