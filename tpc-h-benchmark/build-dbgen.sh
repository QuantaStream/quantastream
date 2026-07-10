#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <dbgen-dir>" >&2
  exit 1
fi

DBGEN_DIR=$1

if [[ ! -d "${DBGEN_DIR}" ]]; then
  echo "dbgen directory not found: ${DBGEN_DIR}" >&2
  echo "Install or unpack the official TPC-H dbgen kit locally, then rerun this script." >&2
  exit 1
fi

if [[ -x "${DBGEN_DIR}/dbgen" ]]; then
  echo "dbgen already built: ${DBGEN_DIR}/dbgen"
else
  echo "Building dbgen in ${DBGEN_DIR}"
  make -C "${DBGEN_DIR}" dbgen
fi

if [[ ! -x "${DBGEN_DIR}/dbgen" ]]; then
  echo "dbgen was not created or is not executable: ${DBGEN_DIR}/dbgen" >&2
  exit 1
fi

if [[ -f "${DBGEN_DIR}/qgen" || -x "${DBGEN_DIR}/qgen" ]]; then
  echo "qgen present: ${DBGEN_DIR}/qgen"
fi

echo "dbgen ready: ${DBGEN_DIR}/dbgen"
