#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <dbgen-dir> <scale-factor> [output-dir]" >&2
  exit 1
fi

DBGEN_DIR=$1
SCALE_FACTOR=$2
OUTPUT_DIR=${3:-local/data/sf-${SCALE_FACTOR}}

if [[ ! -x "${DBGEN_DIR}/dbgen" ]]; then
  echo "dbgen is not executable: ${DBGEN_DIR}/dbgen" >&2
  echo "Run ./build-dbgen.sh ${DBGEN_DIR} first." >&2
  exit 1
fi

if [[ ! -f "${DBGEN_DIR}/dists.dss" ]]; then
  echo "dbgen distribution file not found: ${DBGEN_DIR}/dists.dss" >&2
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"
OUTPUT_DIR=$(cd "${OUTPUT_DIR}" && pwd)

echo "Generating TPC-H data scale_factor=${SCALE_FACTOR} output_dir=${OUTPUT_DIR}"
(
  cd "${OUTPUT_DIR}"
  "${DBGEN_DIR}/dbgen" -s "${SCALE_FACTOR}" -f -b "${DBGEN_DIR}/dists.dss"
)

echo "Generated files:"
ls -lh "${OUTPUT_DIR}"/*.tbl
