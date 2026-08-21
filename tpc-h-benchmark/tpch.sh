#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

DATA_DIR=${1:-local/data/sf-0.01}
WORKERS=${2:-3}
BATCH_SIZE=${3:-1000}

exec ./tpch-direct.sh "${DATA_DIR}" "${WORKERS}" "${BATCH_SIZE}"
