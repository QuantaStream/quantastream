#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-4000}"
USER="${USER:-MOLIG004}"
DB="${DB:-quanta}"
SUITE="${SUITE:-inabox_standard_smoke.yaml}"
CASE="${CASE:-}"

echo
echo "===== inabox-standard ${SUITE} ====="
echo "target=${HOST}:${PORT} db=${DB} user=${USER}"
echo "This smoke expects a running quantastream process and already staged TPCH data."

args=(
  -engine inabox-standard
  -suite_file "sqltests/${SUITE}"
  -host "${HOST}"
  -user "${USER}"
  -db "${DB}"
  -port "${PORT}"
)

if [[ -n "${CASE}" ]]; then
  args+=(-case "${CASE}")
fi

go run . "${args[@]}"
