#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

CONSUL="${CONSUL:-127.0.0.1:8500}"
SLOW_THRESHOLD="${SLOW_THRESHOLD:-10s}"
RUN_TPCH="${RUN_TPCH:-0}"

run_suite() {
  local suite="$1"
  shift || true
  echo
  echo "===== legacy-direct ${suite} ====="
  go run . -engine legacy-direct -suite_file "sqltests/${suite}" -consul "${CONSUL}" "$@"
}

run_suite legacy_direct_smoke.yaml
run_suite legacy_direct_basic.yaml
run_suite legacy_direct_qa_basic.yaml
run_suite legacy_direct_joins.yaml
run_suite mutate_tests_body.yaml

if [[ "${RUN_TPCH}" == "1" ]]; then
  run_suite legacy_direct_tpch_kernels.yaml -slow_threshold "${SLOW_THRESHOLD}"
else
  echo
  echo "===== legacy-direct legacy_direct_tpch_kernels.yaml skipped ====="
  echo "Set RUN_TPCH=1 to run the broad TPCH kernel gate."
  echo "Set SLOW_THRESHOLD=${SLOW_THRESHOLD} to adjust the slow-case summary threshold."
fi
