#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  MYSQL_DSN='user:pass@tcp(host:3306)/db' ./run-mysql-compat.sh
  MYSQL_DSN='user:pass@tcp(host:3306)/db' MYSQL_COMPAT_MODE=diff TARGET_ENGINE=inabox-direct ./run-mysql-compat.sh
  MYSQL_DSN='user:pass@tcp(host:3306)/db' MYSQL_COMPAT_SUITE=all MYSQL_COMPAT_MODE=diff ./run-mysql-compat.sh
  MYSQL_DSN='user:pass@tcp(host:3306)/db' MYSQL_COMPAT_SUITE=boundaries MYSQL_COMPAT_MODE=diff ./run-mysql-compat.sh

Environment:
  MYSQL_DSN              Required database/sql DSN for the stock MySQL reference.
  MYSQL_COMPAT_SUITE     Suite path, "all", "boundaries", or "all-with-boundaries". Defaults to sqltests/mysql_compat_select.yaml.
  MYSQL_COMPAT_OUTPUT    Capture output path. Defaults to expected/local/<suite basename>.
  MYSQL_COMPAT_MODE      capture or diff. Defaults to capture.
  TARGET_ENGINE          Diff target engine. Defaults to inabox-direct.
  MYSQL_DRIVER           database/sql driver. Defaults to mysql.
  CONSUL                 Consul address for engines that need it. Defaults to 127.0.0.1:8500.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ -z "${MYSQL_DSN:-}" ]]; then
  echo "MYSQL_DSN is required." >&2
  usage >&2
  exit 2
fi

default_suites=(
  sqltests/mysql_compat_select.yaml
  sqltests/mysql_compat_predicates.yaml
  sqltests/mysql_compat_functions.yaml
  sqltests/mysql_compat_group_order.yaml
  sqltests/mysql_compat_joins.yaml
  sqltests/mysql_compat_subqueries.yaml
  sqltests/mysql_compat_mutations.yaml
  sqltests/mysql_compat_views.yaml
)

boundary_suites=(
  sqltests/mysql_compat_select_boundaries.yaml
  sqltests/mysql_compat_predicates_boundaries.yaml
  sqltests/mysql_compat_functions_boundaries.yaml
  sqltests/mysql_compat_group_order_boundaries.yaml
  sqltests/mysql_compat_joins_boundaries.yaml
  sqltests/mysql_compat_subqueries_boundaries.yaml
  sqltests/mysql_compat_mutations_boundaries.yaml
  sqltests/mysql_compat_views_boundaries.yaml
)

suite="${MYSQL_COMPAT_SUITE:-sqltests/mysql_compat_select.yaml}"
mode="${MYSQL_COMPAT_MODE:-capture}"
target="${TARGET_ENGINE:-inabox-direct}"
driver="${MYSQL_DRIVER:-mysql}"
consul="${CONSUL:-127.0.0.1:8500}"

run_suite() {
  local suite_path="$1"
  local base
  local output
  base="$(basename "${suite_path}")"
  output="${MYSQL_COMPAT_OUTPUT:-expected/local/${base}}"
  mkdir -p "$(dirname "${output}")"

  case "${mode}" in
    capture)
      go run . \
        -engine mysql-reference \
        -suite_file "${suite_path}" \
        -mysql_driver "${driver}" \
        -mysql_dsn "${MYSQL_DSN}" \
        -capture_expected "${output}"
      ;;
    diff)
      go run . \
        -engine_diff "mysql-reference,${target}" \
        -suite_file "${suite_path}" \
        -mysql_driver "${driver}" \
        -mysql_dsn "${MYSQL_DSN}" \
        -consul "${consul}" \
        -compat_report
      ;;
    *)
      echo "Unsupported MYSQL_COMPAT_MODE: ${mode}" >&2
      usage >&2
      exit 2
      ;;
  esac
}

if [[ "${suite}" == "all" || "${suite}" == "boundaries" || "${suite}" == "all-with-boundaries" ]]; then
  if [[ -n "${MYSQL_COMPAT_OUTPUT:-}" ]]; then
    echo "MYSQL_COMPAT_OUTPUT cannot be used with MYSQL_COMPAT_SUITE=${suite}." >&2
    exit 2
  fi
  case "${suite}" in
    all)
      for suite_path in "${default_suites[@]}"; do
        run_suite "${suite_path}"
      done
      ;;
    boundaries)
      for suite_path in "${boundary_suites[@]}"; do
        run_suite "${suite_path}"
      done
      ;;
    all-with-boundaries)
      for suite_path in "${default_suites[@]}" "${boundary_suites[@]}"; do
        run_suite "${suite_path}"
      done
      ;;
  esac
else
  run_suite "${suite}"
fi
