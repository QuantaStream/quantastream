#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  MYSQL_DSN='user:pass@tcp(host:3306)/db' ./run-mysql-compat.sh
  MYSQL_DSN='user:pass@tcp(host:3306)/db' MYSQL_COMPAT_MODE=diff TARGET_ENGINE=inabox-direct ./run-mysql-compat.sh

Environment:
  MYSQL_DSN              Required database/sql DSN for the stock MySQL reference.
  MYSQL_COMPAT_SUITE     Suite path. Defaults to sqltests/mysql_compat_select.yaml.
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

suite="${MYSQL_COMPAT_SUITE:-sqltests/mysql_compat_select.yaml}"
mode="${MYSQL_COMPAT_MODE:-capture}"
target="${TARGET_ENGINE:-inabox-direct}"
driver="${MYSQL_DRIVER:-mysql}"
consul="${CONSUL:-127.0.0.1:8500}"
base="$(basename "${suite}")"
output="${MYSQL_COMPAT_OUTPUT:-expected/local/${base}}"

mkdir -p "$(dirname "${output}")"

case "${mode}" in
  capture)
    go run . \
      -engine mysql-reference \
      -suite_file "${suite}" \
      -mysql_driver "${driver}" \
      -mysql_dsn "${MYSQL_DSN}" \
      -capture_expected "${output}"
    ;;
  diff)
    go run . \
      -engine_diff "mysql-reference,${target}" \
      -suite_file "${suite}" \
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
