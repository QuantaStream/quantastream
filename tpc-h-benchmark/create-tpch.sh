#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QS_REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TPCH_DDL_MODE=${TPCH_DDL_MODE:-admin}
ADMIN_BIN=${ADMIN_BIN:-}
QUANTA_HOST=${QUANTA_HOST:-127.0.0.1}
QUANTA_PORT=${QUANTA_PORT:-4000}
QUANTA_USER=${QUANTA_USER:-qstream}
QUANTA_DB=${QUANTA_DB:-quanta}

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

run_admin() {
  if [[ -n "${ADMIN_BIN}" ]]; then
    "${ADMIN_BIN}" "$@"
    return
  fi
  go -C "${QS_REPO_ROOT}" run ./qstream-admin "$@"
}

case "${TPCH_DDL_MODE}" in
  admin)
    for table in "${TABLES[@]}"; do
      run_admin create "${table}"
    done
    ;;
  sql)
    sql=""
    for table in "${TABLES[@]}"; do
      sql+="create table ${table};"$'\n'
    done
    mysql -vvv -h "${QUANTA_HOST}" -P "${QUANTA_PORT}" -u "${QUANTA_USER}" -D "${QUANTA_DB}" -e "${sql}"
    ;;
  *)
    echo "unsupported TPCH_DDL_MODE=${TPCH_DDL_MODE}; expected admin or sql" >&2
    exit 2
    ;;
esac
