#!/usr/bin/env bash
set -euo pipefail

export PATH="$PATH:."

TPCH_DDL_MODE=${TPCH_DDL_MODE:-admin}
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

case "${TPCH_DDL_MODE}" in
  admin)
    for table in "${TABLES[@]}"; do
      quanta-admin create "${table}"
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
