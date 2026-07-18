#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ./load-mysql-tpch.sh [tpch-data-dir]

Environment:
  MYSQL_HOST            MySQL host. Defaults to 127.0.0.1.
  MYSQL_PORT            MySQL port. Defaults to 3306.
  MYSQL_USER            MySQL user. Defaults to root.
  MYSQL_PASSWORD        MySQL password. Defaults to empty.
  MYSQL_DATABASE        Target database. Defaults to tpch.
  MYSQL_RESET           Drop and recreate TPC-H tables before load. Defaults to 1.
  MYSQL_INDEX_PROFILE   none, pk, or benchmark. Defaults to benchmark.
  RUN_COUNTS            Validate table counts after load. Defaults to 1.

The loader uses LOAD DATA LOCAL INFILE, so the MySQL client and server must
allow local infile loading. For MySQL 8 this commonly requires starting the
server with local_infile=ON or setting it globally before the load.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${1:-local/data/sf-0.01}"
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
MYSQL_DATABASE="${MYSQL_DATABASE:-tpch}"
MYSQL_RESET="${MYSQL_RESET:-1}"
MYSQL_INDEX_PROFILE="${MYSQL_INDEX_PROFILE:-benchmark}"
RUN_COUNTS="${RUN_COUNTS:-1}"

TABLES=(
  region
  nation
  supplier
  customer
  part
  partsupp
  orders
  lineitem
)

if [[ "${DATA_DIR}" != /* ]]; then
  DATA_DIR="${SCRIPT_DIR}/${DATA_DIR}"
fi
DATA_DIR="$(cd "${DATA_DIR}" && pwd)"

if ! command -v mysql >/dev/null 2>&1; then
  echo "mysql client not found on PATH" >&2
  exit 2
fi

for table in "${TABLES[@]}"; do
  if [[ ! -f "${DATA_DIR}/${table}.tbl" ]]; then
    echo "TPC-H data file not found: ${DATA_DIR}/${table}.tbl" >&2
    exit 2
  fi
done

case "${MYSQL_INDEX_PROFILE}" in
  none|pk|benchmark) ;;
  *)
    echo "MYSQL_INDEX_PROFILE must be one of: none, pk, benchmark" >&2
    exit 2
    ;;
esac

is_enabled() {
  case "${1}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

quote_ident() {
  local value="${1//\`/\`\`}"
  printf '`%s`' "${value}"
}

sql_string() {
  local value="${1//\'/\'\'}"
  printf "'%s'" "${value}"
}

mysql_args=(
  --local-infile=1
  --protocol=tcp
  -h "${MYSQL_HOST}"
  -P "${MYSQL_PORT}"
  -u "${MYSQL_USER}"
)

mysql_env=()
if [[ -n "${MYSQL_PASSWORD}" ]]; then
  mysql_env=(MYSQL_PWD="${MYSQL_PASSWORD}")
fi

run_mysql() {
  env "${mysql_env[@]}" mysql "${mysql_args[@]}" "$@"
}

run_mysql_db() {
  env "${mysql_env[@]}" mysql "${mysql_args[@]}" -D "${MYSQL_DATABASE}" "$@"
}

run_step() {
  local name="$1"
  shift
  local start end elapsed
  start="$(date +%s)"
  echo "===== ${name} start $(date -u +%Y-%m-%dT%H:%M:%SZ) ====="
  "$@"
  end="$(date +%s)"
  elapsed="$((end - start))"
  echo "===== ${name} end elapsed=${elapsed}s ====="
  echo
}

write_schema_sql() {
  local output="$1"
  local db
  db="$(quote_ident "${MYSQL_DATABASE}")"
  {
    echo "CREATE DATABASE IF NOT EXISTS ${db};"
    echo "USE ${db};"
    echo "SET FOREIGN_KEY_CHECKS=0;"
    if is_enabled "${MYSQL_RESET}"; then
      echo "DROP TABLE IF EXISTS lineitem;"
      echo "DROP TABLE IF EXISTS orders;"
      echo "DROP TABLE IF EXISTS partsupp;"
      echo "DROP TABLE IF EXISTS part;"
      echo "DROP TABLE IF EXISTS customer;"
      echo "DROP TABLE IF EXISTS supplier;"
      echo "DROP TABLE IF EXISTS nation;"
      echo "DROP TABLE IF EXISTS region;"
    fi
    cat <<'SQL'
CREATE TABLE IF NOT EXISTS region (
  r_regionkey INT NOT NULL,
  r_name CHAR(25) NOT NULL,
  r_comment VARCHAR(152)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS nation (
  n_nationkey INT NOT NULL,
  n_name CHAR(25) NOT NULL,
  n_regionkey INT NOT NULL,
  n_comment VARCHAR(152)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS supplier (
  s_suppkey INT NOT NULL,
  s_name CHAR(25) NOT NULL,
  s_address VARCHAR(40) NOT NULL,
  s_nationkey INT NOT NULL,
  s_phone CHAR(15) NOT NULL,
  s_acctbal DECIMAL(15,2) NOT NULL,
  s_comment VARCHAR(101) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS customer (
  c_custkey INT NOT NULL,
  c_name VARCHAR(25) NOT NULL,
  c_address VARCHAR(40) NOT NULL,
  c_nationkey INT NOT NULL,
  c_phone CHAR(15) NOT NULL,
  c_acctbal DECIMAL(15,2) NOT NULL,
  c_mktsegment CHAR(10) NOT NULL,
  c_comment VARCHAR(117) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS part (
  p_partkey INT NOT NULL,
  p_name VARCHAR(55) NOT NULL,
  p_mfgr CHAR(25) NOT NULL,
  p_brand CHAR(10) NOT NULL,
  p_type VARCHAR(25) NOT NULL,
  p_size INT NOT NULL,
  p_container CHAR(10) NOT NULL,
  p_retailprice DECIMAL(15,2) NOT NULL,
  p_comment VARCHAR(23) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS partsupp (
  ps_partkey INT NOT NULL,
  ps_suppkey INT NOT NULL,
  ps_availqty INT NOT NULL,
  ps_supplycost DECIMAL(15,2) NOT NULL,
  ps_comment VARCHAR(199) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS orders (
  o_orderkey INT NOT NULL,
  o_custkey INT NOT NULL,
  o_orderstatus CHAR(1) NOT NULL,
  o_totalprice DECIMAL(15,2) NOT NULL,
  o_orderdate DATE NOT NULL,
  o_orderpriority CHAR(15) NOT NULL,
  o_clerk CHAR(15) NOT NULL,
  o_shippriority INT NOT NULL,
  o_comment VARCHAR(79) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS lineitem (
  l_orderkey INT NOT NULL,
  l_partkey INT NOT NULL,
  l_suppkey INT NOT NULL,
  l_linenumber INT NOT NULL,
  l_quantity DECIMAL(15,2) NOT NULL,
  l_extendedprice DECIMAL(15,2) NOT NULL,
  l_discount DECIMAL(15,2) NOT NULL,
  l_tax DECIMAL(15,2) NOT NULL,
  l_returnflag CHAR(1) NOT NULL,
  l_linestatus CHAR(1) NOT NULL,
  l_shipdate DATE NOT NULL,
  l_commitdate DATE NOT NULL,
  l_receiptdate DATE NOT NULL,
  l_shipinstruct CHAR(25) NOT NULL,
  l_shipmode CHAR(10) NOT NULL,
  l_comment VARCHAR(44) NOT NULL
) ENGINE=InnoDB;
SQL
  } >"${output}"
}

load_table() {
  local table="$1"
  local columns="$2"
  local data_file="${DATA_DIR}/${table}.tbl"
  local data_literal
  data_literal="$(sql_string "${data_file}")"
  echo "loading ${table} from ${data_file}"
  run_mysql_db --show-warnings -e "LOAD DATA LOCAL INFILE ${data_literal} INTO TABLE $(quote_ident "${table}") FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (${columns}, @discard);"
}

load_tables() {
  load_table region "r_regionkey, r_name, r_comment"
  load_table nation "n_nationkey, n_name, n_regionkey, n_comment"
  load_table supplier "s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment"
  load_table customer "c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment"
  load_table part "p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment"
  load_table partsupp "ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment"
  load_table orders "o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment"
  load_table lineitem "l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment"
}

write_index_sql() {
  local output="$1"
  local db
  db="$(quote_ident "${MYSQL_DATABASE}")"
  {
    echo "USE ${db};"
    if [[ "${MYSQL_INDEX_PROFILE}" == "pk" || "${MYSQL_INDEX_PROFILE}" == "benchmark" ]]; then
      cat <<'SQL'
ALTER TABLE region ADD PRIMARY KEY (r_regionkey);
ALTER TABLE nation ADD PRIMARY KEY (n_nationkey);
ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey);
ALTER TABLE customer ADD PRIMARY KEY (c_custkey);
ALTER TABLE part ADD PRIMARY KEY (p_partkey);
ALTER TABLE partsupp ADD PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE orders ADD PRIMARY KEY (o_orderkey);
ALTER TABLE lineitem ADD PRIMARY KEY (l_orderkey, l_linenumber);
SQL
    fi
    if [[ "${MYSQL_INDEX_PROFILE}" == "benchmark" ]]; then
      cat <<'SQL'
CREATE INDEX idx_nation_region ON nation(n_regionkey);
CREATE INDEX idx_supplier_nation ON supplier(s_nationkey);
CREATE INDEX idx_customer_nation ON customer(c_nationkey);
CREATE INDEX idx_customer_mktsegment ON customer(c_mktsegment);
CREATE INDEX idx_orders_custkey ON orders(o_custkey);
CREATE INDEX idx_orders_orderdate ON orders(o_orderdate);
CREATE INDEX idx_lineitem_orderkey ON lineitem(l_orderkey);
CREATE INDEX idx_lineitem_part_supp ON lineitem(l_partkey, l_suppkey);
CREATE INDEX idx_lineitem_suppkey ON lineitem(l_suppkey);
CREATE INDEX idx_lineitem_shipdate ON lineitem(l_shipdate);
CREATE INDEX idx_lineitem_receiptdate ON lineitem(l_receiptdate);
CREATE INDEX idx_lineitem_commit_receipt ON lineitem(l_commitdate, l_receiptdate);
CREATE INDEX idx_part_brand_container ON part(p_brand, p_container);
CREATE INDEX idx_part_size ON part(p_size);
CREATE INDEX idx_part_type ON part(p_type);
CREATE INDEX idx_partsupp_suppkey ON partsupp(ps_suppkey);
CREATE INDEX idx_partsupp_partkey ON partsupp(ps_partkey);
SQL
    fi
  } >"${output}"
}

create_schema() {
  local ddl
  ddl="$(mktemp)"
  write_schema_sql "${ddl}"
  run_mysql <"${ddl}"
  rm -f "${ddl}"
}

create_indexes() {
  if [[ "${MYSQL_INDEX_PROFILE}" == "none" ]]; then
    echo "skipping indexes MYSQL_INDEX_PROFILE=none"
    return 0
  fi
  local index_sql
  index_sql="$(mktemp)"
  write_index_sql "${index_sql}"
  run_mysql <"${index_sql}"
  rm -f "${index_sql}"
}

validate_counts() {
  local failures=0
  for table in "${TABLES[@]}"; do
    local expected actual
    expected="$(wc -l < "${DATA_DIR}/${table}.tbl" | tr -d '[:space:]')"
    actual="$(run_mysql_db -N -B -e "SELECT COUNT(*) FROM $(quote_ident "${table}");" | tail -n 1 | tr -d '[:space:]')"
    if [[ "${actual}" == "${expected}" ]]; then
      echo "PASS count ${table} expected=${expected} actual=${actual}"
    else
      echo "FAIL count ${table} expected=${expected} actual=${actual}" >&2
      failures=$((failures + 1))
    fi
  done
  if [[ "${failures}" -ne 0 ]]; then
    return 1
  fi
}

echo "TPC-H MySQL load"
echo "data_dir=${DATA_DIR}"
echo "target=${MYSQL_HOST}:${MYSQL_PORT}"
echo "database=${MYSQL_DATABASE}"
echo "user=${MYSQL_USER}"
echo "reset=${MYSQL_RESET}"
echo "index_profile=${MYSQL_INDEX_PROFILE}"
echo "run_counts=${RUN_COUNTS}"
echo

if local_infile="$(run_mysql -N -B -e "SHOW GLOBAL VARIABLES LIKE 'local_infile';" 2>/dev/null | awk '{print $2}' | tail -n 1)"; then
  if [[ "${local_infile}" != "ON" && "${local_infile}" != "1" ]]; then
    echo "warning: MySQL global local_infile appears to be ${local_infile:-unknown}; LOAD DATA LOCAL INFILE may fail" >&2
  fi
fi

run_step create_schema create_schema
run_step load_tables load_tables
run_step create_indexes create_indexes
if is_enabled "${RUN_COUNTS}"; then
  run_step validate_counts validate_counts
fi

echo "MySQL TPC-H load complete"
echo "SQLRunner DSN example:"
echo "  MYSQL_DSN='${MYSQL_USER}:<password>@tcp(${MYSQL_HOST}:${MYSQL_PORT})/${MYSQL_DATABASE}'"
