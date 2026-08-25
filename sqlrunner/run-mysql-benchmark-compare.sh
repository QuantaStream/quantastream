#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  MYSQL_DSN='user:pass@tcp(mysql-host:3306)/db' ./run-mysql-benchmark-compare.sh

Environment:
  MYSQL_DSN                 Required database/sql DSN for the stock MySQL reference.
  MYSQL_DRIVER              database/sql driver. Defaults to mysql.
  SUITE_FILE                SQLRunner suite. Defaults to ../tpc-h-benchmark/sqltests/tpch_benchmark_readonly.yaml.
  TARGET_ENGINE             QuantaStream target engine. Defaults to inabox-standard.
  TARGET_HOST               QuantaStream host. Defaults to QUANTA_HOST or 127.0.0.1.
  TARGET_PORT               QuantaStream port. Defaults to QUANTA_PORT or 4000.
  TARGET_USER               QuantaStream user. Defaults to QUANTA_USER or qstream.
  TARGET_PASSWORD           QuantaStream password. Defaults to QUANTA_PASSWORD or empty.
  TARGET_DB                 QuantaStream database. Defaults to QUANTA_DB or quanta.
  TARGET_CONSUL             Consul address for engines that need it. Defaults to QUANTA_CONSUL or 127.0.0.1:8500.
  BENCHMARK_PROFILE         Profile label. Defaults to mysql-reference-compare.
  BENCHMARK_WARMUP          Warm-up suite runs. Defaults to 1.
  BENCHMARK_RUNS            Measured suite runs. Defaults to 3.
  BENCHMARK_LIMIT           Comparison rows to print. Defaults to 20. Use 0 for all.
  BENCHMARK_METADATA        Additional comma-separated key=value metadata.
  BENCHMARK_DATASET         Dataset label. Defaults to tpch for TPC-H suites.
  BENCHMARK_SCALE_FACTOR    Dataset scale factor label. Falls back to TPCH_SCALE_FACTOR or SCALE_FACTOR.
  BENCHMARK_OUTPUT_DIR      Output directory. Defaults to expected/local/mysql-benchmarks/<timestamp>.
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/benchmark_metadata.sh"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"

MYSQL_DRIVER="${MYSQL_DRIVER:-mysql}"
SUITE_FILE="${SUITE_FILE:-../tpc-h-benchmark/sqltests/tpch_benchmark_readonly.yaml}"
TARGET_ENGINE="${TARGET_ENGINE:-inabox-standard}"
TARGET_HOST="${TARGET_HOST:-${QUANTA_HOST:-127.0.0.1}}"
TARGET_PORT="${TARGET_PORT:-${QUANTA_PORT:-4000}}"
TARGET_USER="${TARGET_USER:-${QUANTA_USER:-qstream}}"
TARGET_PASSWORD="${TARGET_PASSWORD:-${QUANTA_PASSWORD:-}}"
TARGET_DB="${TARGET_DB:-${QUANTA_DB:-quanta}}"
TARGET_CONSUL="${TARGET_CONSUL:-${QUANTA_CONSUL:-127.0.0.1:8500}}"
BENCHMARK_PROFILE="${BENCHMARK_PROFILE:-mysql-reference-compare}"
BENCHMARK_WARMUP="${BENCHMARK_WARMUP:-1}"
BENCHMARK_RUNS="${BENCHMARK_RUNS:-3}"
BENCHMARK_LIMIT="${BENCHMARK_LIMIT:-20}"
BENCHMARK_OUTPUT_DIR="${BENCHMARK_OUTPUT_DIR:-${SCRIPT_DIR}/expected/local/mysql-benchmarks/${RUN_ID}}"

reference_metadata="$(benchmark_metadata_join "$(benchmark_base_metadata "$SUITE_FILE" "mysql-reference")" "reference=mysql,target=${TARGET_ENGINE}")"
reference_metadata="$(benchmark_metadata_join "$reference_metadata" "${BENCHMARK_METADATA:-}")"
target_metadata="$(benchmark_metadata_join "$(benchmark_base_metadata "$SUITE_FILE" "$TARGET_ENGINE" "$TARGET_HOST" "$TARGET_PORT")" "reference=mysql,target=${TARGET_ENGINE}")"
target_metadata="$(benchmark_metadata_join "$target_metadata" "${BENCHMARK_METADATA:-}")"

mkdir -p "$BENCHMARK_OUTPUT_DIR"

reference_report="${BENCHMARK_OUTPUT_DIR}/mysql-reference.json"
target_report="${BENCHMARK_OUTPUT_DIR}/${TARGET_ENGINE}.json"
comparison_report="${BENCHMARK_OUTPUT_DIR}/comparison.md"

echo "===== benchmark reference=mysql-reference report=${reference_report} ====="
(cd "$SCRIPT_DIR" && go run . \
  -engine mysql-reference \
  -suite_file "$SUITE_FILE" \
  -mysql_driver "$MYSQL_DRIVER" \
  -mysql_dsn "$MYSQL_DSN" \
  -benchmark_profile "$BENCHMARK_PROFILE" \
  -benchmark_warmup "$BENCHMARK_WARMUP" \
  -benchmark_runs "$BENCHMARK_RUNS" \
  -benchmark_metadata "$reference_metadata" \
  -benchmark_report "$reference_report" \
  -precise_timing)

echo "===== benchmark target=${TARGET_ENGINE} report=${target_report} ====="
(cd "$SCRIPT_DIR" && go run . \
  -engine "$TARGET_ENGINE" \
  -suite_file "$SUITE_FILE" \
  -host "$TARGET_HOST" \
  -port "$TARGET_PORT" \
  -user "$TARGET_USER" \
  -password "$TARGET_PASSWORD" \
  -db "$TARGET_DB" \
  -consul "$TARGET_CONSUL" \
  -benchmark_profile "$BENCHMARK_PROFILE" \
  -benchmark_warmup "$BENCHMARK_WARMUP" \
  -benchmark_runs "$BENCHMARK_RUNS" \
  -benchmark_metadata "$target_metadata" \
  -benchmark_report "$target_report" \
  -precise_timing)

(cd "$SCRIPT_DIR" && go run . \
  -benchmark_compare "${reference_report},${target_report}" \
  -benchmark_limit "$BENCHMARK_LIMIT") | tee "$comparison_report"

echo "mysql benchmark comparison written to ${comparison_report}"
