#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
cd "$repo_root"

CONFIG_DIR="${QUANTASTREAM_CONFIG_DIR:-configuration}"
DATA_DIR="${QUANTASTREAM_DATA_DIR:-data}"
BIND_ADDRESS="${QUANTASTREAM_BIND:-127.0.0.1}"
MYSQL_PORT="${QUANTASTREAM_MYSQL_PORT:-4000}"
DATABASE="${QUANTASTREAM_DATABASE:-quanta}"
RUNTIME_PROBES="${QUANTASTREAM_RUNTIME_PROBES:-false}"

usage() {
  cat <<'EOF'
Usage: ./start-standard.sh

Environment:
  QUANTASTREAM_CONFIG_DIR   Schema/catalog config directory. Defaults to configuration.
  QUANTASTREAM_DATA_DIR     Local inabox-standard data directory. Defaults to data.
  QUANTASTREAM_BIND         MySQL bind address. Defaults to 127.0.0.1.
  QUANTASTREAM_MYSQL_PORT   MySQL listen port. Defaults to 4000.
  QUANTASTREAM_DATABASE     Default database/schema. Defaults to quanta.
  QUANTASTREAM_RUNTIME_PROBES
                           Set to true to log runtime execution probes.

Examples:
  ./start-standard.sh

  QUANTASTREAM_CONFIG_DIR=tpc-h-benchmark/config \
  QUANTASTREAM_DATA_DIR=tpc-h-benchmark/local/standard-data \
    ./start-standard.sh
EOF
}

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

echo "Starting inabox-standard process"
echo "config_dir=${CONFIG_DIR}"
echo "data_dir=${DATA_DIR}"
echo "mysql=${BIND_ADDRESS}:${MYSQL_PORT}"
echo "database=${DATABASE}"
echo "runtime_probes=${RUNTIME_PROBES}"

build_dir="$(mktemp -d "${TMPDIR:-/tmp}/quantastream-standard.XXXXXX")"
server_bin="${build_dir}/quantastream"
server_pid=""

cleanup() {
  local exit_code=$?
  if [[ -n "${server_pid}" ]] && kill -0 "${server_pid}" >/dev/null 2>&1; then
    kill "${server_pid}" >/dev/null 2>&1 || true
    wait "${server_pid}" >/dev/null 2>&1 || true
  fi
  rm -rf "${build_dir}"
  exit "${exit_code}"
}

terminate() {
  local signal="$1"
  if [[ -n "${server_pid}" ]] && kill -0 "${server_pid}" >/dev/null 2>&1; then
    kill "-${signal}" "${server_pid}" >/dev/null 2>&1 || true
    wait "${server_pid}" >/dev/null 2>&1 || true
  fi
  rm -rf "${build_dir}"
  exit 0
}

trap cleanup EXIT
trap 'terminate TERM' TERM
trap 'terminate INT' INT

go build -o "${server_bin}" ./cmd/quantastream

"${server_bin}" \
  -config-dir "$CONFIG_DIR" \
  -data-dir "$DATA_DIR" \
  -bind "$BIND_ADDRESS" \
  -mysql-port "$MYSQL_PORT" \
  -database "$DATABASE" \
  -runtime-probes="${RUNTIME_PROBES}" &
server_pid="$!"
wait "${server_pid}"
