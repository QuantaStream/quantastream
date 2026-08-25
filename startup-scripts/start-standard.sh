#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
cd "$repo_root"

CONFIG_DIR="${QUANTASTREAM_CONFIG_DIR:-configuration}"
DATA_DIR="${QUANTASTREAM_DATA_DIR:-data}"
WAL_PATH="${QUANTASTREAM_WAL_PATH:-}"
BIND_ADDRESS="${QUANTASTREAM_BIND:-127.0.0.1}"
MYSQL_PORT="${QUANTASTREAM_MYSQL_PORT:-4000}"
NATIVE_GRPC_BIND="${QUANTASTREAM_NATIVE_GRPC_BIND:-}"
NATIVE_GRPC_PORT="${QUANTASTREAM_NATIVE_GRPC_PORT:-0}"
DATABASE="${QUANTASTREAM_DATABASE:-quanta}"
AUTH_MODE="${QUANTASTREAM_AUTH_MODE:-permissive}"
AUTH_USER="${QUANTASTREAM_AUTH_USER:-}"
AUTH_PASSWORD="${QUANTASTREAM_AUTH_PASSWORD:-}"
AUTH_ACCOUNT_FILE="${QUANTASTREAM_AUTH_ACCOUNT_FILE:-}"
RUNTIME_PROBES="${QUANTASTREAM_RUNTIME_PROBES:-false}"

usage() {
  cat <<'EOF'
Usage: ./start-standard.sh

Environment:
  QUANTASTREAM_CONFIG_DIR   Schema/catalog config directory. Defaults to configuration.
  QUANTASTREAM_DATA_DIR     Local inabox-standard data directory. Defaults to data.
  QUANTASTREAM_WAL_PATH     Optional local write-ahead log path. Disabled when empty.
  QUANTASTREAM_BIND         MySQL bind address. Defaults to 127.0.0.1.
  QUANTASTREAM_MYSQL_PORT   MySQL listen port. Defaults to 4000.
  QUANTASTREAM_NATIVE_GRPC_BIND
                           Native node gRPC bind address. Defaults to QUANTASTREAM_BIND.
  QUANTASTREAM_NATIVE_GRPC_PORT
                           Native node gRPC listen port for high-throughput loaders.
                           Defaults to 0, which disables the listener.
  QUANTASTREAM_DATABASE     Default database/schema. Defaults to quanta.
  QUANTASTREAM_AUTH_MODE    MySQL auth mode: permissive or static. Defaults to permissive.
  QUANTASTREAM_AUTH_USER    Static auth username. Defaults to qstream when static auth is enabled.
  QUANTASTREAM_AUTH_PASSWORD
                           Static auth password. Empty password is allowed.
  QUANTASTREAM_AUTH_ACCOUNT_FILE
                           YAML static auth account file used when auth mode is static.
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
if [[ -n "${WAL_PATH}" ]]; then
  echo "wal=${WAL_PATH}"
else
  echo "wal=disabled"
fi
echo "mysql=${BIND_ADDRESS}:${MYSQL_PORT}"
if [[ "${NATIVE_GRPC_PORT}" != "0" ]]; then
  echo "native_grpc=${NATIVE_GRPC_BIND:-$BIND_ADDRESS}:${NATIVE_GRPC_PORT}"
else
  echo "native_grpc=disabled"
fi
echo "database=${DATABASE}"
echo "auth=${AUTH_MODE}"
if [[ "${AUTH_MODE}" == "static" && -n "${AUTH_USER}" ]]; then
  echo "auth_user=${AUTH_USER}"
fi
if [[ "${AUTH_MODE}" == "static" && -n "${AUTH_ACCOUNT_FILE}" ]]; then
  echo "auth_account_file=${AUTH_ACCOUNT_FILE}"
fi
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
  -wal-path "$WAL_PATH" \
  -bind "$BIND_ADDRESS" \
  -mysql-port "$MYSQL_PORT" \
  -native-grpc-bind "$NATIVE_GRPC_BIND" \
  -native-grpc-port "$NATIVE_GRPC_PORT" \
  -database "$DATABASE" \
  -auth-mode "$AUTH_MODE" \
  -auth-user "$AUTH_USER" \
  -auth-account-file "$AUTH_ACCOUNT_FILE" \
  -runtime-probes="${RUNTIME_PROBES}" &
server_pid="$!"
wait "${server_pid}"
