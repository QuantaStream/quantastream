#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

SERVICE_NAME="${SERVICE_NAME:-quantastream-proxy}"
SERVICE_USER="${SERVICE_USER:-${SUDO_USER:-$(id -un)}}"
INSTALL_DIR="${INSTALL_DIR:-/opt/quantastream/bin}"
ENV_DIR="${ENV_DIR:-/etc/quantastream}"
BIND_ADDRESS="${QUANTASTREAM_BIND:-0.0.0.0}"
MYSQL_PORT="${QUANTASTREAM_MYSQL_PORT:-4000}"
CONSUL_ENDPOINT="${QUANTASTREAM_CONSUL_ENDPOINT:-127.0.0.1:8500}"
NODE_PORT="${QUANTASTREAM_NODE_PORT:-4400}"
SCHEMA_DIR="${QUANTASTREAM_SCHEMA_DIR:-$repo_root/tpc-h-benchmark/config}"
DATABASE="${QUANTASTREAM_DATABASE:-quanta}"
RUNTIME_PROBES="${QUANTASTREAM_RUNTIME_PROBES:-false}"
SESSION_POOL_SIZE="${QUANTASTREAM_SESSION_POOL_SIZE:-0}"
PPROF_BIND="${QUANTASTREAM_PPROF_BIND:-}"
GRAPH_EQUALITY_ROLE_SEED="${QUANTASTREAM_GRAPH_EQUALITY_ROLE_SEED:-0}"
ENABLE_NOW="${ENABLE_NOW:-1}"

resolve_go() {
  if [[ -n "${GO_BIN:-}" && -x "$GO_BIN" ]]; then
    printf '%s\n' "$GO_BIN"
    return
  fi
  if command -v go >/dev/null 2>&1; then
    command -v go
    return
  fi
  if [[ -n "${SUDO_USER:-}" && "$SUDO_USER" != "root" ]] && command -v runuser >/dev/null 2>&1; then
    local user_go
    user_go="$(runuser -u "$SUDO_USER" -- bash -lc 'command -v go' 2>/dev/null || true)"
    if [[ -n "$user_go" && -x "$user_go" ]]; then
      printf '%s\n' "$user_go"
      return
    fi
  fi
  for candidate in /usr/local/go/bin/go /usr/bin/go /snap/bin/go; do
    if [[ -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return
    fi
  done
  echo "go not found. Set GO_BIN=/path/to/go or install Go for root/sudo PATH." >&2
  exit 2
}

usage() {
  cat <<'EOF'
Usage: sudo ./startup-scripts/install-distributed-proxy-service.sh

Builds quantastream-proxy and installs a systemd service for the
MySQL-compatible distributed-cluster front door.

Environment:
  SERVICE_NAME                  systemd unit name. Defaults to quantastream-proxy.
  SERVICE_USER                  Linux user running the service. Defaults to sudo caller.
  INSTALL_DIR                   binary install directory. Defaults to /opt/quantastream/bin.
  ENV_DIR                       environment file directory. Defaults to /etc/quantastream.
  QUANTASTREAM_BIND             MySQL bind address. Defaults to 0.0.0.0.
  QUANTASTREAM_MYSQL_PORT       MySQL listen port. Defaults to 4000.
  QUANTASTREAM_CONSUL_ENDPOINT  local Consul agent endpoint. Defaults to 127.0.0.1:8500.
  QUANTASTREAM_NODE_PORT        Quanta data-node service port. Defaults to 4400.
  QUANTASTREAM_SCHEMA_DIR       schema/catalog directory.
  QUANTASTREAM_DATABASE         default database/schema. Defaults to quanta.
  QUANTASTREAM_RUNTIME_PROBES   set true to log runtime probes.
  QUANTASTREAM_GRAPH_EQUALITY_ROLE_SEED
                                  set 1 to enable graph equality role seeds. Defaults to 0.
  GO_BIN                        optional absolute path to the Go binary.
  ENABLE_NOW=0                  install and enable without starting immediately.
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

if [[ "$(id -u)" -ne 0 ]]; then
  echo "This installer writes /opt, /etc, and systemd units. Re-run with sudo." >&2
  exit 2
fi

mkdir -p "$INSTALL_DIR" "$ENV_DIR"
GO_BIN="$(resolve_go)"
"$GO_BIN" -C "$repo_root" build -o "$INSTALL_DIR/quantastream-proxy" ./cmd/quantastream-proxy
chmod 0755 "$INSTALL_DIR/quantastream-proxy"
chmod 0755 "$repo_root/startup-scripts/start-distributed-proxy.sh"

cat > "$ENV_DIR/proxy.env" <<EOF
QUANTASTREAM_PROXY_BIN=$INSTALL_DIR/quantastream-proxy
QUANTASTREAM_BIND=$BIND_ADDRESS
QUANTASTREAM_MYSQL_PORT=$MYSQL_PORT
QUANTASTREAM_CONSUL_ENDPOINT=$CONSUL_ENDPOINT
QUANTASTREAM_NODE_PORT=$NODE_PORT
QUANTASTREAM_SCHEMA_DIR=$SCHEMA_DIR
QUANTASTREAM_DATABASE=$DATABASE
QUANTASTREAM_RUNTIME_PROBES=$RUNTIME_PROBES
QUANTASTREAM_SESSION_POOL_SIZE=$SESSION_POOL_SIZE
QUANTASTREAM_PPROF_BIND=$PPROF_BIND
QUANTASTREAM_GRAPH_EQUALITY_ROLE_SEED=$GRAPH_EQUALITY_ROLE_SEED
EOF
chmod 0644 "$ENV_DIR/proxy.env"

cat > "/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=QuantaStream distributed MySQL proxy
After=network-online.target consul.service
Wants=network-online.target
Requires=consul.service

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$repo_root
EnvironmentFile=$ENV_DIR/proxy.env
ExecStart=$repo_root/startup-scripts/start-distributed-proxy.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
if [[ "$ENABLE_NOW" =~ ^(1|true|TRUE|yes|YES|on|ON)$ ]]; then
  systemctl restart "$SERVICE_NAME"
fi
systemctl --no-pager status "$SERVICE_NAME" || true
