#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

SERVICE_NAME="${SERVICE_NAME:-quantastream-node}"
SERVICE_USER="${SERVICE_USER:-${SUDO_USER:-$(id -un)}}"
INSTALL_DIR="${INSTALL_DIR:-/opt/quantastream/bin}"
ENV_DIR="${ENV_DIR:-/etc/quantastream}"
DATA_DIR="${QUANTASTREAM_DATA_DIR:-$repo_root/tpc-h-benchmark/local/distributed-data}"
NODE_HASH_KEY="${QUANTASTREAM_NODE_HASH_KEY:-$(hostname -s)}"
NODE_BIND="${QUANTASTREAM_NODE_BIND:-0.0.0.0}"
NODE_PORT="${QUANTASTREAM_NODE_PORT:-4400}"
CONSUL_ENDPOINT="${QUANTASTREAM_CONSUL_ENDPOINT:-127.0.0.1:8500}"
ENVIRONMENT="${QUANTASTREAM_ENV:-PROD}"
LOG_LEVEL="${QUANTASTREAM_LOG_LEVEL:-INFO}"
PPROF="${QUANTASTREAM_PPROF:-false}"
SKIP_NODE_SYNC="${QUANTASTREAM_SKIP_NODE_SYNC:-1}"
REQUIRE_LOCAL_CONSUL="${QUANTASTREAM_REQUIRE_LOCAL_CONSUL:-auto}"
REVERSE_ARTIFACT_WARM_MEMBERSHIP_TIMEOUT="${QUANTASTREAM_REVERSE_ARTIFACT_WARM_MEMBERSHIP_TIMEOUT:-3m}"
CONSUL_HEALTH_CHECK_PROFILE="${QUANTASTREAM_CONSUL_HEALTH_CHECK_PROFILE:-}"
CONSUL_HEALTH_CHECK_INTERVAL="${QUANTASTREAM_CONSUL_HEALTH_CHECK_INTERVAL:-}"
CONSUL_HEALTH_CHECK_TIMEOUT="${QUANTASTREAM_CONSUL_HEALTH_CHECK_TIMEOUT:-}"
CONSUL_HEALTH_CHECK_FAILURES_BEFORE_CRITICAL="${QUANTASTREAM_CONSUL_HEALTH_CHECK_FAILURES_BEFORE_CRITICAL:-}"
CONSUL_HEALTH_CHECK_DEREGISTER_AFTER="${QUANTASTREAM_CONSUL_HEALTH_CHECK_DEREGISTER_AFTER:-}"
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
Usage: sudo ./startup-scripts/install-distributed-node-service.sh

Builds quantastream-node and installs a systemd service for distributed data
nodes. Override configuration with environment variables, for example:

  QUANTASTREAM_NODE_HASH_KEY=qs-server-1 \
  QUANTASTREAM_DATA_DIR=/home/ubuntu/quantastream/tpc-h-benchmark/local/distributed-data \
    sudo -E ./startup-scripts/install-distributed-node-service.sh

Environment:
  SERVICE_NAME                  systemd unit name. Defaults to quantastream-node.
  SERVICE_USER                  Linux user running the service. Defaults to sudo caller.
  INSTALL_DIR                   binary install directory. Defaults to /opt/quantastream/bin.
  ENV_DIR                       environment file directory. Defaults to /etc/quantastream.
  QUANTASTREAM_NODE_HASH_KEY    consistent-hash key. Defaults to hostname -s.
  QUANTASTREAM_DATA_DIR         data directory. Defaults to tpc-h-benchmark/local/distributed-data.
  QUANTASTREAM_NODE_BIND        node bind address. Defaults to 0.0.0.0.
  QUANTASTREAM_NODE_PORT        node service port. Defaults to 4400.
  QUANTASTREAM_CONSUL_ENDPOINT  local Consul agent endpoint. Defaults to 127.0.0.1:8500.
  QUANTASTREAM_REQUIRE_LOCAL_CONSUL
                                1/0/auto. Defaults to auto, requiring consul.service
                                only when the Consul endpoint is localhost.
  QUANTASTREAM_SKIP_NODE_SYNC    skip legacy peer sync on startup and mark node active. Defaults to 1.
  QUANTASTREAM_REVERSE_ARTIFACT_WARM_MEMBERSHIP_TIMEOUT
                                wait for full membership before warming owned
                                reverse artifact caches. Defaults to 3m. Use 0
                                to skip the wait.
  QUANTASTREAM_CONSUL_HEALTH_CHECK_PROFILE
                                Consul health-check profile. Empty/production
                                uses fast failure detection. Use bulk-load for
                                large benchmark loads.
  QUANTASTREAM_CONSUL_HEALTH_CHECK_INTERVAL
                                Consul gRPC health-check interval. Empty uses
                                the node default.
  QUANTASTREAM_CONSUL_HEALTH_CHECK_TIMEOUT
                                Consul gRPC health-check timeout. Empty leaves
                                Consul's default behavior.
  QUANTASTREAM_CONSUL_HEALTH_CHECK_FAILURES_BEFORE_CRITICAL
                                Consecutive health failures before critical.
                                Empty or 0 leaves Consul's default behavior.
  QUANTASTREAM_CONSUL_HEALTH_CHECK_DEREGISTER_AFTER
                                Time critical before service deregistration.
                                Empty leaves Consul's default behavior.
  GO_BIN                        optional absolute path to the Go binary.
  ENABLE_NOW=0                  install and enable without starting immediately.
EOF
}

is_local_consul_endpoint() {
  case "$CONSUL_ENDPOINT" in
    127.0.0.1:*|localhost:*|::1:*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

should_require_local_consul() {
  case "$REQUIRE_LOCAL_CONSUL" in
    1|true|TRUE|yes|YES|on|ON)
      return 0
      ;;
    0|false|FALSE|no|NO|off|OFF)
      return 1
      ;;
    auto|AUTO|"")
      is_local_consul_endpoint
      return
      ;;
    *)
      echo "Invalid QUANTASTREAM_REQUIRE_LOCAL_CONSUL=$REQUIRE_LOCAL_CONSUL; use 1, 0, or auto." >&2
      exit 2
      ;;
  esac
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

mkdir -p "$INSTALL_DIR" "$ENV_DIR" "$DATA_DIR"
chown "$SERVICE_USER":"$SERVICE_USER" "$DATA_DIR"
GO_BIN="$(resolve_go)"
"$GO_BIN" -C "$repo_root" build -o "$INSTALL_DIR/quantastream-node" ./qstream-node.go
chmod 0755 "$INSTALL_DIR/quantastream-node"
chmod 0755 "$repo_root/startup-scripts/start-distributed-node.sh"

cat > "$ENV_DIR/node.env" <<EOF
QUANTASTREAM_NODE_BIN=$INSTALL_DIR/quantastream-node
QUANTASTREAM_NODE_HASH_KEY=$NODE_HASH_KEY
QUANTASTREAM_DATA_DIR=$DATA_DIR
QUANTASTREAM_NODE_BIND=$NODE_BIND
QUANTASTREAM_NODE_PORT=$NODE_PORT
QUANTASTREAM_CONSUL_ENDPOINT=$CONSUL_ENDPOINT
QUANTASTREAM_ENV=$ENVIRONMENT
QUANTASTREAM_LOG_LEVEL=$LOG_LEVEL
QUANTASTREAM_PPROF=$PPROF
QUANTASTREAM_SKIP_NODE_SYNC=$SKIP_NODE_SYNC
QUANTASTREAM_REQUIRE_LOCAL_CONSUL=$REQUIRE_LOCAL_CONSUL
QUANTASTREAM_REVERSE_ARTIFACT_WARM_MEMBERSHIP_TIMEOUT=$REVERSE_ARTIFACT_WARM_MEMBERSHIP_TIMEOUT
QUANTASTREAM_CONSUL_HEALTH_CHECK_PROFILE=$CONSUL_HEALTH_CHECK_PROFILE
QUANTASTREAM_CONSUL_HEALTH_CHECK_INTERVAL=$CONSUL_HEALTH_CHECK_INTERVAL
QUANTASTREAM_CONSUL_HEALTH_CHECK_TIMEOUT=$CONSUL_HEALTH_CHECK_TIMEOUT
QUANTASTREAM_CONSUL_HEALTH_CHECK_FAILURES_BEFORE_CRITICAL=$CONSUL_HEALTH_CHECK_FAILURES_BEFORE_CRITICAL
QUANTASTREAM_CONSUL_HEALTH_CHECK_DEREGISTER_AFTER=$CONSUL_HEALTH_CHECK_DEREGISTER_AFTER
EOF
chmod 0644 "$ENV_DIR/node.env"

unit_after="network-online.target"
unit_requires=""
if should_require_local_consul; then
  unit_after="$unit_after consul.service"
  unit_requires="Requires=consul.service"
fi

cat > "/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=QuantaStream distributed data node
After=$unit_after
Wants=network-online.target
$unit_requires

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$repo_root
EnvironmentFile=$ENV_DIR/node.env
ExecStart=$repo_root/startup-scripts/start-distributed-node.sh
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
