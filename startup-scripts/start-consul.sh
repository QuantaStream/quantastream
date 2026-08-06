#!/usr/bin/env bash

set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
runtime_dir="$script_dir/.local/run"
data_dir="$script_dir/.local/consul/data"
log_dir="$script_dir/.local/logs"
pid_file="$runtime_dir/consul.pid"
log_file="$log_dir/consul.log"

if ! command -v consul >/dev/null 2>&1; then
	echo "consul is not installed or is not on PATH" >&2
	exit 1
fi

leader="$(curl -fsS http://127.0.0.1:8500/v1/status/leader 2>/dev/null || true)"
if [[ -n "$leader" && "$leader" != '""' ]]; then
	echo "Consul is already running"
	exit 0
fi

mkdir -p "$runtime_dir" "$data_dir" "$log_dir"

if [[ -f "$pid_file" ]]; then
	old_pid="$(cat "$pid_file" 2>/dev/null || true)"
	if [[ -n "$old_pid" ]] && kill -0 "$old_pid" >/dev/null 2>&1; then
		echo "Consul appears to be starting or running with pid $old_pid"
		exit 0
	fi
	rm -f "$pid_file"
fi

echo "Starting durable single-node Consul server in the background"
echo "Consul data directory: $data_dir"
echo "Consul log: $log_file"
setsid consul agent \
	-server \
	-bootstrap-expect=1 \
	-node=quanta-local-consul \
	-client=127.0.0.1 \
	-bind=127.0.0.1 \
	-data-dir="$data_dir" \
	-log-level=warn \
	>"$log_file" 2>&1 &
pid="$!"
echo "$pid" > "$pid_file"

for _ in {1..300}; do
	leader="$(curl -fsS http://127.0.0.1:8500/v1/status/leader 2>/dev/null || true)"
	if [[ -n "$leader" && "$leader" != '""' ]]; then
		echo "Consul started with pid $pid"
		exit 0
	fi
	sleep 0.1
done

echo "Consul did not become ready; stopping pid $pid" >&2
tail -n 40 "$log_file" >&2 || true
if kill -0 "$pid" >/dev/null 2>&1; then
	kill "$pid" >/dev/null 2>&1 || true
	for _ in {1..20}; do
		if ! kill -0 "$pid" >/dev/null 2>&1; then
			break
		fi
		sleep 0.1
	done
	if kill -0 "$pid" >/dev/null 2>&1; then
		kill -9 "$pid" >/dev/null 2>&1 || true
	fi
fi
rm -f "$pid_file"
exit 1
