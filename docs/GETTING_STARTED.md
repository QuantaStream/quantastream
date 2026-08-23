# Getting Started With QuantaStream Binaries

This runbook starts a local single-node QuantaStream engine from a release
bundle, writes one row through the JSON loader, reads it through the
MySQL-compatible endpoint, and proves a local backup/restore path.

Every command block below is intended to be pasted as-is into a Linux shell. The
commands assume `bash`, `curl`, and the `mysql` command-line client are
available.

Use the full product name in docs and support tickets; the binaries use the
shorter `qstream-*` names where the full name would be unwieldy.

## 1. Unpack The Release

Run this from the directory containing the downloaded release archive:

```bash
set -Eeuo pipefail

archive="$(ls -1 qstream-*-linux-amd64.tar.gz | tail -n 1)"
sha256sum -c SHA256SUMS --ignore-missing
tar -xzf "$archive"
cd "$(tar -tzf "$archive" | sed -n '1s#/.*##p')"

./bin/quantastream -version
./bin/qstream-admin version
./bin/qstream-loader -version
./bin/qstream-stream-loader -version
```

## 2. Prepare Runtime Files

The packaged `configuration/` directory contains configuration documentation and
schema reference material. Runtime table schemas should live in a separate
directory owned by the deployment. This runbook uses `./runtime/config`.

The following block resets only the local smoke directories inside the extracted
release bundle.

```bash
set -Eeuo pipefail

rm -rf ./data ./data-restored ./backups ./auth ./runtime ./logs ./qstream-support.tar.gz
mkdir -p data backups auth runtime/config/release_smoke logs

cat > ./runtime/config/release_smoke/schema.yaml <<'YAML'
tableName: release_smoke
selector: type = "release_smoke"
primaryKey: id
attributes:
- sourceName: /id
  fieldName: id
  mappingStrategy: StringLexBSI
  configuration:
    length: "0"
  type: String
- sourceName: /name
  fieldName: name
  mappingStrategy: StringEnum
  type: String
- sourceName: /score
  fieldName: score
  mappingStrategy: IntBSI
  type: Integer
- sourceName: /latitude
  fieldName: latitude
  mappingStrategy: FloatScaleBSI
  type: Float
  scale: 4
YAML

QUANTASTREAM_AUTH_PASSWORD=root \
./bin/qstream-admin auth upsert \
  --account-file ./auth/accounts.yaml \
  --user root \
  --default-database quanta

./bin/qstream-admin access upsert \
  --policy-file ./auth/access-policy.yaml \
  --principal-kind user \
  --principal root \
  --privilege select \
  --table '*'

./bin/qstream-admin auth validate \
  --account-file ./auth/accounts.yaml

./bin/qstream-admin access validate \
  --policy-file ./auth/access-policy.yaml

test -f ./runtime/config/release_smoke/schema.yaml
```

The account file stores verifier hashes, not the cleartext password. Use a real
password and service-secret mechanism outside local smoke tests.

## 3. Start The Single-Node Engine

This block starts the engine in the background, records its process ID under
`./runtime/quantastream.pid`, and waits for both local ports to open.

```bash
set -Eeuo pipefail

for port in 4000 4100; do
  if (echo > "/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
    echo "port already in use: $port" >&2
    exit 1
  fi
done

./bin/quantastream \
  -config-dir ./runtime/config \
  -data-dir ./data \
  -wal-path ./data/storage.wal \
  -bind 127.0.0.1 \
  -mysql-port 4000 \
  -native-grpc-bind 127.0.0.1 \
  -native-grpc-port 4100 \
  -database quanta \
  -auth-mode static \
  -auth-account-file ./auth/accounts.yaml \
  -access-policy-file ./auth/access-policy.yaml \
  > ./logs/quantastream.log 2>&1 &

echo "$!" > ./runtime/quantastream.pid
echo "engine_pid=$(cat ./runtime/quantastream.pid)"

for port in 4000 4100; do
  ready=0
  for _ in $(seq 1 45); do
    if (echo > "/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then
    echo "engine did not open port $port" >&2
    tail -80 ./logs/quantastream.log >&2 || true
    exit 1
  fi
done

tail -20 ./logs/quantastream.log
```

Expected startup lines include:

```text
mode=inabox-standard
mysql=127.0.0.1:4000
native_grpc=127.0.0.1:4100
listening=127.0.0.1:4000
native_grpc_listening=127.0.0.1:4100
```

The `-wal-path` flag enables the local write-ahead log and checkpoint file under
the data directory. Keeping the WAL inside `./data` makes local backups
self-contained.

## 4. Inspect The Running Engine

```bash
set -Eeuo pipefail

./bin/qstream-admin wal validate \
  --path ./data/storage.wal

./bin/qstream-admin wal plan \
  --path ./data/storage.wal

./bin/qstream-admin doctor local \
  --data-dir ./data \
  --config-dir ./runtime/config \
  --wal-path ./data/storage.wal \
  --auth-account-file ./auth/accounts.yaml \
  --access-policy-file ./auth/access-policy.yaml \
  --mysql-addr 127.0.0.1:4000 \
  --native-grpc-addr 127.0.0.1:4100
```

On this smoke setup, `doctor local` should report one table schema from
`./runtime/config`.

## 5. Connect With A MySQL Client

```bash
mysql -h 127.0.0.1 -P 4000 -u root -proot -D quanta \
  -e "select @@version, @@version_comment; show tables;"
```

The version comment includes the QuantaStream build identity when the binary was
created by the release script.

## 6. Start The JSON Loader

The loader accepts JSON event batches and writes them through the native gRPC
endpoint exposed by the single-node engine.

```bash
set -Eeuo pipefail

if (echo > /dev/tcp/127.0.0.1/8088) >/dev/null 2>&1; then
  echo "port already in use: 8088" >&2
  exit 1
fi

./bin/qstream-loader \
  -listen 127.0.0.1:8088 \
  -config-dir ./runtime/config \
  -database quanta \
  -tables release_smoke \
  -connection-mode standard-native \
  -native-grpc-addr 127.0.0.1:4100 \
  -flush-interval 100ms \
  > ./logs/qstream-loader.log 2>&1 &

echo "$!" > ./runtime/qstream-loader.pid
echo "loader_pid=$(cat ./runtime/qstream-loader.pid)"

ready=0
for _ in $(seq 1 45); do
  if curl -fsS http://127.0.0.1:8088/healthz >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "JSON loader did not become ready" >&2
  tail -80 ./logs/qstream-loader.log >&2 || true
  exit 1
fi

curl -fsS http://127.0.0.1:8088/healthz
```

## 7. Send A JSON Smoke Batch

```bash
set -Eeuo pipefail

curl -fsS http://127.0.0.1:8088/ingest/json \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {
        "mode": "stream",
        "event_id": "release-smoke-row-1",
        "source": "getting-started",
        "event_time": "2026-08-21T00:00:00Z",
        "source_offset": "getting-started:1",
        "shard_key": "release-smoke-row-1",
        "payload": {
          "type": "release_smoke",
          "id": "release-smoke-row-1",
          "name": "Release Smoke",
          "score": 1,
          "latitude": 9.9281
        }
      }
    ]
  }'

for _ in $(seq 1 30); do
  row_count="$(mysql -h 127.0.0.1 -P 4000 -u root -proot -D quanta \
    --batch --skip-column-names \
    -e "select count(*) from release_smoke where id = 'release-smoke-row-1';" 2>/dev/null | tr -d '[:space:]')"
  if [ "$row_count" = "1" ]; then
    break
  fi
  sleep 1
done

if [ "${row_count:-0}" != "1" ]; then
  echo "release_smoke row was not visible through MySQL" >&2
  tail -80 ./logs/qstream-loader.log >&2 || true
  tail -80 ./logs/quantastream.log >&2 || true
  exit 1
fi

mysql -h 127.0.0.1 -P 4000 -u root -proot -D quanta \
  -e "select id, name, score, latitude from release_smoke;"
```

For TPC-H experiments, use the `qstream-tpch-loader` binary with the TPC-H
configuration and data directories. The TPC-H runbooks in `docs/TPCH.md` and
`docs/DEPLOYMENT.md` describe the larger benchmark flow.

## 8. Create And Prove A Local Backup

For this first local snapshot path, make sure the source engine has committed
or drained any recent writes before taking the backup. The backup command
quiesces new storage mutations while it copies, but it snapshots durable
filesystem state. The command below asks the running single-node engine to
commit dirty buffers after the quiescence barrier is active and before copying
the snapshot.

This smoke stops the JSON loader before the backup so the data set is stable.

```bash
set -Eeuo pipefail

if [ -f ./runtime/qstream-loader.pid ]; then
  kill "$(cat ./runtime/qstream-loader.pid)" 2>/dev/null || true
  rm -f ./runtime/qstream-loader.pid
fi

rm -rf ./backups/smoke-backup

./bin/qstream-admin backup create \
  --data-dir ./data \
  --target file://$PWD/backups/smoke-backup \
  --quiesce \
  --engine-flush standard-native \
  --native-grpc-addr 127.0.0.1:4100 \
  --wal-path ./data/storage.wal

./bin/qstream-admin backup inspect \
  --source file://$PWD/backups/smoke-backup

./bin/qstream-admin backup validate \
  --source file://$PWD/backups/smoke-backup

./bin/qstream-admin backup smoke \
  --source file://$PWD/backups/smoke-backup
```

Use `backup inspect` for quick manifest triage, `backup validate` to prove the
snapshot bytes, and `backup smoke` to restore into a temporary directory and
validate the restored image. The smoke command removes its temporary restore
directory unless `--keep-restore-dir` is supplied.

Distributed engine flush is currently a commit primitive before a local
snapshot, not a coordinated multi-node backup/restore suite.

To perform a manual restore, stop the engine first and restore into a new empty
directory:

```bash
set -Eeuo pipefail

if [ -f ./runtime/quantastream.pid ]; then
  kill "$(cat ./runtime/quantastream.pid)" 2>/dev/null || true
  rm -f ./runtime/quantastream.pid
fi

rm -rf ./data-restored

./bin/qstream-admin backup restore \
  --source file://$PWD/backups/smoke-backup \
  --data-dir ./data-restored
```

## 9. Optional Systemd Templates

Release bundles include single-node systemd examples under `examples/systemd/`.
They are templates, not installers:

```bash
sudo useradd --system --home /var/lib/quantastream --shell /usr/sbin/nologin quantastream
sudo install -d -o quantastream -g quantastream /etc/quantastream /var/lib/quantastream
sudo cp -R ./runtime/config /etc/quantastream/configuration
sudo cp ./auth/accounts.yaml /etc/quantastream/accounts.yaml
sudo cp ./auth/access-policy.yaml /etc/quantastream/access-policy.yaml
sudo cp ./examples/systemd/qstream-single-node.env /etc/quantastream/qstream-single-node.env
sudo cp ./examples/systemd/qstream-single-node.service /etc/systemd/system/qstream-single-node.service
```

Edit `/etc/quantastream/qstream-single-node.env` for your installation paths,
then run `systemctl daemon-reload` and start the service when ready.

## 10. Stop Local Processes

```bash
set -Eeuo pipefail

if [ -f ./runtime/qstream-loader.pid ]; then
  kill "$(cat ./runtime/qstream-loader.pid)" 2>/dev/null || true
  rm -f ./runtime/qstream-loader.pid
fi

if [ -f ./runtime/quantastream.pid ]; then
  kill "$(cat ./runtime/quantastream.pid)" 2>/dev/null || true
  rm -f ./runtime/quantastream.pid
fi
```

The local data lives under `./data`.

For support, capture a diagnostic bundle:

```bash
./bin/qstream-admin support bundle \
  --output ./qstream-support.tar.gz \
  --data-dir ./data \
  --wal-path ./data/storage.wal \
  --auth-account-file ./auth/accounts.yaml \
  --access-policy-file ./auth/access-policy.yaml \
  --backup-source file://$PWD/backups/smoke-backup \
  --log-path ./logs/quantastream.log,./logs/qstream-loader.log
```

The support bundle includes version/runtime metadata, a catalog/config summary,
redacted static security validation, WAL planning output, backup manifests,
optional log tails, and best-effort Consul service-discovery status. It does not
include table data files or raw auth/access policy files. For production support
collection, use the first response runbook in `docs/DEPLOYMENT.md`.

For quick manual triage, capture these lines:

```bash
./bin/quantastream -version
./bin/qstream-admin version
./bin/qstream-admin backup inspect --source file://$PWD/backups/smoke-backup
./bin/qstream-admin wal plan --path ./data/storage.wal
mysql -h 127.0.0.1 -P 4000 -u root -proot -D quanta \
  -e "select @@version, @@version_comment;"
```
