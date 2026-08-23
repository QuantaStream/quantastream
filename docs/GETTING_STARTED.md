# Getting Started With QuantaStream Binaries

This runbook starts a local single-node QuantaStream engine from a release
bundle. Use the full product name in docs and support tickets; the binaries use
the shorter `qstream-*` names where the full name would be unwieldy.

## 1. Unpack The Release

```bash
tar -xzf qstream-<version>-linux-amd64.tar.gz
cd qstream-<version>-linux-amd64
```

Verify the binaries:

```bash
./bin/quantastream -version
./bin/qstream-admin version
./bin/qstream-loader -version
./bin/qstream-stream-loader -version
```

## 2. Prepare A Runtime Schema And Security Files

The packaged `configuration/` directory contains configuration documentation and
reference material. Runtime table schemas should live in a separate directory
that you own for the deployment. This runbook uses `./runtime/config`.

```bash
mkdir -p data backups auth runtime/config/release_smoke

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
```

The account file stores verifier hashes, not the cleartext password. Use a real
password and service-secret mechanism outside local smoke tests.

## 3. Start The Single-Node Engine

```bash
mkdir -p data backups

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
  -access-policy-file ./auth/access-policy.yaml
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

## 4. Inspect Local Durability State

In a second terminal:

```bash
./bin/qstream-admin wal validate \
  --path ./data/storage.wal

./bin/qstream-admin wal plan \
  --path ./data/storage.wal
```

On a fresh engine these commands report an empty or checkpoint-clean WAL. If a
command reports a replay or pending tail, follow the printed hint before taking a
backup.

Run a local preflight over the same paths and endpoints:

```bash
./bin/qstream-admin doctor local \
  --data-dir ./data \
  --config-dir ./runtime/config \
  --wal-path ./data/storage.wal \
  --auth-account-file ./auth/accounts.yaml \
  --access-policy-file ./auth/access-policy.yaml \
  --mysql-addr 127.0.0.1:4000 \
  --native-grpc-addr 127.0.0.1:4100
```

## 5. Connect With A MySQL Client

In a second terminal:

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
./bin/qstream-loader \
  -listen 127.0.0.1:8088 \
  -config-dir ./runtime/config \
  -database quanta \
  -tables release_smoke \
  -connection-mode standard-native \
  -native-grpc-addr 127.0.0.1:4100
```

## 7. Send A JSON Smoke Batch

```bash
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
the snapshot:

```bash
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

## 10. Stop The Engine

Press `Ctrl-C` in the engine terminal. The local data lives under `./data`.

For support, capture a diagnostic bundle:

```bash
./bin/qstream-admin support bundle \
  --output ./qstream-support.tar.gz \
  --data-dir ./data \
  --wal-path ./data/storage.wal \
  --auth-account-file ./auth/accounts.yaml \
  --access-policy-file ./auth/access-policy.yaml \
  --backup-source file://$PWD/backups/smoke-backup
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
