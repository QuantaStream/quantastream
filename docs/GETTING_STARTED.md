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

## 2. Start The Single-Node Engine

```bash
mkdir -p data backups

./bin/quantastream \
  -config-dir ./configuration \
  -data-dir ./data \
  -wal-path ./data/storage.wal \
  -bind 127.0.0.1 \
  -mysql-port 4000 \
  -native-grpc-bind 127.0.0.1 \
  -native-grpc-port 4100 \
  -database quanta \
  -auth-mode static \
  -auth-user root \
  -auth-password root
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

## 3. Inspect Local Durability State

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

## 4. Connect With A MySQL Client

In a second terminal:

```bash
mysql -h 127.0.0.1 -P 4000 -u root -proot -D quanta \
  -e "select @@version, @@version_comment; show tables;"
```

The version comment includes the QuantaStream build identity when the binary was
created by the release script.

## 5. Start The JSON Loader

The loader accepts JSON event batches and writes them through the native gRPC
endpoint exposed by the single-node engine.

```bash
./bin/qstream-loader \
  -listen 127.0.0.1:8088 \
  -config-dir ./configuration \
  -database quanta \
  -connection-mode standard-native \
  -native-grpc-addr 127.0.0.1:4100
```

## 6. Send A Streaming Smoke Batch

```bash
./bin/qstream-stream-loader \
  -target http://127.0.0.1:8088/ingest/json \
  -orders 10 \
  -lineitems 4 \
  -batch-size 25
```

For TPC-H experiments, use the `qstream-tpch-loader` binary with the TPC-H
configuration and data directories. The TPC-H runbooks in `docs/TPCH.md` and
`docs/DEPLOYMENT.md` describe the larger benchmark flow.

## 7. Create And Prove A Local Backup

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

## 8. Stop The Engine

Press `Ctrl-C` in the engine terminal. The local data lives under `./data`.

For support, capture these lines:

```bash
./bin/quantastream -version
./bin/qstream-admin version
./bin/qstream-admin backup inspect --source file://$PWD/backups/smoke-backup
./bin/qstream-admin wal plan --path ./data/storage.wal
mysql -h 127.0.0.1 -P 4000 -u root -proot -D quanta \
  -e "select @@version, @@version_comment;"
```
