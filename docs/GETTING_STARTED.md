# Getting Started With QuantaStream Binaries

This guide starts a local single-node QuantaStream engine, writes one JSON row,
and reads it back through the MySQL-compatible endpoint.

The packaged `configuration/` directory contains documentation and schema
reference material. Runtime schemas for this guide live under `./runtime/config`.

## 1. Unpack

Run this from the directory containing the downloaded release archive and
`SHA256SUMS` file.

```bash
archive="$(ls -1 qstream-*-linux-amd64.tar.gz | tail -n 1)"
sha256sum -c SHA256SUMS --ignore-missing
tar -xzf "$archive"
cd "$(tar -tzf "$archive" | sed -n '1s#/.*##p')"

./bin/quantastream -version
./bin/qstream-admin version
./bin/qstream-loader -version
```

## 2. Create A Tiny Runtime Schema

This creates a fresh local smoke schema, a local root account, and a simple
select policy.

```bash
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
```

The password for this local smoke account is `root`.

## 3. Start The Engine

This starts the engine in the background. If the local engine ports are already
open, the block assumes the engine is running and leaves it alone.

```bash
mkdir -p data runtime logs

if (echo > /dev/tcp/127.0.0.1/4000) >/dev/null 2>&1 && \
   (echo > /dev/tcp/127.0.0.1/4100) >/dev/null 2>&1; then
  echo "QuantaStream engine already appears reachable on 127.0.0.1:4000 and 127.0.0.1:4100"
elif [ -f ./runtime/quantastream.pid ] && kill -0 "$(cat ./runtime/quantastream.pid)" 2>/dev/null; then
  echo "QuantaStream engine already running: pid=$(cat ./runtime/quantastream.pid)"
else
  nohup ./bin/quantastream \
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
  echo "QuantaStream engine started: pid=$(cat ./runtime/quantastream.pid)"
fi

sleep 2
tail -30 ./logs/quantastream.log 2>/dev/null || true
```

Expected lines include:

```text
listening=127.0.0.1:4000
native_grpc_listening=127.0.0.1:4100
```

Check the local engine:

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

`doctor_result=PASS` means the local engine is ready. A warning about missing
`CATALOG_OBJECTS` is fine for this tiny smoke setup.

## 4. Start The JSON Loader

The loader accepts JSON event batches and writes them through the engine's native
gRPC endpoint. If the loader is already running, this block leaves it alone.

```bash
mkdir -p runtime logs

if curl -fsS http://127.0.0.1:8088/healthz >/dev/null 2>&1; then
  echo "QStream JSON loader already running"
else
  nohup ./bin/qstream-loader \
    -listen 127.0.0.1:8088 \
    -config-dir ./runtime/config \
    -database quanta \
    -tables release_smoke \
    -connection-mode standard-native \
    -native-grpc-addr 127.0.0.1:4100 \
    -flush-interval 100ms \
    > ./logs/qstream-loader.log 2>&1 &
  echo "$!" > ./runtime/qstream-loader.pid
  echo "QStream JSON loader started: pid=$(cat ./runtime/qstream-loader.pid)"
  sleep 2
fi

curl -fsS http://127.0.0.1:8088/healthz || tail -40 ./logs/qstream-loader.log
```

## 5. Write And Read One Row

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

sleep 1

mysql -h 127.0.0.1 -P 4000 -u root -proot -D quanta \
  -e "select id, name, score, latitude from release_smoke;"
```

You can also connect with MySQL Workbench or another MySQL-compatible client:

```text
Host: 127.0.0.1
Port: 4000
User: root
Password: root
Database: quanta
```

## 6. Optional Backup Smoke

This stops the JSON loader so the example data set is quiet, then creates and
validates a local backup.

```bash
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

./bin/qstream-admin backup validate \
  --source file://$PWD/backups/smoke-backup
```

## 7. Stop Local Processes

```bash
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

## Troubleshooting

If a startup command does not behave as expected, check the logs:

```bash
tail -80 ./logs/quantastream.log
tail -80 ./logs/qstream-loader.log
```

If you want a clean retry:

```bash
rm -rf ./data ./backups ./auth ./runtime ./logs
```

Then start again from step 2.
