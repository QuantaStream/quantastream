# JSON Loader Tutorial

This tutorial shows the smallest useful `qstream-loader` path: define one
event-shaped table, start the single-node engine with native gRPC enabled, post
a JSON event batch, and read the row through the MySQL-compatible endpoint.

Start with [GETTING_STARTED.md](GETTING_STARTED.md) first if you have not yet
verified the binary bundle.

## 1. Create A Loader Schema

Run these commands from the unpacked release directory.

```bash
mkdir -p data auth runtime/config/release_smoke logs

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
```

## 2. Start The Engine

If you already have the getting-started engine running, stop it first so this
tutorial can use its own runtime schema.

```bash
if [ -f ./runtime/quantastream.pid ]; then
  kill "$(cat ./runtime/quantastream.pid)" 2>/dev/null || true
  rm -f ./runtime/quantastream.pid
fi

nohup ./bin/quantastream \
  -config-dir ./runtime/config \
  -data-dir ./data \
  -wal-path ./data/storage.wal \
  -bind 127.0.0.1 \
  -mysql-port 4000 \
  -native-grpc-bind 127.0.0.1 \
  -native-grpc-port 4100 \
  -database quanta \
  > ./logs/quantastream.log 2>&1 &

echo "$!" > ./runtime/quantastream.pid
sleep 2
tail -30 ./logs/quantastream.log
```

## 3. Start The Loader

```bash
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
sleep 2

curl -fsS http://127.0.0.1:8088/healthz
```

## 4. Post An Event

```bash
curl -fsS http://127.0.0.1:8088/ingest/json \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {
        "mode": "stream",
        "event_id": "release-smoke-row-1",
        "source": "json-loader-tutorial",
        "event_time": "2026-08-21T00:00:00Z",
        "source_offset": "json-loader-tutorial:1",
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
```

## 5. Read It Back

```bash
mysql -h 127.0.0.1 -P 4000 -u qstream -D quanta \
  -e "select id, name, score, latitude from release_smoke;"
```

The loader status endpoint is useful while you experiment:

```bash
curl -fsS http://127.0.0.1:8088/stats | python3 -m json.tool | head -80
```

## 6. Stop

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
