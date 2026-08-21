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
mkdir -p data

./bin/quantastream \
  -config-dir ./configuration \
  -data-dir ./data \
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

## 3. Connect With A MySQL Client

In a second terminal:

```bash
mysql -h 127.0.0.1 -P 4000 -u root -proot -D quanta \
  -e "select @@version, @@version_comment; show tables;"
```

The version comment includes the QuantaStream build identity when the binary was
created by the release script.

## 4. Start The JSON Loader

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

## 5. Send A Streaming Smoke Batch

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

## 6. Stop The Engine

Press `Ctrl-C` in the engine terminal. The local data lives under `./data`.

For support, capture these lines:

```bash
./bin/quantastream -version
./bin/qstream-admin version
mysql -h 127.0.0.1 -P 4000 -u root -proot -D quanta \
  -e "select @@version, @@version_comment;"
```
