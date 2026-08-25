# Getting Started With QuantaStream Binaries

This guide starts a local single-node QuantaStream engine, restores the bundled
TPC-H SF0.01 sample, and runs a few queries through the MySQL-compatible
endpoint.

The packaged `configuration/` directory contains schema documentation. The
runnable sample schema and backup live under `./samples/tpch-sf-0.01/`.

## 1. Unpack

Run this from the directory containing the downloaded release archive and
`SHA256SUMS` file.

```bash
archive="$(ls -1 qstream-*-linux-amd64.tar.gz | tail -n 1)"
sha256sum -c SHA256SUMS --ignore-missing
tar -xzf "$archive"
cd "$(tar -tzf "$archive" | sed -n '1s#/.*##p')"
QSTREAM_HOME="$PWD"

echo "Using QStream bundle at: $QSTREAM_HOME"

./bin/quantastream -version
./bin/qstream-admin version
./bin/qstream-loader -version
```

## 2. Restore The Sample Data

The release bundle includes a small TPC-H SF0.01 backup. Restore it into the
local data directory.

The restore can take a few minutes and currently does not print progress while
it copies and validates the backup contents.

```bash
rm -rf ./data

./bin/qstream-admin backup restore \
  --source "file://${QSTREAM_HOME}/samples/tpch-sf-0.01/backup" \
  --data-dir ./data
```

`QSTREAM_HOME` is set in step 1 to the unpacked release directory. If you open
a new terminal, return to that directory and set it again:

```bash
cd /path/to/qstream-<version>-linux-amd64
QSTREAM_HOME="$PWD"
```

## 3. Start QuantaStream

```bash
mkdir -p runtime logs

nohup ./bin/quantastream \
  -config-dir ./samples/tpch-sf-0.01/config \
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

Expected lines include:

```text
listening=127.0.0.1:4000
native_grpc_listening=127.0.0.1:4100
```

## 4. Run Doctor

```bash
./bin/qstream-admin doctor local \
  --data-dir ./data \
  --config-dir ./samples/tpch-sf-0.01/config \
  --wal-path ./data/storage.wal \
  --mysql-addr 127.0.0.1:4000 \
  --native-grpc-addr 127.0.0.1:4100
```

`doctor_result=PASS` means the local engine is ready.

## 5. Query The Sample

Use the MySQL command-line client:

```bash
mysql -h 127.0.0.1 -P 4000 -u qstream -D quanta
```

Or connect with MySQL Workbench:

```text
Host: 127.0.0.1
Port: 4000
User: qstream
Password: leave blank
Database: quanta
```

Try a few queries:

```sql
select count(*) as lineitem_rows
from lineitem;
```

```sql
select
  l_returnflag,
  l_linestatus,
  count(*) as rows,
  sum(l_quantity) as total_quantity,
  sum(l_extendedprice * (1 - l_discount)) as discounted_revenue
from lineitem
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus;
```

```sql
select
  o_orderpriority,
  count(*) as orders
from orders
group by o_orderpriority
order by orders desc;
```

```sql
select
  n.n_name,
  count(*) as customers
from customer c
join nation n on c.c_nationkey = n.n_nationkey
join region r on n.n_regionkey = r.r_regionkey
where r.r_name = 'ASIA'
group by n.n_name
order by customers desc;
```

## 6. Stop

```bash
if [ -f ./runtime/quantastream.pid ]; then
  kill "$(cat ./runtime/quantastream.pid)" 2>/dev/null || true
  rm -f ./runtime/quantastream.pid
fi
```

The local data lives under `./data`.

## Next

- Use [DEPLOYMENT.md](DEPLOYMENT.md) for operational topics such as backups,
  WAL inspection, auth files, and systemd services.
- Use [JSON_LOADER_TUTORIAL.md](JSON_LOADER_TUTORIAL.md) when you are ready to
  ingest JSON events through `qstream-loader`.
- Use [SCHEMA_DESIGN.md](SCHEMA_DESIGN.md) and the `configuration/` directory
  to design your own tables.

## Troubleshooting

If startup does not behave as expected, check the log:

```bash
tail -80 ./logs/quantastream.log
```

If port `4000` or `4100` is already in use, stop the existing local
QuantaStream process before starting this guide again.
