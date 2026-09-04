# Getting Started With QuantaStream Binaries

This guide starts a local single-node QuantaStream engine, restores the bundled
TPC-H SF0.01 sample, and runs a few queries through the MySQL-compatible
endpoint.

You can begin here from a website, GitHub, a release announcement, or an
already-extracted release bundle. No earlier README steps are required.

## Before You Start

You need:

- Linux AMD64 or WSL2 on an AMD64 Windows host;
- a shell with `sha256sum` and `tar`;
- a MySQL-compatible command-line client for the final query step; and
- about 1 GB of free space for the archive, extracted bundle, and sample data.

On WSL2, download and unpack inside the Linux filesystem, such as under your
Linux home directory. Extraction on a Windows-mounted path such as `/mnt/c`
can be substantially slower.

Check the required commands:

```bash
command -v sha256sum
command -v tar
command -v mysql
```

MySQL Workbench can be used instead of the `mysql` command for querying, but
the command-line client gives the shortest reproducible path.

## 1. Download

Create a working directory and download the current release archive and its
checksum file:

```bash
mkdir -p ~/quantastream-evaluation
cd ~/quantastream-evaluation

QS_VERSION=0.1.3

gh release download "v${QS_VERSION}" \
  --repo QuantaStream/quantastream \
  --pattern "qstream-${QS_VERSION}-linux-amd64.tar.gz" \
  --pattern SHA256SUMS
```

If you do not use the GitHub CLI, download both files from
[GitHub Releases](https://github.com/QuantaStream/quantastream/releases):

- `qstream-0.1.3-linux-amd64.tar.gz`
- `SHA256SUMS`

Place both files in the same directory before continuing.

## 2. Verify and Unpack

Run this from the directory containing the downloaded release archive and
`SHA256SUMS` file.

```bash
QS_VERSION=0.1.3
archive="qstream-${QS_VERSION}-linux-amd64.tar.gz"

sha256sum -c SHA256SUMS --ignore-missing
tar -xzf "$archive"
cd "qstream-${QS_VERSION}-linux-amd64"
QSTREAM_HOME="$PWD"

echo "Using QStream bundle at: $QSTREAM_HOME"

./bin/quantastream -version
./bin/qstream-admin version
./bin/qstream-loader -version
```

If you are reading this file from inside an already-extracted release bundle,
do not download or unpack it again. Start here instead:

```bash
cd /path/to/qstream-0.1.3-linux-amd64
QSTREAM_HOME="$PWD"

./bin/quantastream -version
./bin/qstream-admin version
./bin/qstream-loader -version
```

Then continue with step 3.

The packaged `configuration/` directory contains schema reference material.
The runnable sample schema and backup live under `samples/tpch-sf-0.01/`.

## 3. Restore The Sample Data

The release bundle includes a small TPC-H SF0.01 backup. Restore it into the
local data directory.

The restore can take a few minutes and currently does not print progress while
it copies and validates the backup contents.

```bash
./bin/qstream-admin backup restore \
  --source "file://${QSTREAM_HOME}/samples/tpch-sf-0.01/backup" \
  --data-dir ./data
```

`QSTREAM_HOME` is set in step 2 to the unpacked release directory. If you open
a new terminal, return to that directory and set it again:

```bash
cd /path/to/qstream-<version>-linux-amd64
QSTREAM_HOME="$PWD"
```

## 4. Start QuantaStream

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
  -auth-mode permissive \
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

This walkthrough opts into permissive authentication for an isolated loopback
evaluation. Permissive mode accepts every syntactically valid MySQL handshake
and cannot be used with a non-loopback MySQL bind. Use static authentication
with configured credentials before exposing QuantaStream to another host.

## 5. Run Doctor

```bash
./bin/qstream-admin doctor local \
  --data-dir ./data \
  --config-dir ./samples/tpch-sf-0.01/config \
  --wal-path ./data/storage.wal \
  --mysql-addr 127.0.0.1:4000 \
  --native-grpc-addr 127.0.0.1:4100
```

`doctor_result=PASS` means the local engine is ready.

## 6. Query The Sample

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

## 7. Stop

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
