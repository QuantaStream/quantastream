# Static MySQL Auth And SQL Access Policies

QuantaStream can run with deployment-managed MySQL authentication and SQL
authorization files. These files are local YAML inputs read by the standard
server or distributed proxy at startup. They are intentionally simple, auditable
configuration artifacts; distributed mode does not store these auth files in
Consul.

## Account File

Static MySQL authentication uses an account file with one or more users:

```yaml
accounts:
  - username: analyst
    default_database: quanta
    roles:
      - reader
    mysql_native_password_verifier: <hex verifier>
    caching_sha2_password_verifier: <hex verifier>
```

Use the admin tool to avoid storing cleartext passwords. Release artifacts use
`./bin/qstream-admin`; source checkout examples use `go run ./qstream-admin`:

```bash
cd /home/ubuntu/quantastream

QUANTASTREAM_AUTH_PASSWORD='replace-me' \
go run ./qstream-admin auth upsert \
  --account-file /etc/quantastream/accounts.yaml \
  --user analyst \
  --default-database quanta \
  --roles reader

go run ./qstream-admin auth list \
  --account-file /etc/quantastream/accounts.yaml

go run ./qstream-admin auth validate \
  --account-file /etc/quantastream/accounts.yaml
```

The admin command writes password verifier hashes and keeps the file mode at
`0600`.

## Access Policy File

Static SQL authorization uses grants assigned to either users or roles:

```yaml
grants:
  - principal_kind: role
    principal: reader
    privilege: select
    schema: quanta
    table: orders

  - principal_kind: role
    principal: reader
    privilege: select
    schema: quanta
    table: lineitem
    fields:
      - l_orderkey
      - l_extendedprice
      - l_discount

  - principal_kind: role
    principal: loader
    privilege: insert
    schema: quanta
    table: lineitem
```

Use the admin tool to maintain the policy file:

```bash
cd /home/ubuntu/quantastream

go run ./qstream-admin access upsert \
  --policy-file /etc/quantastream/access-policy.yaml \
  --principal-kind role \
  --principal reader \
  --privilege select \
  --schema quanta \
  --table orders

go run ./qstream-admin access upsert \
  --policy-file /etc/quantastream/access-policy.yaml \
  --principal-kind role \
  --principal reader \
  --privilege select \
  --schema quanta \
  --table lineitem \
  --fields l_orderkey,l_extendedprice,l_discount

go run ./qstream-admin access list \
  --policy-file /etc/quantastream/access-policy.yaml

go run ./qstream-admin access validate \
  --policy-file /etc/quantastream/access-policy.yaml
```

Supported privileges are `select`, `insert`, `update`, `delete`, `truncate`,
`create`, and `drop`.

## Starting Standard Mode

Pass both files when starting the single-node standard server:

```bash
QUANTASTREAM_AUTH_MODE=static \
QUANTASTREAM_AUTH_ACCOUNT_FILE=/etc/quantastream/accounts.yaml \
QUANTASTREAM_ACCESS_POLICY_FILE=/etc/quantastream/access-policy.yaml \
./startup-scripts/start-standard.sh
```

Or use the direct binary flags:

```bash
go run ./cmd/quantastream \
  -mode inabox-standard \
  -config-dir configuration \
  -data-dir data \
  -auth-mode static \
  -auth-account-file /etc/quantastream/accounts.yaml \
  -access-policy-file /etc/quantastream/access-policy.yaml
```

Use `-status` to validate auth configuration without opening the listener:

```bash
go run ./cmd/quantastream \
  -status \
  -auth-mode static \
  -auth-account-file /etc/quantastream/accounts.yaml \
  -access-policy-file /etc/quantastream/access-policy.yaml
```

The status output should include:

```text
auth=static
auth_account_file=/etc/quantastream/accounts.yaml
authorization=static_policy
access_policy_file=/etc/quantastream/access-policy.yaml
```

## Starting Distributed Proxy Mode

The distributed proxy reads the same files from the proxy host:

```bash
QUANTASTREAM_AUTH_MODE=static \
QUANTASTREAM_AUTH_ACCOUNT_FILE=/etc/quantastream/accounts.yaml \
QUANTASTREAM_ACCESS_POLICY_FILE=/etc/quantastream/access-policy.yaml \
go run ./cmd/quantastream-proxy \
  -bind 0.0.0.0 \
  -mysql-port 4000 \
  -consul 127.0.0.1:8500 \
  -schema-dir tpc-h-benchmark/config \
  -database quanta
```

In distributed mode, Consul remains responsible for cluster discovery and
catalog metadata. Static auth and access policy files remain local deployment
inputs to the proxy process.

## Operational Notes

- Empty `QUANTASTREAM_ACCESS_POLICY_FILE` leaves SQL authorization permissive.
- Once an access policy file is configured, every query or mutation must satisfy
  the required table/field grants.
- Run `qstream-admin auth validate` and `qstream-admin access validate` before
  restarting a service with changed static security files.
- Prefer role grants for normal users. Put roles on accounts, then grant SQL
  privileges to roles.
- Include `schema: quanta` for TPC-H and standard QuantaStream deployments.
- Restart the server or proxy after changing auth or access policy files.
