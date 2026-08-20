# MySQL Driver Smoke

`mysql-driver-smoke` is a small `database/sql` client for checking that common MySQL driver behavior works against a QuantaStream MySQL-compatible endpoint. It is also a compact usage reference for application code.

The smoke covers:

- opening and pinging a MySQL DSN
- metadata queries used by SQL tools
- prepared `SELECT` queries with string and numeric parameters
- optional prepared single-row `INSERT` with prepared cleanup
- optional prepared multi-row `INSERT` against the QA catalog, when installed
- the TPC-H Q3 view query, when `q3_order_line_base` is installed

## TPC-H Endpoint

Use this against a standard TPC-H single-node server or proxy endpoint:

```bash
go run ./cmd/mysql-driver-smoke \
  -dsn 'MOLIG004@tcp(127.0.0.1:4000)/quanta?parseTime=true' \
  -timeout 90s \
  -max-rows 3
```

Add `-prepared-write` to exercise a prepared `INSERT` into the TPC-H `customer` table. This mutates data, then deletes the smoke row and verifies cleanup:

```bash
go run ./cmd/mysql-driver-smoke \
  -dsn 'MOLIG004@tcp(127.0.0.1:4000)/quanta?parseTime=true' \
  -timeout 90s \
  -max-rows 1 \
  -prepared-write
```

## QA Catalog Endpoint

Use `-prepared-batch` when the endpoint has the old QA catalog tables installed. It runs a three-row prepared `INSERT` into `customers_qa`, then deletes those rows and verifies cleanup:

```bash
go run ./cmd/mysql-driver-smoke \
  -dsn 'MOLIG004@tcp(127.0.0.1:4000)/quanta?parseTime=true' \
  -timeout 90s \
  -max-rows 1 \
  -prepared-batch
```

On TPC-H-only endpoints, `-prepared-batch` skips cleanly with `customers_qa not installed; skipped`.

## Notes

`-prepared-write` and `-prepared-batch` are intentionally opt-in because they write data. They are designed to use high-key or smoke-key rows and self-clean before returning.
