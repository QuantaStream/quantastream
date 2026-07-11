# SQLRunner Expected Suites

This directory is reserved for SQLRunner suites generated from reference engine
runs. The usual MySQL compatibility workflow is:

1. Capture expected behavior from stock MySQL into `expected/local/` or another
   caller-chosen ignored path.
2. Run that generated suite against QuantaStream.
3. Promote a generated suite into this directory only when it becomes a stable,
   reviewed compatibility contract.

Ignored local output paths:

- `sqlrunner/expected/local/`
- `sqlrunner/expected/generated/`
- `sqlrunner/expected/*.local.yaml`

Example from the `sqlrunner` directory:

```bash
go run . -engine mysql-reference -suite_file sqltests/mysql_compat_select.yaml -mysql_dsn 'user:pass@tcp(127.0.0.1:3306)/test' -capture_expected expected/local/mysql_compat_select.yaml
```
