# SQLRunner Test Suites

The broad suites in this directory are executable behavior contracts for the
native refactor path. Keep reusable SQL semantics in the general suites, and use
TPC-H suites as analytical shape coverage rather than as formal benchmark runs.

`legacy_direct_tpch_kernels.yaml` is the broad legacy-direct TPC-H signal. It
should stay useful for cross-query regression checks and avoid absorbing every
long-running probe discovered during TPC-H work.

Focused per-query suites, such as `legacy_direct_tpch_q9.yaml`, may contain
heavier staged kernels, explicit expected-error boundaries, and local wall-time
notes. Use them when a query family needs deeper coverage without making the
main kernel suite harder to run casually.

## MySQL compatibility suites

`mysql_compat_*.yaml` suites are compatibility-lab inputs. They are organized
by SQL surface area and carry `feature`, `compatibility`, and `requires`
metadata for scorecards. These suites are intended to run against a stock MySQL
reference and QuantaStream comparison harness; they are not TPC-H benchmarks and
should not absorb engine-specific profiling probes.

The current seed suites are:

- `mysql_compat_select.yaml`
- `mysql_compat_predicates.yaml`
- `mysql_compat_functions.yaml`

## Capturing MySQL Reference Results

Compatibility suites can generate runnable SQLRunner suites with `expect`
blocks captured from any SQLRunner engine. The normal workflow is to run
`-capture_expected` against stock MySQL, then run the generated suite against
QuantaStream. A live MySQL server is only required when the selected engine is
`mysql-reference`.

```bash
go run . -engine mysql-reference -suite_file sqltests/mysql_compat_select.yaml -mysql_dsn 'user:pass@tcp(127.0.0.1:3306)/test' -capture_expected expected/mysql_compat_select.yaml
```
