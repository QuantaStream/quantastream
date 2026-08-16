# SQLRunner Test Suites

The broad suites in this directory are executable behavior contracts for the
native refactor path. Keep reusable SQL semantics in the general suites, and use
TPC-H suites as analytical shape coverage rather than as formal benchmark runs.

`inabox_direct_tpch_kernels.yaml` is the broad inabox-direct TPC-H signal. It
should stay useful for cross-query regression checks and avoid absorbing every
long-running probe discovered during TPC-H work.

Focused per-query suites, such as `inabox_direct_tpch_q9.yaml`, may contain
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
- `mysql_compat_group_order.yaml`
- `mysql_compat_joins.yaml`
- `mysql_compat_subqueries.yaml`
- `mysql_compat_mutations.yaml`
- `mysql_compat_views.yaml`

Boundary suites such as `mysql_compat_views_boundaries.yaml` and
`mysql_compat_group_order_boundaries.yaml` intentionally track MySQL-compatible
behavior that QuantaStream does not support yet. Run them against a QuantaStream
target to see `XFAIL` roadmap gaps. Do not include them in MySQL reference
captures or `MYSQL_COMPAT_SUITE=all`, because stock MySQL success would
correctly appear as `XPASS`.

## Capturing MySQL Reference Results

Compatibility suites can generate runnable SQLRunner suites with `expect`
blocks captured from any SQLRunner engine. The normal workflow is to run
`-capture_expected` against stock MySQL, then run the generated suite against
QuantaStream. A live MySQL server is only required when the selected engine is
`mysql-reference`.

```bash
go run . -engine mysql-reference -suite_file sqltests/mysql_compat_select.yaml -mysql_dsn 'user:pass@tcp(127.0.0.1:3306)/test' -capture_expected expected/local/mysql_compat_select.yaml
```

Generated compatibility suites should be written under `sqlrunner/expected/local/`
for local work. Promote generated suites into `sqlrunner/expected/` only after
they become reviewed compatibility contracts.

The same capture-and-run flow can be executed in one command when both engines
are available:

```bash
go run . -engine_diff mysql-reference,inabox-direct -suite_file sqltests/mysql_compat_select.yaml -mysql_dsn 'user:pass@tcp(127.0.0.1:3306)/test'
```


## Live MySQL Helper

SQLRunner does not start or own a MySQL server. When a stock MySQL reference is
available, `run-mysql-compat.sh` standardizes capture and diff commands from the
`sqlrunner` directory:

```bash
MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/test' ./run-mysql-compat.sh
MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/test' MYSQL_COMPAT_MODE=diff TARGET_ENGINE=inabox-direct ./run-mysql-compat.sh
MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/test' MYSQL_COMPAT_SUITE=sqltests/mysql_compat_views.yaml MYSQL_COMPAT_MODE=diff TARGET_ENGINE=inabox-standard ./run-mysql-compat.sh
MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/test' MYSQL_COMPAT_SUITE=all MYSQL_COMPAT_MODE=diff TARGET_ENGINE=inabox-standard ./run-mysql-compat.sh
```

Run the view boundary suite directly against QuantaStream targets:

```bash
go run . -engine inabox-direct -consul 127.0.0.1:8500 -suite_file sqltests/mysql_compat_views_boundaries.yaml -compat_report
go run . -engine inabox-direct -consul 127.0.0.1:8500 -suite_file sqltests/mysql_compat_group_order_boundaries.yaml -compat_report
```
