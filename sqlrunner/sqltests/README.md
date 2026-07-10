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
