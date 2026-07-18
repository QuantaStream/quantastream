# SQL Rewrite Debt

This document tracks temporary SQL-to-SQL rewrites that exist only because
some subquery intent is not planner-native yet. They keep useful TPCH and
SQLRunner shapes moving while the parser, IR, planner, and executor layers
catch up. These rewrites are compatibility scaffolding, not product
architecture.

## Current Preflight Rewrites

| Rule | Current SQL shape | Temporary strategy | Helper-plan descriptors | Current regression coverage | Future replacement |
|---|---|---|---|---|---|
| `correlated_aggregate_preflight` | Correlated aggregate predicates such as `l_quantity < (select factor * avg(l2.l_quantity) ... where l2.l_partkey = p.p_partkey)`. | Route parent-key lookup and aggregate-threshold materialization through native subquery step contracts backed by the SQL adapter, then expand the predicate into equivalent per-key threshold branches before native planning. | `parent_key_lookup`, `aggregate_threshold_lookup` | `qsruntime/sql_runtime_test.go`; inabox-direct TPCH Q17-style probes when present. | Typed correlated aggregate subquery IR lowered by planner-owned aggregate-threshold and semi-join kernels. |

## Native Promotion Order

`scalar_subquery` is the first promoted shape. Scalar subqueries in `WHERE` and
`HAVING`, plus projection-only scalar subqueries in the `SELECT` list, are now
parsed as typed expression nodes and materialized into literals inside the
runtime before bitmap lowering. The parent SQL text is no longer rewritten for
those shapes, and the old scalar SQL-text rewrite scanner has been deleted.

`parent_key_lookup` and `aggregate_threshold_lookup` still have native-ready
contracts under `correlated_aggregate_preflight`. Both still delegate to the
SQL-backed adapter internally, but inspection can now tell the execution story
as scalar materialization, parent-key lookup, and aggregate-threshold lookup.

## Guardrails

- Every entry returned by `SQLRuntime.preflightRewriteRules()` must have an
  inventory entry in `qsruntime.preflightRewriteInventory()`.
- Each rewrite must expose a stable rule id, reason, source SQL shape,
  temporary strategy, helper-plan kind list, regression coverage note, and
  future IR replacement path.
- Rewrites should emit optimizer trace metadata, per-rule duration, preflight
  inspection summaries, descriptor reports, subquery intent reports, and
  helper-plan descriptors so non-planner-native subquery intent remains visible
  in diagnostics.
- Parent-key and aggregate-threshold preflight now route through the native
  subquery step executor contract before falling back to SQL-backed adapter
  internals.
- Helper-plan descriptors now route through injectable execution boundaries
  with typed scalar, parent-key, and aggregate threshold payloads. The default
  implementation still delegates to SQL-backed helper work today, but helper
  SQL is now fallback/debug context rather than the only contract.
- Delete temporary rewrite code as soon as the equivalent parser, IR, planner,
  or executor capability makes it eligible for deletion.
