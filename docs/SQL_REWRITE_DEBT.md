# SQL Rewrite Debt

This document tracks temporary SQL-to-SQL rewrites that exist only because
some subquery intent is not planner-native yet. They keep useful TPCH and
SQLRunner shapes moving while the parser, IR, planner, and executor layers
catch up. These rewrites are compatibility scaffolding, not product
architecture.

## Current Preflight Rewrites

| Rule | Current SQL shape | Temporary strategy | Helper-plan descriptors | Current regression coverage | Future replacement |
|---|---|---|---|---|---|
| `correlated_aggregate_preflight` | Correlated aggregate predicates such as `l_quantity < (select factor * avg(l2.l_quantity) ... where l2.l_partkey = p.p_partkey)`. | Route parent-key lookup and aggregate-threshold materialization through native subquery step contracts, then expand the predicate into equivalent per-key threshold branches before native planning. Helper SQL remains as debug/fallback context, not the execution contract for runtime-ready native steps. | `parent_key_lookup`, `aggregate_threshold_lookup` | `qsruntime/sql_runtime_test.go`; inabox-direct TPCH Q17-style probes when present. | Typed correlated aggregate subquery IR lowered by planner-owned aggregate-threshold and semi-join kernels. |

## Native Promotion Order

`scalar_subquery` is the first promoted shape. Scalar subqueries in `WHERE` and
`HAVING`, plus projection-only scalar subqueries in the `SELECT` list, are now
parsed as typed expression nodes and materialized into literals inside the
runtime before bitmap lowering. The parent SQL text is no longer rewritten for
those shapes, and the old scalar SQL-text rewrite scanner has been deleted.

`parent_key_lookup` and `aggregate_threshold_lookup` have native-ready contracts
under `correlated_aggregate_preflight`. When the runtime is ready, parent-key
lookup now builds a bound query from its typed payload instead of reparsing the
helper SQL text, and aggregate-threshold lookup builds its runtime request from
typed parent keys and rownums. Inspection can tell the execution story as scalar
materialization, parent-key lookup, and aggregate-threshold lookup while the
outer correlated SQL rewrite is still being retired.

The Q17-style correlated aggregate descriptor and qsbridge subquery intent now
carry typed table/alias/field references for the outer value, inner aggregate
value, correlated keys, and required parent filters. The simple parser can now
recognize the Q17 correlated aggregate predicate and bind it into
`QueryIR.Subqueries` as typed intent. The runtime recognizer is still a
temporary SQL-pattern match for execution rewrite, but descriptor reports and
helper intent are derived from typed fields rather than ad hoc qualified-name
strings.

The remaining SQL-pattern matching for this shape is isolated behind the private
`correlatedAverageQuantitySQLRecognizer` boundary. When correlated aggregate
subqueries become parser/IR-native, that recognizer and its SQL rewrite caller
are the intended deletion point.

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
  subquery step executor contract before falling back to SQL-backed helper work
  when the runtime is not ready for native execution.
- Helper-plan descriptors now route through injectable execution boundaries
  with typed scalar, parent-key, and aggregate threshold payloads. Helper SQL is
  fallback/debug context rather than the only contract.
- Delete temporary rewrite code as soon as the equivalent parser, IR, planner,
  or executor capability makes it eligible for deletion.
