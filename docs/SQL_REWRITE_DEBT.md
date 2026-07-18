# SQL Rewrite Debt

This document tracks temporary preflight transforms that exist only because
some subquery intent is not planner-native yet. They keep useful TPCH and
SQLRunner shapes moving while the parser, IR, planner, and executor layers
catch up. These transforms are compatibility scaffolding, not product
architecture.

## Current Preflight Rewrites

| Rule | Current SQL shape | Temporary strategy | Helper-plan descriptors | Current regression coverage | Future replacement |
|---|---|---|---|---|---|
| `correlated_aggregate_preflight` | Correlated aggregate predicates such as `l_quantity < (select factor * avg(l2.l_quantity) ... where l2.l_partkey = p.p_partkey)`. | Use typed correlated aggregate intent from the bound query, route parent-key lookup and aggregate-threshold materialization through native subquery step contracts, then attach native correlated aggregate predicate metadata to the runtime request. Helper SQL remains as debug/fallback context, not the execution contract for runtime-ready native steps. | `parent_key_lookup`, `aggregate_threshold_lookup` | `qsruntime/sql_runtime_test.go`; inabox-direct TPCH Q17-style probes when present. | Typed correlated aggregate subquery IR lowered by planner-owned aggregate-threshold and semi-join kernels. |

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
outer correlated preflight transform is still being retired.

The Q17-style correlated aggregate descriptor and qsbridge subquery intent now
carry typed table/alias/field references for the outer value, inner aggregate
value, correlated keys, source predicate text, and required parent filters. The
simple parser recognizes the Q17 correlated aggregate predicate and binds it
into `QueryIR.Subqueries` as typed intent. Runtime preflight now requires that
bound intent when constructing the temporary execution transform; the old
SQL-pattern recognizer fallback has been deleted. The parent SQL text is no
longer rewritten for this shape; preflight preserves the original SQL and
attaches a native correlated aggregate predicate to the runtime request.

The remaining debt for this shape is the preflight orchestration shell around
parent-key lookup and aggregate-threshold lookup. When correlated aggregate
subqueries become fully planner/executor-native, the preflight transform caller
is the intended deletion point.

## Surface Classification

Some code still uses helper-shaped names because that is where the first
subquery scaffolding landed. The distinction below is intentional: typed native
steps can be renamed or moved later, while compatibility fallback code should be
deleted when native execution covers the required shapes.

| Surface | Disposition | Current contract | Deletion or rename trigger |
|---|---|---|---|
| `scalar_subquery_materialization` | `typed_native_step` | Typed scalar subquery materialization through `NativeSubqueryStepExecutionRequest`. | Rename helper-shaped request/report wrappers after scalar materialization is owned directly by the planner/executor pipeline. |
| `parent_key_lookup` | `typed_native_step` | Typed parent-key lookup feeding correlated aggregate threshold work. | Rename helper-shaped request/report wrappers after correlated aggregate planning consumes `NativeSubqueryStep` directly. |
| `aggregate_threshold_lookup` | `typed_native_step` | Typed aggregate-threshold lookup feeding native correlated aggregate predicate thresholds. | Replace preflight orchestration with planner-owned aggregate-threshold execution. |
| `correlated_aggregate_preflight_transform` | `temporary_transform` | Temporary typed transform that consumes Q17-style correlated aggregate intent and attaches native predicate metadata. | Delete when correlated aggregate subqueries are represented and executed as native planner nodes. |
| `sql_backed_preflight_helper_executor` | `compatibility_fallback` | Fallback executor that routes helper SQL through `SQLRuntime` only when no native step is available or the default adapter cannot build a native runtime request yet. | Delete when scalar, parent-key, aggregate-threshold, and sibling-membership paths all have required native executors and the default adapter no longer needs SQL fallback. |

## Guardrails

- Every entry returned by `SQLRuntime.preflightRewriteRules()` must have an
  inventory entry in `qsruntime.preflightRewriteInventory()`.
- Each transform must expose a stable rule id, reason, source SQL shape,
  temporary strategy, helper-plan kind list, regression coverage note, and
  future IR replacement path.
- Transforms should emit optimizer trace metadata, per-rule duration, preflight
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
