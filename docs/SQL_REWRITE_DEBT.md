# SQL Rewrite Debt History

Active SQL-text rewrite debt is retired. `SQLRuntime.ExecuteSQL` no longer
passes user SQL through preflight rewrite rules before planning. The current
contract is parser and binder first, then native subquery preparation and
runtime-native predicate metadata.

This document remains as a historical checkpoint until the helper-shaped names
below are renamed or moved into their final planner/executor packages.

## Active Preflight Rewrites

None.

`SQLRuntime.preflightRewriteRules()` and `qsruntime.preflightRewriteInventory()`
must remain empty. New SQL support should be added through parser, IR, planner,
optimizer, executor, or native subquery preparation surfaces rather than by
rewriting SQL text.

## Retired Shape

The former `correlated_aggregate_preflight` path handled Q17-style correlated
aggregate predicates such as:

```sql
l_quantity < (
  select factor * avg(l2.l_quantity)
  from lineitem as l2
  where l2.l_partkey = p.p_partkey
)
```

That shape is now parsed as typed correlated aggregate intent and prepared as a
native correlated aggregate predicate. Parent-key lookup and aggregate-threshold
materialization still use helper-shaped request/report structs, but helper SQL
is fallback/debug context rather than the execution contract.

The optimizer trace now records `correlated_aggregate_native_predicate` for this
runtime preparation step.

## Surface Classification

Some code still uses helper-shaped names because that is where the first
subquery scaffolding landed. The distinction below is intentional: typed native
steps can be renamed or moved later, while compatibility fallback code should be
deleted when native execution covers the required shapes.

| Surface | Disposition | Current contract | Deletion or rename trigger |
|---|---|---|---|
| `scalar_subquery_materialization` | `typed_native_step` | Typed scalar subquery materialization through `NativeSubqueryStepExecutionRequest`. | Rename helper-shaped request/report wrappers after scalar materialization is owned directly by the planner/executor pipeline. |
| `parent_key_lookup` | `typed_native_step` | Typed parent-key lookup feeding correlated aggregate threshold work. | Rename helper-shaped request/report wrappers after correlated aggregate planning consumes `NativeSubqueryStep` directly. |
| `aggregate_threshold_lookup` | `typed_native_step` | Typed aggregate-threshold lookup feeding native correlated aggregate predicate thresholds. | Rename helper-shaped request/report wrappers after aggregate-threshold execution is owned directly by the planner/executor pipeline. |
| `correlated_aggregate_preflight_transform` | `temporary_transform` | Historical transform name for the retired Q17 SQL rewrite path. | Delete or rename when correlated aggregate native preparation is moved into its final planner/executor package. |
| `sql_backed_preflight_helper_executor` | `compatibility_fallback` | Fallback executor that routes helper SQL through `SQLRuntime` only when no native step is available or the default adapter cannot build a native runtime request yet. | Delete when scalar, parent-key, aggregate-threshold, and sibling-membership paths all have required native executors and the default adapter no longer needs SQL fallback. |

## Guardrails

- Keep `SQLRuntime.preflightRewriteRules()` empty.
- Do not add new SQL-text rewrite rules for planner or parser gaps.
- Prefer typed `QueryIR`, native subquery steps, optimizer traces, and runtime
  predicate metadata.
- Helper SQL may exist as debug/fallback context, but it should not be the
  primary execution contract for native-ready shapes.
- Delete temporary rewrite code as soon as the equivalent parser, IR, planner,
  or executor capability makes it eligible for deletion.
