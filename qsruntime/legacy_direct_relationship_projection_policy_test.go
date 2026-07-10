package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestLegacyDirectRelationshipProjectionPolicyDefaultsToNarrowed(t *testing.T) {
	edge := legacyDirectRelationshipEdge{
		childTable:      "lineitem",
		childField:      "l_orderkey",
		projectionScope: qsbridge.RelationshipVectorProjectionScopePredicateWindow,
	}
	scratchpad := newLegacyDirectRelationshipGraphScratchpad(map[string][]qsbridge.QuantaRownum{
		"l": {1, 2, 3, 4},
	}, []legacyDirectRelationshipEdge{{
		childRole:  "l",
		childTable: "lineitem",
	}})

	policy := legacyDirectRelationshipProjectionPolicy(edge, []qsbridge.QuantaRownum{1, 2}, scratchpad, 3)

	if policy.Strategy != legacyDirectRelationshipProjectionStrategyNarrowed {
		t.Fatalf("strategy = %q, want narrowed", policy.Strategy)
	}
	if policy.AppliedStrategy != legacyDirectRelationshipProjectionStrategyNarrowed {
		t.Fatalf("applied strategy = %q, want narrowed", policy.AppliedStrategy)
	}
	if !policy.ObserveOnly {
		t.Fatalf("observe only = false, want true")
	}
	if policy.Reason != "edge_scope_prefers_narrowed" {
		t.Fatalf("reason = %q, want edge_scope_prefers_narrowed", policy.Reason)
	}
}

func TestLegacyDirectRelationshipProjectionPolicyRecommendsBroadForReusableNearFullChildSet(t *testing.T) {
	edge := legacyDirectRelationshipEdge{
		childTable:      "lineitem",
		childField:      "l_orderkey",
		projectionScope: qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset,
	}
	initialRows := make([]qsbridge.QuantaRownum, 100)
	childRows := make([]qsbridge.QuantaRownum, 95)
	for i := range initialRows {
		initialRows[i] = qsbridge.QuantaRownum(i + 1)
	}
	copy(childRows, initialRows[:95])
	scratchpad := newLegacyDirectRelationshipGraphScratchpad(map[string][]qsbridge.QuantaRownum{
		"l": initialRows,
	}, []legacyDirectRelationshipEdge{{
		childRole:  "l",
		childTable: "lineitem",
	}})

	policy := legacyDirectRelationshipProjectionPolicy(edge, childRows, scratchpad, 2)

	if policy.Strategy != legacyDirectRelationshipProjectionStrategyBroadFromScratchpad {
		t.Fatalf("strategy = %q, want broad_from_scratchpad", policy.Strategy)
	}
	if policy.AppliedStrategy != legacyDirectRelationshipProjectionStrategyNarrowed {
		t.Fatalf("applied strategy = %q, want narrowed observe-only behavior", policy.AppliedStrategy)
	}
	if policy.Reason != "broad_projection_reusable" {
		t.Fatalf("reason = %q, want broad_projection_reusable", policy.Reason)
	}
	if policy.ChildRows != 95 || policy.InitialChildRows != 100 || policy.ReuseEstimate != 2 {
		t.Fatalf("policy counts = child %d initial %d reuse %d, want 95/100/2", policy.ChildRows, policy.InitialChildRows, policy.ReuseEstimate)
	}
}

func TestLegacyDirectRelationshipProjectionRowsForAppliedPolicyUsesScratchpadWhenBroadApplies(t *testing.T) {
	edge := legacyDirectRelationshipEdge{
		childTable:      "lineitem",
		childField:      "l_orderkey",
		projectionScope: qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset,
	}
	initialRows := []qsbridge.QuantaRownum{1, 2, 3, 4}
	childRows := []qsbridge.QuantaRownum{1, 2, 3}
	scratchpad := newLegacyDirectRelationshipGraphScratchpad(map[string][]qsbridge.QuantaRownum{
		"l": initialRows,
	}, []legacyDirectRelationshipEdge{{
		childRole:  "l",
		childTable: "lineitem",
	}})
	policy := legacyDirectRelationshipProjectionPolicy(edge, childRows, scratchpad, 2)
	policy.AppliedStrategy = legacyDirectRelationshipProjectionStrategyBroadFromScratchpad

	rows := legacyDirectRelationshipProjectionRowsForAppliedPolicy(edge, childRows, scratchpad, policy)

	if len(rows) != len(initialRows) {
		t.Fatalf("projection rows = %#v, want scratchpad initial rows", rows)
	}
}

func TestLegacyDirectRelationshipProjectionRowsForGraphReduceUsesCachedBroadProjection(t *testing.T) {
	edge := legacyDirectRelationshipEdge{
		childTable:      "lineitem",
		childField:      "l_orderkey",
		projectionScope: qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset,
	}
	initialRows := []qsbridge.QuantaRownum{1, 2, 3, 4}
	childRows := []qsbridge.QuantaRownum{1, 2}
	scratchpad := newLegacyDirectRelationshipGraphScratchpad(map[string][]qsbridge.QuantaRownum{
		"l": initialRows,
	}, []legacyDirectRelationshipEdge{{
		childRole:  "l",
		childTable: "lineitem",
	}})
	executor := LegacyDirectRelationshipVectorJoinExecutor{ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache()}
	fromTime, toTime := executor.legacyDirectRelationshipVectorProjectionWindowForEdge(ExecutionRequest{}, edge, initialRows)
	cacheKey := executor.legacyDirectRelationshipProjectionCacheKey(edge.childTable, edge.childField, fromTime, toTime, legacyDirectRelationshipBitmap(initialRows))
	executor.ProjectionCache.Put(cacheKey, testRelationshipVectorBSI(map[uint64]int64{1: 10}))
	policy := legacyDirectRelationshipProjectionPolicy(edge, childRows, scratchpad, 1)

	rows, applied := executor.legacyDirectRelationshipProjectionRowsForGraphReduce(ExecutionRequest{}, edge, childRows, scratchpad, policy)

	if len(rows) != len(initialRows) {
		t.Fatalf("projection rows = %#v, want cached broad scratchpad initial rows", rows)
	}
	if applied.AppliedStrategy != legacyDirectRelationshipProjectionStrategyBroadFromScratchpad || applied.Reason != "broad_projection_cache_hit" || applied.ObserveOnly {
		t.Fatalf("applied policy = %#v, want broad cache-hit policy", applied)
	}
}

func TestLegacyDirectRelationshipProjectionPolicyKeepsNarrowedForSmallChildSet(t *testing.T) {
	edge := legacyDirectRelationshipEdge{
		childTable:      "lineitem",
		childField:      "l_orderkey",
		projectionScope: qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset,
	}
	initialRows := make([]qsbridge.QuantaRownum, 100)
	childRows := make([]qsbridge.QuantaRownum, 50)
	scratchpad := newLegacyDirectRelationshipGraphScratchpad(map[string][]qsbridge.QuantaRownum{
		"l": initialRows,
	}, []legacyDirectRelationshipEdge{{
		childRole:  "l",
		childTable: "lineitem",
	}})

	policy := legacyDirectRelationshipProjectionPolicy(edge, childRows, scratchpad, 4)

	if policy.Strategy != legacyDirectRelationshipProjectionStrategyNarrowed {
		t.Fatalf("strategy = %q, want narrowed", policy.Strategy)
	}
	if policy.Reason != "child_rows_smaller_than_initial" {
		t.Fatalf("reason = %q, want child_rows_smaller_than_initial", policy.Reason)
	}
}

func TestLegacyDirectRelationshipProjectionReuseEstimateCountsRemainingMatchingEdges(t *testing.T) {
	edges := []legacyDirectRelationshipEdge{
		{childTable: "lineitem", childField: "l_orderkey"},
		{childTable: "orders", childField: "o_custkey"},
		{childTable: "lineitem", childField: "l_orderkey"},
	}

	got := legacyDirectRelationshipProjectionReuseEstimate(edges, 0, edges[0])

	if got != 2 {
		t.Fatalf("reuse estimate = %d, want 2", got)
	}
}
