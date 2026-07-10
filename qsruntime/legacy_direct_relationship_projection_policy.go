package qsruntime

import (
	"strconv"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type legacyDirectRelationshipProjectionStrategy string

const (
	legacyDirectRelationshipProjectionStrategyNarrowed            legacyDirectRelationshipProjectionStrategy = "narrowed"
	legacyDirectRelationshipProjectionStrategyBroadFromScratchpad legacyDirectRelationshipProjectionStrategy = "broad_from_scratchpad"
)

type legacyDirectRelationshipProjectionPolicyResult struct {
	Strategy         legacyDirectRelationshipProjectionStrategy
	AppliedStrategy  legacyDirectRelationshipProjectionStrategy
	Reason           string
	ChildRows        int
	InitialChildRows int
	ReuseEstimate    int
	ObserveOnly      bool
}

func legacyDirectRelationshipProjectionPolicy(edge legacyDirectRelationshipEdge, childRows []qsbridge.QuantaRownum, scratchpad legacyDirectRelationshipGraphScratchpad, reuseEstimate int) legacyDirectRelationshipProjectionPolicyResult {
	result := legacyDirectRelationshipProjectionPolicyResult{
		Strategy:        legacyDirectRelationshipProjectionStrategyNarrowed,
		AppliedStrategy: legacyDirectRelationshipProjectionStrategyNarrowed,
		Reason:          "narrowed_behavior_preserved",
		ChildRows:       len(childRows),
		ReuseEstimate:   reuseEstimate,
		ObserveOnly:     true,
	}
	initialRows, ok := scratchpad.initialRowsForTable(edge.childTable)
	if !ok || len(initialRows) == 0 {
		result.Reason = "initial_child_rows_unavailable"
		return result
	}
	result.InitialChildRows = len(initialRows)
	if edge.projectionScope != qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset {
		result.Reason = "edge_scope_prefers_narrowed"
		return result
	}
	if reuseEstimate <= 1 {
		result.Reason = "single_use_projection"
		return result
	}
	if len(childRows) < legacyDirectRelationshipProjectionBroadThreshold(len(initialRows)) {
		result.Reason = "child_rows_smaller_than_initial"
		return result
	}
	result.Strategy = legacyDirectRelationshipProjectionStrategyBroadFromScratchpad
	result.Reason = "broad_projection_reusable"
	return result
}

func legacyDirectRelationshipProjectionRowsForAppliedPolicy(edge legacyDirectRelationshipEdge, childRows []qsbridge.QuantaRownum, scratchpad legacyDirectRelationshipGraphScratchpad, policy legacyDirectRelationshipProjectionPolicyResult) []qsbridge.QuantaRownum {
	if policy.AppliedStrategy != legacyDirectRelationshipProjectionStrategyBroadFromScratchpad {
		return childRows
	}
	initialRows, ok := scratchpad.initialRowsForTable(edge.childTable)
	if !ok || len(initialRows) == 0 {
		return childRows
	}
	return initialRows
}

func legacyDirectRelationshipProjectionBroadThreshold(initialRows int) int {
	if initialRows <= 0 {
		return 0
	}
	return (initialRows*9 + 9) / 10
}

func legacyDirectRelationshipProjectionReuseEstimate(edges []legacyDirectRelationshipEdge, start int, edge legacyDirectRelationshipEdge) int {
	reuse := 0
	for _, candidate := range edges[start:] {
		if candidate.childTable == edge.childTable && candidate.childField == edge.childField {
			reuse++
		}
	}
	return reuse
}

func legacyDirectRelationshipProjectionPolicyProbes(prefix string, policy legacyDirectRelationshipProjectionPolicyResult) []ExecutionProbe {
	return []ExecutionProbe{
		legacyDirectRelationshipProbe(prefix+"projection_policy_strategy", string(policy.Strategy)),
		legacyDirectRelationshipProbe(prefix+"projection_policy_applied_strategy", string(policy.AppliedStrategy)),
		legacyDirectRelationshipProbe(prefix+"projection_policy_reason", policy.Reason),
		legacyDirectRelationshipProbe(prefix+"projection_policy_child_rows", strconv.Itoa(policy.ChildRows)),
		legacyDirectRelationshipProbe(prefix+"projection_policy_initial_child_rows", strconv.Itoa(policy.InitialChildRows)),
		legacyDirectRelationshipProbe(prefix+"projection_policy_reuse_estimate", strconv.Itoa(policy.ReuseEstimate)),
		legacyDirectRelationshipProbe(prefix+"projection_policy_observe_only", strconv.FormatBool(policy.ObserveOnly)),
	}
}
