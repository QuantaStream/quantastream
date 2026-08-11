package qsruntime

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type legacyDirectRelationshipEdgeOrderStrategy string

const (
	legacyDirectRelationshipEdgeOrderStrategyInput       legacyDirectRelationshipEdgeOrderStrategy = "input"
	legacyDirectRelationshipEdgeOrderStrategyCardinality legacyDirectRelationshipEdgeOrderStrategy = "cardinality"
	legacyDirectRelationshipEdgeOrderStrategyFrontier    legacyDirectRelationshipEdgeOrderStrategy = "frontier_cardinality"
)

type legacyDirectRelationshipEdgeOrderCandidate struct {
	InputOrdinal          int
	RecommendedOrdinal    int
	Edge                  legacyDirectRelationshipEdge
	ParentRows            int
	ChildRows             int
	CurrentRows           int
	ProjectionReuse       int
	ProjectionScope       qsbridge.RelationshipVectorProjectionScope
	ProjectionCost        int
	ProjectionReuseCredit int
	Score                 int
	Reason                string
	FrontierConnected     bool
	AppliedOrder          legacyDirectRelationshipEdgeOrderStrategy
	RecommendedStrategy   legacyDirectRelationshipEdgeOrderStrategy
	ObserveOnly           bool
}

type legacyDirectRelationshipEdgeOrderPolicyResult struct {
	Candidates          []legacyDirectRelationshipEdgeOrderCandidate
	InputSequence       []int
	RecommendedSequence []int
	AppliedStrategy     legacyDirectRelationshipEdgeOrderStrategy
	RecommendedStrategy legacyDirectRelationshipEdgeOrderStrategy
	ApplyRequested      bool
	ApplyEligible       bool
	ApplyReason         string
	FirstChangedRank    int
	FirstChangedInput   int
	ObserveOnly         bool
}

type legacyDirectRelationshipSinglePassPolicyResult struct {
	Eligible bool
	Mode     string
	Reason   string
}

func legacyDirectRelationshipEdgeOrderPolicy(edges []legacyDirectRelationshipEdge, rowsByRole map[string][]qsbridge.QuantaRownum, applyRecommended bool) legacyDirectRelationshipEdgeOrderPolicyResult {
	result := legacyDirectRelationshipEdgeOrderPolicyResult{
		Candidates:          make([]legacyDirectRelationshipEdgeOrderCandidate, 0, len(edges)),
		InputSequence:       make([]int, 0, len(edges)),
		RecommendedSequence: make([]int, 0, len(edges)),
		AppliedStrategy:     legacyDirectRelationshipEdgeOrderStrategyInput,
		RecommendedStrategy: legacyDirectRelationshipEdgeOrderStrategyFrontier,
		ApplyRequested:      applyRecommended,
		ApplyReason:         "observe_only",
		ObserveOnly:         true,
	}
	for i, edge := range edges {
		result.InputSequence = append(result.InputSequence, i+1)
		parentRows := len(rowsByRole[edge.parentKey()])
		childRows := len(rowsByRole[edge.childKey()])
		reuse := legacyDirectRelationshipProjectionReuseEstimate(edges, i, edge)
		candidate := legacyDirectRelationshipEdgeOrderCandidate{
			InputOrdinal:        i + 1,
			Edge:                edge,
			ParentRows:          parentRows,
			ChildRows:           childRows,
			CurrentRows:         legacyDirectRelationshipEdgeOrderCurrentRows(parentRows, childRows),
			ProjectionReuse:     reuse,
			ProjectionScope:     edge.projectionScope,
			AppliedOrder:        legacyDirectRelationshipEdgeOrderStrategyInput,
			RecommendedStrategy: legacyDirectRelationshipEdgeOrderStrategyFrontier,
			ObserveOnly:         !applyRecommended,
		}
		candidate.ProjectionCost = legacyDirectRelationshipEdgeOrderProjectionCost(candidate)
		candidate.ProjectionReuseCredit = legacyDirectRelationshipEdgeOrderProjectionReuseCredit(candidate)
		if applyRecommended {
			candidate.AppliedOrder = legacyDirectRelationshipEdgeOrderStrategyFrontier
		}
		candidate.Score, candidate.Reason = legacyDirectRelationshipEdgeOrderScore(candidate)
		result.Candidates = append(result.Candidates, candidate)
	}
	result.Candidates = legacyDirectRelationshipEdgeOrderFrontierRecommendation(result.Candidates)
	for i := range result.Candidates {
		result.Candidates[i].RecommendedOrdinal = i + 1
		result.RecommendedSequence = append(result.RecommendedSequence, result.Candidates[i].InputOrdinal)
	}
	if applyRecommended {
		result.ObserveOnly = false
		result.FirstChangedRank, result.FirstChangedInput = legacyDirectRelationshipEdgeOrderFirstChange(result.RecommendedSequence)
		result.ApplyEligible = legacyDirectRelationshipEdgeOrderShouldApply(edges, result.RecommendedSequence)
		if result.ApplyEligible {
			result.AppliedStrategy = legacyDirectRelationshipEdgeOrderStrategyFrontier
			result.ApplyReason = "front_loaded_selectivity_change"
		} else {
			result.ApplyReason = "recommendation_not_front_loaded"
		}
		for i := range result.Candidates {
			result.Candidates[i].AppliedOrder = result.AppliedStrategy
			result.Candidates[i].ObserveOnly = result.ObserveOnly
		}
	}
	return result
}

func legacyDirectRelationshipSinglePassParentToChildPolicy(candidates []legacyDirectRelationshipEdgeOrderCandidate) legacyDirectRelationshipSinglePassPolicyResult {
	result := legacyDirectRelationshipSinglePassPolicyResult{
		Mode:   "single_pass_parent_to_child",
		Reason: "topological_parent_to_child",
	}
	if len(candidates) == 0 {
		result.Reason = "no_edges"
		return result
	}
	for i, candidate := range candidates {
		edge := candidate.Edge
		parentKey := edge.parentKey()
		childKey := edge.childKey()
		if parentKey == "" || childKey == "" {
			result.Reason = "missing_role_key"
			return result
		}
		if parentKey == childKey {
			result.Reason = "self_edge"
			return result
		}
		if edge.sqlKind != qsbridge.JoinKindInner || edge.leftOuterPreservesParent {
			result.Reason = "non_inner_join"
			return result
		}
		for dependencyIndex, dependency := range candidates {
			if dependencyIndex == i {
				continue
			}
			if dependency.Edge.childKey() == parentKey && dependencyIndex > i {
				result.Reason = "dependency_after_consumer"
				return result
			}
		}
	}
	result.Eligible = true
	return result
}

func legacyDirectRelationshipSinglePassPolicyProbes(prefix string, policy legacyDirectRelationshipSinglePassPolicyResult, applied bool) []ExecutionProbe {
	return []ExecutionProbe{
		legacyDirectRelationshipProbe(prefix+"single_pass_mode", policy.Mode),
		legacyDirectRelationshipProbe(prefix+"single_pass_eligible", strconv.FormatBool(policy.Eligible)),
		legacyDirectRelationshipProbe(prefix+"single_pass_reason", policy.Reason),
		legacyDirectRelationshipProbe(prefix+"single_pass_applied", strconv.FormatBool(applied)),
	}
}

func legacyDirectRelationshipEdgeOrderExecutionCandidates(edges []legacyDirectRelationshipEdge, policy legacyDirectRelationshipEdgeOrderPolicyResult) []legacyDirectRelationshipEdgeOrderCandidate {
	if policy.AppliedStrategy == legacyDirectRelationshipEdgeOrderStrategyCardinality || policy.AppliedStrategy == legacyDirectRelationshipEdgeOrderStrategyFrontier {
		return append([]legacyDirectRelationshipEdgeOrderCandidate(nil), policy.Candidates...)
	}
	candidates := make([]legacyDirectRelationshipEdgeOrderCandidate, 0, len(edges))
	for i, edge := range edges {
		candidates = append(candidates, legacyDirectRelationshipEdgeOrderCandidate{
			InputOrdinal:        i + 1,
			RecommendedOrdinal:  i + 1,
			Edge:                edge,
			AppliedOrder:        legacyDirectRelationshipEdgeOrderStrategyInput,
			RecommendedStrategy: policy.RecommendedStrategy,
			ObserveOnly:         true,
		})
	}
	return candidates
}

func legacyDirectRelationshipEdgeOrderFrontierRecommendation(candidates []legacyDirectRelationshipEdgeOrderCandidate) []legacyDirectRelationshipEdgeOrderCandidate {
	remaining := append([]legacyDirectRelationshipEdgeOrderCandidate(nil), candidates...)
	ordered := make([]legacyDirectRelationshipEdgeOrderCandidate, 0, len(candidates))
	frontier := make(map[string]struct{}, len(candidates)*2)
	for len(remaining) > 0 {
		bestIndex := legacyDirectRelationshipEdgeOrderBestCandidateIndex(remaining, frontier)
		bestIndex = legacyDirectRelationshipEdgeOrderDependencyRootIndex(remaining, bestIndex)
		best := remaining[bestIndex]
		best.FrontierConnected = legacyDirectRelationshipEdgeOrderCandidateTouchesFrontier(best, frontier)
		ordered = append(ordered, best)
		frontier[best.Edge.parentKey()] = struct{}{}
		frontier[best.Edge.childKey()] = struct{}{}
		remaining = append(remaining[:bestIndex], remaining[bestIndex+1:]...)
	}
	return ordered
}

func legacyDirectRelationshipEdgeOrderDependencyRootIndex(candidates []legacyDirectRelationshipEdgeOrderCandidate, candidateIndex int) int {
	if candidateIndex < 0 || candidateIndex >= len(candidates) {
		return candidateIndex
	}
	seen := make(map[int]struct{}, len(candidates))
	for {
		if _, ok := seen[candidateIndex]; ok {
			return candidateIndex
		}
		seen[candidateIndex] = struct{}{}
		dependencyIndex := legacyDirectRelationshipEdgeOrderParentDependencyIndex(candidates, candidateIndex)
		if dependencyIndex == -1 {
			return candidateIndex
		}
		candidateIndex = dependencyIndex
	}
}

func legacyDirectRelationshipEdgeOrderParentDependencyIndex(candidates []legacyDirectRelationshipEdgeOrderCandidate, candidateIndex int) int {
	if candidateIndex < 0 || candidateIndex >= len(candidates) {
		return -1
	}
	parentKey := candidates[candidateIndex].Edge.parentKey()
	if parentKey == "" {
		return -1
	}
	bestIndex := -1
	for i, candidate := range candidates {
		if i == candidateIndex || candidate.Edge.childKey() != parentKey {
			continue
		}
		if bestIndex == -1 || legacyDirectRelationshipEdgeOrderCandidateLess(candidate, candidates[bestIndex]) {
			bestIndex = i
		}
	}
	return bestIndex
}

func legacyDirectRelationshipEdgeOrderBestCandidateIndex(candidates []legacyDirectRelationshipEdgeOrderCandidate, frontier map[string]struct{}) int {
	bestIndex := -1
	for i, candidate := range candidates {
		if len(frontier) > 0 && !legacyDirectRelationshipEdgeOrderCandidateTouchesFrontier(candidate, frontier) {
			continue
		}
		if bestIndex == -1 || legacyDirectRelationshipEdgeOrderCandidateLess(candidate, candidates[bestIndex]) {
			bestIndex = i
		}
	}
	if bestIndex != -1 {
		return bestIndex
	}
	bestIndex = 0
	for i := 1; i < len(candidates); i++ {
		if legacyDirectRelationshipEdgeOrderCandidateLess(candidates[i], candidates[bestIndex]) {
			bestIndex = i
		}
	}
	return bestIndex
}

func legacyDirectRelationshipEdgeOrderCandidateTouchesFrontier(candidate legacyDirectRelationshipEdgeOrderCandidate, frontier map[string]struct{}) bool {
	if len(frontier) == 0 {
		return false
	}
	if _, ok := frontier[candidate.Edge.parentKey()]; ok {
		return true
	}
	if _, ok := frontier[candidate.Edge.childKey()]; ok {
		return true
	}
	return false
}

func legacyDirectRelationshipEdgeOrderCandidateLess(left legacyDirectRelationshipEdgeOrderCandidate, right legacyDirectRelationshipEdgeOrderCandidate) bool {
	if left.Score != right.Score {
		return left.Score < right.Score
	}
	return left.InputOrdinal < right.InputOrdinal
}

func legacyDirectRelationshipEdgeOrderCurrentRows(parentRows int, childRows int) int {
	if parentRows == 0 || childRows == 0 {
		return 0
	}
	if parentRows < childRows {
		return parentRows
	}
	return childRows
}

func legacyDirectRelationshipEdgeOrderShouldApply(edges []legacyDirectRelationshipEdge, recommendedSequence []int) bool {
	if len(edges) < 2 || len(recommendedSequence) != len(edges) {
		return false
	}
	if recommendedSequence[0] == 1 {
		return false
	}
	for i, inputOrdinal := range recommendedSequence {
		if inputOrdinal != i+1 {
			return true
		}
	}
	return false
}

func legacyDirectRelationshipEdgeOrderFirstChange(recommendedSequence []int) (int, int) {
	for i, inputOrdinal := range recommendedSequence {
		if inputOrdinal != i+1 {
			return i + 1, inputOrdinal
		}
	}
	return 0, 0
}

func legacyDirectRelationshipEdgeOrderScore(candidate legacyDirectRelationshipEdgeOrderCandidate) (int, string) {
	if candidate.CurrentRows == 0 {
		return 0, "empty_domain_first"
	}
	score := candidate.CurrentRows + candidate.ProjectionCost - candidate.ProjectionReuseCredit
	if score < 1 {
		score = 1
	}
	reason := "smallest_candidate_domain"
	if candidate.ChildRows > candidate.ParentRows && candidate.ParentRows > 0 {
		reason = "selective_parent_domain"
	}
	if candidate.ParentRows > candidate.ChildRows && candidate.ChildRows > 0 {
		reason = "selective_child_domain"
	}
	if candidate.ProjectionCost > 0 {
		if reason == "smallest_candidate_domain" {
			reason = "broad_projection_cost"
		} else {
			reason = reason + "_broad_projection_cost"
		}
	}
	if candidate.ProjectionReuseCredit > 0 {
		reason = reason + "_reusable_vector"
	}
	return score, reason
}

func legacyDirectRelationshipEdgeOrderProjectionCost(candidate legacyDirectRelationshipEdgeOrderCandidate) int {
	if candidate.ProjectionScope != qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset {
		return 0
	}
	if candidate.ChildRows <= 0 {
		return 0
	}
	cost := candidate.ChildRows / 10
	if cost == 0 {
		return 1
	}
	return cost
}

func legacyDirectRelationshipEdgeOrderProjectionReuseCredit(candidate legacyDirectRelationshipEdgeOrderCandidate) int {
	if candidate.ProjectionReuse <= 1 || candidate.ProjectionCost <= 0 {
		return 0
	}
	return candidate.ProjectionCost * (candidate.ProjectionReuse - 1) / candidate.ProjectionReuse
}

func legacyDirectRelationshipEdgeOrderPolicyProbes(prefix string, policy legacyDirectRelationshipEdgeOrderPolicyResult) []ExecutionProbe {
	probes := []ExecutionProbe{
		legacyDirectRelationshipProbe(prefix+"edge_policy_observe_only", strconv.FormatBool(policy.ObserveOnly)),
		legacyDirectRelationshipProbe(prefix+"edge_policy_applied_order", string(policy.AppliedStrategy)),
		legacyDirectRelationshipProbe(prefix+"edge_policy_input_order", legacyDirectRelationshipEdgeOrderSequence(policy.InputSequence)),
		legacyDirectRelationshipProbe(prefix+"edge_policy_recommended_order", legacyDirectRelationshipEdgeOrderSequence(policy.RecommendedSequence)),
		legacyDirectRelationshipProbe(prefix+"edge_policy_apply_requested", strconv.FormatBool(policy.ApplyRequested)),
		legacyDirectRelationshipProbe(prefix+"edge_policy_apply_eligible", strconv.FormatBool(policy.ApplyEligible)),
		legacyDirectRelationshipProbe(prefix+"edge_policy_apply_reason", policy.ApplyReason),
		legacyDirectRelationshipProbe(prefix+"edge_policy_first_changed_rank", strconv.Itoa(policy.FirstChangedRank)),
		legacyDirectRelationshipProbe(prefix+"edge_policy_first_changed_input_edge", strconv.Itoa(policy.FirstChangedInput)),
	}
	for _, candidate := range policy.Candidates {
		candidatePrefix := fmt.Sprintf("%sedge_policy_rank_%d_", prefix, candidate.RecommendedOrdinal)
		probes = append(probes,
			legacyDirectRelationshipProbe(candidatePrefix+"input_edge", strconv.Itoa(candidate.InputOrdinal)),
			legacyDirectRelationshipProbe(candidatePrefix+"parent_role", candidate.Edge.parentKey()),
			legacyDirectRelationshipProbe(candidatePrefix+"child_role", candidate.Edge.childKey()),
			legacyDirectRelationshipProbe(candidatePrefix+"parent_rows", strconv.Itoa(candidate.ParentRows)),
			legacyDirectRelationshipProbe(candidatePrefix+"child_rows", strconv.Itoa(candidate.ChildRows)),
			legacyDirectRelationshipProbe(candidatePrefix+"score", strconv.Itoa(candidate.Score)),
			legacyDirectRelationshipProbe(candidatePrefix+"reason", candidate.Reason),
			legacyDirectRelationshipProbe(candidatePrefix+"frontier_connected", strconv.FormatBool(candidate.FrontierConnected)),
			legacyDirectRelationshipProbe(candidatePrefix+"projection_cost", strconv.Itoa(candidate.ProjectionCost)),
			legacyDirectRelationshipProbe(candidatePrefix+"projection_reuse_credit", strconv.Itoa(candidate.ProjectionReuseCredit)),
			legacyDirectRelationshipProbe(candidatePrefix+"projection_reuse", strconv.Itoa(candidate.ProjectionReuse)),
			legacyDirectRelationshipProbe(candidatePrefix+"projection_scope", string(candidate.ProjectionScope)),
		)
	}
	return probes
}

func legacyDirectRelationshipEdgeOrderRemainingProjectionReuse(candidates []legacyDirectRelationshipEdgeOrderCandidate, start int, edge legacyDirectRelationshipEdge) int {
	reuse := 0
	for _, candidate := range candidates[start:] {
		if candidate.Edge.childTable == edge.childTable && candidate.Edge.childField == edge.childField {
			reuse++
		}
	}
	return reuse
}

func legacyDirectRelationshipEdgeOrderSequence(sequence []int) string {
	if len(sequence) == 0 {
		return ""
	}
	parts := make([]string, 0, len(sequence))
	for _, ordinal := range sequence {
		parts = append(parts, strconv.Itoa(ordinal))
	}
	return strings.Join(parts, ",")
}
