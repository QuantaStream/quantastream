package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestLegacyDirectRelationshipEdgeOrderPolicyRanksSelectiveEdgesFirst(t *testing.T) {
	edges := []legacyDirectRelationshipEdge{
		{
			parentRole:      "p",
			parentTable:     "part",
			parentField:     "p_partkey",
			childRole:       "l",
			childTable:      "lineitem",
			childField:      "l_partkey",
			projectionScope: qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset,
		},
		{
			parentRole:      "r",
			parentTable:     "region",
			parentField:     "r_regionkey",
			childRole:       "n",
			childTable:      "nation",
			childField:      "n_regionkey",
			projectionScope: qsbridge.RelationshipVectorProjectionScopePredicateWindow,
		},
	}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"p": legacyDirectRelationshipTestRows(2000),
		"l": legacyDirectRelationshipTestRows(60175),
		"r": legacyDirectRelationshipTestRows(1),
		"n": legacyDirectRelationshipTestRows(5),
	}

	policy := legacyDirectRelationshipEdgeOrderPolicy(edges, rowsByRole, false)

	if !policy.ObserveOnly {
		t.Fatalf("observe only = false, want true")
	}
	if policy.AppliedStrategy != legacyDirectRelationshipEdgeOrderStrategyInput {
		t.Fatalf("applied strategy = %q, want input", policy.AppliedStrategy)
	}
	if got := legacyDirectRelationshipEdgeOrderSequence(policy.RecommendedSequence); got != "2,1" {
		t.Fatalf("recommended order = %q, want 2,1", got)
	}
	first := policy.Candidates[0]
	if first.InputOrdinal != 2 {
		t.Fatalf("first input ordinal = %d, want 2", first.InputOrdinal)
	}
	if first.Score >= policy.Candidates[1].Score {
		t.Fatalf("scores = %d then %d, want first lower", first.Score, policy.Candidates[1].Score)
	}
	if first.Reason != "selective_parent_domain" {
		t.Fatalf("reason = %q, want selective_parent_domain", first.Reason)
	}
}

func TestLegacyDirectRelationshipEdgeOrderPolicyRanksEmptyDomainFirst(t *testing.T) {
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "o", parentTable: "orders", childRole: "l", childTable: "lineitem"},
		{parentRole: "s", parentTable: "supplier", childRole: "l", childTable: "lineitem"},
	}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"o": legacyDirectRelationshipTestRows(100),
		"l": legacyDirectRelationshipTestRows(1000),
		"s": {},
	}

	policy := legacyDirectRelationshipEdgeOrderPolicy(edges, rowsByRole, false)

	if got := legacyDirectRelationshipEdgeOrderSequence(policy.RecommendedSequence); got != "2,1" {
		t.Fatalf("recommended order = %q, want 2,1", got)
	}
	if policy.Candidates[0].Reason != "empty_domain_first" {
		t.Fatalf("reason = %q, want empty_domain_first", policy.Candidates[0].Reason)
	}
}

func TestLegacyDirectRelationshipEdgeOrderPolicyProbesExposeRankedRecommendation(t *testing.T) {
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "large", parentTable: "large_parent", childRole: "fact", childTable: "fact"},
		{parentRole: "tiny", parentTable: "tiny_parent", childRole: "dim", childTable: "dim"},
	}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"large": legacyDirectRelationshipTestRows(1000),
		"fact":  legacyDirectRelationshipTestRows(10000),
		"tiny":  legacyDirectRelationshipTestRows(3),
		"dim":   legacyDirectRelationshipTestRows(30),
	}

	probes := legacyDirectRelationshipEdgeOrderPolicyProbes("graph_", legacyDirectRelationshipEdgeOrderPolicy(edges, rowsByRole, false))

	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_observe_only", "true")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_applied_order", "input")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_input_order", "1,2")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_recommended_order", "2,1")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_apply_requested", "false")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_apply_eligible", "false")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_apply_reason", "observe_only")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_first_changed_rank", "0")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_first_changed_input_edge", "0")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_rank_1_input_edge", "2")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_rank_1_reason", "selective_parent_domain")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_rank_1_projection_cost", "0")
	assertExecutionProbe(t, probes, "relationship_join", "graph_edge_policy_rank_1_projection_reuse_credit", "0")
}

func TestLegacyDirectRelationshipEdgeOrderExecutionCandidatesCanApplyRecommendation(t *testing.T) {
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "large", parentTable: "large_parent", childRole: "fact", childTable: "fact"},
		{parentRole: "tiny", parentTable: "tiny_parent", childRole: "dim", childTable: "dim"},
	}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"large": legacyDirectRelationshipTestRows(1000),
		"fact":  legacyDirectRelationshipTestRows(10000),
		"tiny":  legacyDirectRelationshipTestRows(3),
		"dim":   legacyDirectRelationshipTestRows(30),
	}

	policy := legacyDirectRelationshipEdgeOrderPolicy(edges, rowsByRole, true)
	candidates := legacyDirectRelationshipEdgeOrderExecutionCandidates(edges, policy)

	if policy.ObserveOnly {
		t.Fatalf("observe only = true, want false")
	}
	if policy.AppliedStrategy != legacyDirectRelationshipEdgeOrderStrategyFrontier {
		t.Fatalf("applied strategy = %q, want frontier", policy.AppliedStrategy)
	}
	if got := legacyDirectRelationshipEdgeOrderSequence(policy.RecommendedSequence); got != "2,1" {
		t.Fatalf("recommended order = %q, want 2,1", got)
	}
	if len(candidates) != 2 || candidates[0].InputOrdinal != 2 || candidates[1].InputOrdinal != 1 {
		t.Fatalf("candidate order = %#v, want input ordinals 2,1", candidates)
	}
	if policy.ApplyReason != "front_loaded_selectivity_change" {
		t.Fatalf("apply reason = %q, want front_loaded_selectivity_change", policy.ApplyReason)
	}
	if !policy.ApplyEligible {
		t.Fatalf("apply eligible = false, want true")
	}
	if policy.FirstChangedRank != 1 || policy.FirstChangedInput != 2 {
		t.Fatalf("first change = rank %d input %d, want rank 1 input 2", policy.FirstChangedRank, policy.FirstChangedInput)
	}
}

func TestLegacyDirectRelationshipEdgeOrderExecutionCandidatesKeepInputForLateOnlyRecommendation(t *testing.T) {
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "a", parentTable: "a", childRole: "b", childTable: "b"},
		{parentRole: "c", parentTable: "c", childRole: "d", childTable: "d"},
		{parentRole: "e", parentTable: "e", childRole: "f", childTable: "f"},
	}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"a": legacyDirectRelationshipTestRows(1),
		"b": legacyDirectRelationshipTestRows(10),
		"c": legacyDirectRelationshipTestRows(1000),
		"d": legacyDirectRelationshipTestRows(10000),
		"e": legacyDirectRelationshipTestRows(100),
		"f": legacyDirectRelationshipTestRows(1000),
	}

	policy := legacyDirectRelationshipEdgeOrderPolicy(edges, rowsByRole, true)
	candidates := legacyDirectRelationshipEdgeOrderExecutionCandidates(edges, policy)

	if policy.ObserveOnly {
		t.Fatalf("observe only = true, want false")
	}
	if policy.AppliedStrategy != legacyDirectRelationshipEdgeOrderStrategyInput {
		t.Fatalf("applied strategy = %q, want input", policy.AppliedStrategy)
	}
	if got := legacyDirectRelationshipEdgeOrderSequence(policy.RecommendedSequence); got != "1,3,2" {
		t.Fatalf("recommended order = %q, want 1,3,2", got)
	}
	if policy.ApplyReason != "recommendation_not_front_loaded" {
		t.Fatalf("apply reason = %q, want recommendation_not_front_loaded", policy.ApplyReason)
	}
	if policy.ApplyEligible {
		t.Fatalf("apply eligible = true, want false")
	}
	if policy.FirstChangedRank != 2 || policy.FirstChangedInput != 3 {
		t.Fatalf("first change = rank %d input %d, want rank 2 input 3", policy.FirstChangedRank, policy.FirstChangedInput)
	}
	if len(candidates) != 3 || candidates[0].InputOrdinal != 1 || candidates[1].InputOrdinal != 2 || candidates[2].InputOrdinal != 3 {
		t.Fatalf("candidate order = %#v, want input ordinals 1,2,3", candidates)
	}
}

func TestLegacyDirectRelationshipEdgeOrderPolicyPrefersConnectedFrontier(t *testing.T) {
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "a", parentTable: "a", childRole: "b", childTable: "b"},
		{parentRole: "c", parentTable: "c", childRole: "d", childTable: "d"},
		{parentRole: "b", parentTable: "b", childRole: "e", childTable: "e"},
	}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"a": legacyDirectRelationshipTestRows(10),
		"b": legacyDirectRelationshipTestRows(100),
		"c": legacyDirectRelationshipTestRows(20),
		"d": legacyDirectRelationshipTestRows(200),
		"e": legacyDirectRelationshipTestRows(1000),
	}

	policy := legacyDirectRelationshipEdgeOrderPolicy(edges, rowsByRole, false)

	if got := legacyDirectRelationshipEdgeOrderSequence(policy.RecommendedSequence); got != "1,3,2" {
		t.Fatalf("recommended order = %q, want 1,3,2", got)
	}
	if policy.Candidates[0].FrontierConnected {
		t.Fatalf("first candidate frontier connected = true, want false")
	}
	if !policy.Candidates[1].FrontierConnected {
		t.Fatalf("second candidate frontier connected = false, want true")
	}
	if policy.Candidates[2].FrontierConnected {
		t.Fatalf("third candidate frontier connected = true, want false")
	}
}

func TestLegacyDirectRelationshipEdgeOrderPolicyCreditsReusableBroadVector(t *testing.T) {
	edge := legacyDirectRelationshipEdge{
		parentRole:      "p",
		parentTable:     "part",
		childRole:       "l",
		childTable:      "lineitem",
		childField:      "l_partkey",
		projectionScope: qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset,
	}
	candidate := legacyDirectRelationshipEdgeOrderCandidate{
		Edge:            edge,
		ParentRows:      2000,
		ChildRows:       60000,
		CurrentRows:     2000,
		ProjectionReuse: 2,
		ProjectionScope: qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset,
	}
	candidate.ProjectionCost = legacyDirectRelationshipEdgeOrderProjectionCost(candidate)
	candidate.ProjectionReuseCredit = legacyDirectRelationshipEdgeOrderProjectionReuseCredit(candidate)

	score, reason := legacyDirectRelationshipEdgeOrderScore(candidate)

	if candidate.ProjectionCost != 6000 {
		t.Fatalf("projection cost = %d, want 6000", candidate.ProjectionCost)
	}
	if candidate.ProjectionReuseCredit != 3000 {
		t.Fatalf("projection reuse credit = %d, want 3000", candidate.ProjectionReuseCredit)
	}
	if score != 5000 {
		t.Fatalf("score = %d, want 5000", score)
	}
	if reason != "selective_parent_domain_broad_projection_cost_reusable_vector" {
		t.Fatalf("reason = %q, want selective_parent_domain_broad_projection_cost_reusable_vector", reason)
	}
}

func TestLegacyDirectRelationshipEdgeOrderFirstChangeReportsNoChange(t *testing.T) {
	rank, input := legacyDirectRelationshipEdgeOrderFirstChange([]int{1, 2, 3})

	if rank != 0 || input != 0 {
		t.Fatalf("first change = rank %d input %d, want rank 0 input 0", rank, input)
	}
}

func TestLegacyDirectRelationshipEdgeOrderRemainingProjectionReuseUsesAppliedOrder(t *testing.T) {
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "a", parentTable: "a", childRole: "x1", childTable: "x", childField: "x_fk"},
		{parentRole: "b", parentTable: "b", childRole: "y", childTable: "y", childField: "y_fk"},
		{parentRole: "c", parentTable: "c", childRole: "x2", childTable: "x", childField: "x_fk"},
	}
	candidates := []legacyDirectRelationshipEdgeOrderCandidate{
		{InputOrdinal: 1, Edge: edges[0]},
		{InputOrdinal: 3, Edge: edges[2]},
		{InputOrdinal: 2, Edge: edges[1]},
	}

	got := legacyDirectRelationshipEdgeOrderRemainingProjectionReuse(candidates, 0, edges[0])

	if got != 2 {
		t.Fatalf("remaining reuse = %d, want 2", got)
	}
}

func legacyDirectRelationshipTestRows(count int) []qsbridge.QuantaRownum {
	rows := make([]qsbridge.QuantaRownum, count)
	for i := range rows {
		rows[i] = qsbridge.QuantaRownum(i + 1)
	}
	return rows
}
