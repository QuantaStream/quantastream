package qsbridge

import (
	"reflect"
	"testing"
)

func TestRelationshipVectorProjectionReadsDescribeChildToParentTranslation(t *testing.T) {
	lineitem := TableInstance{Table: "lineitem", Alias: "l"}
	orders := TableInstance{Table: "orders", Alias: "o"}
	edge := RelationshipJoinPlanEdge{
		Left:          FieldRef{Table: lineitem, Name: "l_orderkey"},
		Right:         FieldRef{Table: orders, Name: "o_orderkey"},
		SQLKind:       JoinKindInner,
		ExecutionKind: RelationshipJoinExecutionVector,
		Intent:        RelationshipJoinOperationReduce,
		EncodingKind:  RelationshipEncodingVector,
	}
	request := RelationshipJoinPlan{Edges: []RelationshipJoinPlanEdge{edge}}.VectorRequest("lineitem")

	reads := request.RelationshipVectorProjectionReads(map[string]RownumDomainSet{
		"l": {Domain: RownumDomain{Table: lineitem, Role: "l"}, Rownums: []QuantaRownum{10, 11}},
	})

	if len(reads) != 1 {
		t.Fatalf("reads = %#v, want one", reads)
	}
	read := reads[0]
	if read.ID != "relationship_vector.1.l.l_orderkey.o.o_orderkey" || read.ProbePrefix != "relationship_vector_1_l_l_orderkey_o_o_orderkey_" {
		t.Fatalf("read identity = %#v, want stable relationship-vector identity", read)
	}
	if read.Input.Domain.Name() != "l" || read.OutputDomain.Name() != "o" {
		t.Fatalf("domains = %s -> %s, want l -> o", read.Input.Domain.Name(), read.OutputDomain.Name())
	}
	if read.Translation.Direction != RownumDomainTranslationChildToParent || !read.Translation.ChangesDomain() {
		t.Fatalf("translation = %#v, want child-to-parent domain change", read.Translation)
	}
	if read.ProjectionScope != RelationshipVectorProjectionScopePredicateWindow {
		t.Fatalf("projection scope = %q, want predicate window", read.ProjectionScope)
	}
	if !read.CoveragePlan.VerifyCoverage ||
		read.CoveragePlan.ExpectStatus != RelationshipVectorProjectionCoverageComplete ||
		read.CoveragePlan.RecoveryPolicy != RelationshipVectorProjectionRecoveryBroadenAndIntersect {
		t.Fatalf("coverage plan = %#v, want verified complete coverage with broaden-and-intersect recovery", read.CoveragePlan)
	}
	if !read.Cacheable {
		t.Fatalf("read should be cacheable by default")
	}
}

func TestRelationshipVectorProjectionReadCarriesPlannedCoveragePolicy(t *testing.T) {
	lineitem := TableInstance{Table: "lineitem", Alias: "l"}
	orders := TableInstance{Table: "orders", Alias: "o"}
	edge := RelationshipJoinPlanEdge{
		Left:            FieldRef{Table: lineitem, Name: "l_orderkey"},
		Right:           FieldRef{Table: orders, Name: "o_orderkey"},
		ExecutionKind:   RelationshipJoinExecutionVector,
		Intent:          RelationshipJoinOperationReduce,
		EncodingKind:    RelationshipEncodingVector,
		ProjectionScope: RelationshipVectorProjectionScopeBroadFromFoundset,
		CoveragePlan:    NewRelationshipVectorProjectionCoveragePlan(RelationshipVectorProjectionScopeBroadFromFoundset),
	}
	request := RelationshipJoinPlan{Edges: []RelationshipJoinPlanEdge{edge}}.VectorRequest("lineitem")

	read := request.RelationshipVectorProjectionReads(map[string]RownumDomainSet{
		"l": {Domain: RownumDomain{Table: lineitem, Role: "l"}, Rownums: []QuantaRownum{10, 11}},
	})[0]

	if read.ProjectionScope != RelationshipVectorProjectionScopeBroadFromFoundset {
		t.Fatalf("projection scope = %q, want broad_from_foundset", read.ProjectionScope)
	}
	if read.CoveragePlan.ProjectionScope != RelationshipVectorProjectionScopeBroadFromFoundset ||
		read.CoveragePlan.ExpectStatus != RelationshipVectorProjectionCoverageComplete ||
		read.CoveragePlan.RecoveryPolicy != RelationshipVectorProjectionRecoveryBroadenAndIntersect ||
		!read.CoveragePlan.VerifyCoverage {
		t.Fatalf("coverage plan = %#v, want broad verified coverage policy", read.CoveragePlan)
	}
}

func TestRelationshipVectorProjectionReadsDescribeParentToChildExpansion(t *testing.T) {
	lineitem := TableInstance{Table: "lineitem", Alias: "l"}
	orders := TableInstance{Table: "orders", Alias: "o"}
	edge := RelationshipJoinPlanEdge{
		Left:          FieldRef{Table: lineitem, Name: "l_orderkey"},
		Right:         FieldRef{Table: orders, Name: "o_orderkey"},
		SQLKind:       JoinKindInner,
		ExecutionKind: RelationshipJoinExecutionVector,
		Intent:        RelationshipJoinOperationExpand,
		EncodingKind:  RelationshipEncodingVector,
	}
	request := RelationshipJoinPlan{Edges: []RelationshipJoinPlanEdge{edge}}.VectorRequest("orders")

	reads := request.RelationshipVectorProjectionReads(map[string]RownumDomainSet{
		"o": {Domain: RownumDomain{Table: orders, Role: "o"}, Rownums: []QuantaRownum{7}},
	})

	if len(reads) != 1 {
		t.Fatalf("reads = %#v, want one", reads)
	}
	read := reads[0]
	if read.Input.Domain.Name() != "o" || read.OutputDomain.Name() != "l" {
		t.Fatalf("domains = %s -> %s, want o -> l", read.Input.Domain.Name(), read.OutputDomain.Name())
	}
	if read.Translation.Direction != RownumDomainTranslationParentToChild {
		t.Fatalf("translation = %#v, want parent-to-child expansion", read.Translation)
	}
}

func TestRelationshipVectorProjectionResultCarriesTranslatedDomain(t *testing.T) {
	coverage := NewRelationshipVectorProjectionCoverage(2, 2, 2)
	result := RelationshipVectorProjectionResult{
		ID: "relationship_vector.1.l.l_orderkey.o.o_orderkey",
		Output: RownumDomainSet{
			Domain:  RownumDomain{Table: TableInstance{Table: "orders", Alias: "o"}, Role: "o"},
			Rownums: []QuantaRownum{100, 101},
		},
		Coverage: coverage,
		CacheHit: true,
	}

	if result.Output.Domain.Name() != "o" || result.Output.CandidateCount() != 2 || !result.CacheHit {
		t.Fatalf("result = %#v, want cached output domain with two rownums", result)
	}
	if !result.Coverage.Complete() || result.Coverage.RecoveryPolicy != RelationshipVectorProjectionRecoveryUseFoundset {
		t.Fatalf("coverage = %#v, want complete foundset coverage", result.Coverage)
	}
}

func TestRelationshipVectorProjectionCoverageClassifiesRecoveryPolicy(t *testing.T) {
	tests := []struct {
		name          string
		requestedRows int
		projectedRows int
		overlapRows   int
		wantStatus    RelationshipVectorProjectionCoverageStatus
		wantRecovery  RelationshipVectorProjectionRecoveryPolicy
		wantNeeds     bool
	}{
		{
			name:          "complete",
			requestedRows: 3,
			projectedRows: 3,
			overlapRows:   3,
			wantStatus:    RelationshipVectorProjectionCoverageComplete,
			wantRecovery:  RelationshipVectorProjectionRecoveryUseFoundset,
		},
		{
			name:          "partial",
			requestedRows: 3,
			projectedRows: 2,
			overlapRows:   2,
			wantStatus:    RelationshipVectorProjectionCoveragePartial,
			wantRecovery:  RelationshipVectorProjectionRecoveryBroadenAndIntersect,
			wantNeeds:     true,
		},
		{
			name:          "empty",
			requestedRows: 3,
			projectedRows: 0,
			overlapRows:   0,
			wantStatus:    RelationshipVectorProjectionCoverageEmpty,
			wantRecovery:  RelationshipVectorProjectionRecoveryBroadenAndIntersect,
			wantNeeds:     true,
		},
		{
			name:          "empty request",
			requestedRows: 0,
			projectedRows: 0,
			overlapRows:   0,
			wantStatus:    RelationshipVectorProjectionCoverageComplete,
			wantRecovery:  RelationshipVectorProjectionRecoveryUseFoundset,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coverage := NewRelationshipVectorProjectionCoverage(tt.requestedRows, tt.projectedRows, tt.overlapRows)
			if coverage.Status != tt.wantStatus || coverage.RecoveryPolicy != tt.wantRecovery || coverage.NeedsRecovery() != tt.wantNeeds {
				t.Fatalf("coverage = %#v, want status=%s recovery=%s needs=%v", coverage, tt.wantStatus, tt.wantRecovery, tt.wantNeeds)
			}
		})
	}
}

func TestRelationshipVectorProjectionKernelResultMergesOutputDomainSets(t *testing.T) {
	orders := RownumDomain{Table: TableInstance{Table: "orders", Alias: "o"}, Role: "o"}
	customer := RownumDomain{Table: TableInstance{Table: "customer", Alias: "c"}, Role: "c"}
	result := RelationshipVectorProjectionKernelResult{
		Results: []RelationshipVectorProjectionResult{
			{Output: RownumDomainSet{Domain: orders, Rownums: []QuantaRownum{10, 11}}},
			{Output: RownumDomainSet{Domain: orders, Rownums: []QuantaRownum{11, 12}}},
			{Output: RownumDomainSet{Domain: customer, Rownums: []QuantaRownum{7}}},
			{Output: RownumDomainSet{Rownums: []QuantaRownum{999}}},
		},
	}

	sets := result.OutputDomainSets()

	if len(sets) != 2 {
		t.Fatalf("sets = %#v, want two domain outputs", sets)
	}
	if !reflect.DeepEqual(sets["o"].Rownums, []QuantaRownum{10, 11, 12}) {
		t.Fatalf("orders rownums = %#v, want stable deduped merge", sets["o"].Rownums)
	}
	if !reflect.DeepEqual(sets["c"].Rownums, []QuantaRownum{7}) {
		t.Fatalf("customer rownums = %#v, want customer output", sets["c"].Rownums)
	}

	sets["o"].Rownums[0] = 99
	again := result.OutputDomainSets()
	if again["o"].Rownums[0] != 10 {
		t.Fatalf("OutputDomainSets leaked mutable rownums: %#v", again["o"].Rownums)
	}
}
