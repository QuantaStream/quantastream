package qsbridge

import (
	"math/big"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/searchindex"
)

func TestQuantaQueryFragmentCacheIdentityCanonicalizesBatchValues(t *testing.T) {
	boundary := QuantaFragmentCacheBoundary{
		LogicalShard:      "shard-1",
		Replica:           "node-a",
		ReplicaGeneration: "replica-gen-1",
		ShardEpoch:        "shard-epoch-1",
		IndexEpoch:        "index-epoch-1",
		CatalogVersion:    "catalog-v1",
		DictionaryVersion: "dictionary-v1",
		SnapshotID:        "snapshot-1",
	}
	first := QuantaQueryFragment{
		Index:     "lineitem",
		Field:     "l_shipmode",
		Operation: QuantaOperationIntersect,
		Values:    []*big.Int{big.NewInt(8), big.NewInt(7)},
	}
	second := first
	second.Values = []*big.Int{big.NewInt(7), big.NewInt(8)}

	firstIdentity := first.CacheIdentity(boundary)
	secondIdentity := second.CacheIdentity(boundary)
	if firstIdentity.Digest == "" {
		t.Fatalf("expected cache identity digest")
	}
	if firstIdentity.Digest != secondIdentity.Digest {
		t.Fatalf("digest changed for equivalent batch values: %q != %q", firstIdentity.Digest, secondIdentity.Digest)
	}
	if got := firstIdentity.Args[len(firstIdentity.Args)-2:]; got[0] != "values:7" || got[1] != "values:8" {
		t.Fatalf("args were not canonicalized: %#v", firstIdentity.Args)
	}
}

func TestQuantaQueryFragmentCacheIdentityChangesAcrossVersionBoundaries(t *testing.T) {
	fragment := QuantaQueryFragment{
		Index:     "orders",
		Field:     "o_orderkey",
		Operation: QuantaOperationIntersect,
		BSIOp:     QuantaBSIOpRange,
		Begin:     big.NewInt(10),
		End:       big.NewInt(20),
	}
	base := QuantaFragmentCacheBoundary{
		LogicalShard:      "shard-1",
		Replica:           "node-a",
		ReplicaGeneration: "replica-gen-1",
		ShardEpoch:        "shard-epoch-1",
		IndexEpoch:        "index-epoch-1",
		CatalogVersion:    "catalog-v1",
		SnapshotID:        "snapshot-1",
	}
	baseDigest := fragment.CacheIdentity(base).Digest

	changedReplicaGeneration := base
	changedReplicaGeneration.ReplicaGeneration = "replica-gen-2"
	changedIndexEpoch := base
	changedIndexEpoch.IndexEpoch = "index-epoch-2"
	changedCatalogVersion := base
	changedCatalogVersion.CatalogVersion = "catalog-v2"

	for name, boundary := range map[string]QuantaFragmentCacheBoundary{
		"replica_generation": changedReplicaGeneration,
		"index_epoch":        changedIndexEpoch,
		"catalog_version":    changedCatalogVersion,
	} {
		if got := fragment.CacheIdentity(boundary).Digest; got == baseDigest {
			t.Fatalf("%s digest = base digest %q, want distinct", name, got)
		}
	}
}

func TestQuantaIntermediateLowererLowersLegacyBoolPredicate(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_confirmed = true"},
		ExecutionOptions{},
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1: %#v", len(intermediate.Fragments), intermediate.Fragments)
	}
	fragment := intermediate.Fragments[0]
	if fragment.Field != "o_confirmed" || len(fragment.Values) != 1 {
		t.Fatalf("fragment = %#v, want o_confirmed bitmap value", fragment)
	}
	if fragment.Values[0] == nil || fragment.Values[0].Int64() != 1 {
		t.Fatalf("bool true encoded value = %v, want legacy true row id 1", fragment.Values[0])
	}
}

func TestQuantaIntermediateLowererNormalizesFractionalThresholdsForIntegerBSI(t *testing.T) {
	service := simpleRunnerPlanningService()
	tests := []struct {
		name  string
		sql   string
		op    QuantaBSIOp
		value int64
	}{
		{
			name:  "greater_than_floor",
			sql:   "select o_orderkey from orders where o_orderkey > 7.5",
			op:    QuantaBSIOpGT,
			value: 7,
		},
		{
			name:  "greater_equal_ceil",
			sql:   "select o_orderkey from orders where o_orderkey >= 7.5",
			op:    QuantaBSIOpGE,
			value: 8,
		},
		{
			name:  "less_than_ceil",
			sql:   "select o_orderkey from orders where o_orderkey < 7.5",
			op:    QuantaBSIOpLT,
			value: 8,
		},
		{
			name:  "less_equal_floor",
			sql:   "select o_orderkey from orders where o_orderkey <= 7.5",
			op:    QuantaBSIOpLE,
			value: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, request := service.PrepareExecutionRequest(PlanRequest{SQL: tt.sql}, ExecutionOptions{})

			intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
			if diagnostics.BlocksNative() {
				t.Fatalf("lower diagnostics: %#v", diagnostics)
			}
			if len(intermediate.Fragments) != 1 {
				t.Fatalf("fragments = %d, want 1: %#v", len(intermediate.Fragments), intermediate.Fragments)
			}
			fragment := intermediate.Fragments[0]
			if fragment.Field != "o_orderkey" || fragment.BSIOp != tt.op {
				t.Fatalf("fragment = %#v, want o_orderkey %s", fragment, tt.op)
			}
			if fragment.Value == nil || fragment.Value.Int64() != tt.value {
				t.Fatalf("fragment value = %v, want %d", fragment.Value, tt.value)
			}
		})
	}
}

func TestQuantaIntermediateLowererLowersMutationPredicates(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "update orders set o_discount = 1 where o_orderkey = 8"},
		ExecutionOptions{},
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1: %#v", len(intermediate.Fragments), intermediate.Fragments)
	}
	fragment := intermediate.Fragments[0]
	if fragment.Index != "orders" || fragment.Field != "o_orderkey" || fragment.BSIOp != QuantaBSIOpEQ {
		t.Fatalf("fragment = %#v, want orders.o_orderkey EQ", fragment)
	}
	if fragment.Value == nil || fragment.Value.Int64() != 8 {
		t.Fatalf("fragment value = %v, want 8", fragment.Value)
	}
}

func TestQuantaIntermediateLowererLowersMaterializedTextSearch(t *testing.T) {
	field := FieldRef{
		Table: TableInstance{Schema: "quanta", Table: "customer", Alias: "c"},
		Name:  "c_name",
		Type:  DataTypeString,
		Index: IndexBSI,
		Encoding: LegacyEncodingProfile("StringLexBSI", LegacyEncodingOptions{
			Searchable:   true,
			PrefixLength: 10,
			MaxLength:    10,
		}),
	}
	hashes := []*big.Int{big.NewInt(7), new(big.Int).SetUint64(1 << 63)}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{field.Table},
		Predicates: []Predicate{{
			Expr:      TextSearch(field, Literal(ValueString, "Customer"), "").WithHashes(hashes),
			Placement: PredicatePushdown,
			Scope:     PredicateScopeWhere,
		}},
		Result: ResultShape{Kind: ResultQuery},
	}

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerQuery(query, ParameterBindingSet{})
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one text-search fragment", intermediate.Fragments)
	}
	fragment := intermediate.Fragments[0]
	if fragment.Index != "customer" || fragment.Role != "c" {
		t.Fatalf("fragment target = index %q role %q, want customer/c", fragment.Index, fragment.Role)
	}
	if got, want := fragment.Field, searchindex.HashFieldName("c_name"); got != want {
		t.Fatalf("fragment field = %q, want %q", got, want)
	}
	if fragment.Operation != QuantaOperationIntersect || fragment.BSIOp != QuantaBSIOpBatchEQ {
		t.Fatalf("fragment op = %s/%s, want INTERSECT/BATCH_EQ", fragment.Operation, fragment.BSIOp)
	}
	if len(fragment.Values) != 2 || fragment.Values[0].Cmp(hashes[0]) != 0 || fragment.Values[1].Cmp(hashes[1]) != 0 {
		t.Fatalf("fragment values = %#v, want cloned materialized hashes", fragment.Values)
	}
	if &fragment.Values[0] == &hashes[0] || fragment.Values[0] == hashes[0] {
		t.Fatalf("fragment values should be cloned")
	}
}

func TestQuantaIntermediateLowererCarriesProjectionFields(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o.o_orderkey as order_id, o.o_totalprice as total_price from orders as o where o.o_totalprice >= ? order by o.o_totalprice desc limit 2"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueFloat, float64(101)),
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	if len(intermediate.ProjectionFields) != 2 {
		t.Fatalf("projection fields = %d, want 2: %#v", len(intermediate.ProjectionFields), intermediate.ProjectionFields)
	}
	first := intermediate.ProjectionFields[0]
	if first.Index != "orders" || first.Field != "o_totalprice" || first.Type != DataTypeFloat {
		t.Fatalf("first projection field = %#v, want orders.o_totalprice float", first)
	}
	if !first.Visible {
		t.Fatalf("first projection field should be visible: %#v", first)
	}
	second := intermediate.ProjectionFields[1]
	if second.Index != "orders" || second.Field != "o_orderkey" || second.Type != DataTypeInt {
		t.Fatalf("second projection field = %#v, want orders.o_orderkey int", second)
	}
	if !second.Visible {
		t.Fatalf("second projection field should be visible: %#v", second)
	}
}

func TestQuantaIntermediateLowererCoalescesExclusiveDateWindowBounds(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderdate >= '1996-01-01' and o_orderdate < '1996-04-01'"},
		ExecutionOptions{},
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want one coalesced datetime range: %#v", len(intermediate.Fragments), intermediate.Fragments)
	}
	wantBegin := big.NewInt(time.Date(1996, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	wantEnd := big.NewInt(time.Date(1996, 4, 1, 0, 0, 0, 0, time.UTC).UnixMilli() - 1)
	fragment := intermediate.Fragments[0]
	if fragment.BSIOp != QuantaBSIOpRange {
		t.Fatalf("bsi op = %s, want RANGE", fragment.BSIOp)
	}
	if fragment.Begin.Cmp(wantBegin) != 0 {
		t.Fatalf("range begin = %#v, want %s", fragment.Begin, wantBegin)
	}
	if fragment.End.Cmp(wantEnd) != 0 {
		t.Fatalf("range end = %#v, want inclusive end %s", fragment.End, wantEnd)
	}
}

func TestQuantaIntermediateLowererBuildsNestedFilterForMixedBooleanWhere(t *testing.T) {
	statement, parseDiagnostics := SimpleParserBridge{}.Parse(
		"select o_orderkey from orders where (o_orderkey = 7 and o_custkey = 501) or (o_orderkey = 8 and o_custkey = 502)",
	)
	if parseDiagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", parseDiagnostics)
	}
	query, bindDiagnostics := statement.Bind(NewBindContext(testBindCatalog(), "quanta"))
	if bindDiagnostics.BlocksNative() {
		t.Fatalf("bind diagnostics: %#v", bindDiagnostics)
	}
	if query.WhereExpr == nil {
		t.Fatalf("expected bound where expression")
	}
	if len(query.Blockers) != 1 || query.Blockers[0].Code != DiagnosticMixedBooleanPredicate {
		t.Fatalf("blockers = %#v, want one mixed boolean blocker", query.Blockers)
	}

	intermediate, lowerDiagnostics := QuantaIntermediateLowerer{}.LowerQuery(query, ParameterBindingSet{})
	if lowerDiagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", lowerDiagnostics)
	}
	filter := intermediate.Filter
	if filter.Operation != QuantaFilterUnion {
		t.Fatalf("filter operation = %s, want %s: %#v", filter.Operation, QuantaFilterUnion, filter)
	}
	if len(filter.Children) != 2 {
		t.Fatalf("filter children = %d, want 2: %#v", len(filter.Children), filter.Children)
	}
	for branchIndex, branch := range filter.Children {
		if branch.Operation != QuantaFilterIntersect {
			t.Fatalf("branch %d operation = %s, want %s: %#v", branchIndex, branch.Operation, QuantaFilterIntersect, branch)
		}
		if len(branch.Children) != 2 {
			t.Fatalf("branch %d children = %d, want 2: %#v", branchIndex, len(branch.Children), branch.Children)
		}
		for leafIndex, leaf := range branch.Children {
			if !leaf.Leaf() {
				t.Fatalf("branch %d leaf %d operation = %s, want leaf: %#v", branchIndex, leafIndex, leaf.Operation, leaf)
			}
			if leaf.Fragment.Operation != QuantaOperationIntersect {
				t.Fatalf("branch %d leaf %d fragment operation = %s, want %s: %#v", branchIndex, leafIndex, leaf.Fragment.Operation, QuantaOperationIntersect, leaf.Fragment)
			}
			if leaf.Fragment.Index != "orders" {
				t.Fatalf("branch %d leaf %d fragment index = %q, want orders", branchIndex, leafIndex, leaf.Fragment.Index)
			}
		}
	}
}

func TestQuantaIntermediateLowererCoalescesGroupedFilterRangePairs(t *testing.T) {
	filter := quantaIntermediateCoalesceFilterRanges(QuantaFilterExpression{
		Operation: QuantaFilterUnion,
		Children: []QuantaFilterExpression{
			{
				Operation: QuantaFilterIntersect,
				Children: []QuantaFilterExpression{
					testQuantaRangeBoundLeaf("part", "p_size", QuantaBSIOpGE, 1),
					testQuantaRangeBoundLeaf("lineitem", "l_quantity", QuantaBSIOpGE, 1),
					testQuantaRangeBoundLeaf("lineitem", "l_quantity", QuantaBSIOpLE, 11),
					testQuantaRangeBoundLeaf("part", "p_size", QuantaBSIOpLE, 5),
				},
			},
			{
				Operation: QuantaFilterIntersect,
				Children: []QuantaFilterExpression{
					testQuantaRangeBoundLeaf("part", "p_size", QuantaBSIOpGE, 1),
					testQuantaRangeBoundLeaf("lineitem", "l_quantity", QuantaBSIOpGE, 10),
					testQuantaRangeBoundLeaf("lineitem", "l_quantity", QuantaBSIOpLE, 20),
					testQuantaRangeBoundLeaf("part", "p_size", QuantaBSIOpLE, 10),
				},
			},
		},
	})
	if filter.Operation != QuantaFilterUnion || len(filter.Children) != 2 {
		t.Fatalf("filter = %#v, want two-branch union", filter)
	}
	for i, branch := range filter.Children {
		conjuncts := filterDomainConjunctsForFactoring(branch)
		rangeFields := map[string]bool{}
		for _, conjunct := range conjuncts {
			if conjunct.Leaf() && conjunct.Fragment.BSIOp == QuantaBSIOpRange {
				rangeFields[conjunct.Fragment.Index+"."+conjunct.Fragment.Field] = true
			}
		}
		if !rangeFields["lineitem.l_quantity"] {
			t.Fatalf("branch %d missing coalesced l_quantity range: %#v", i, branch)
		}
		if !rangeFields["part.p_size"] {
			t.Fatalf("branch %d missing coalesced p_size range: %#v", i, branch)
		}
		for _, conjunct := range conjuncts {
			if !conjunct.Leaf() {
				continue
			}
			if (conjunct.Fragment.Field == "l_quantity" || conjunct.Fragment.Field == "p_size") && conjunct.Fragment.BSIOp != QuantaBSIOpRange {
				t.Fatalf("branch %d has uncoalesced range bound: %#v", i, conjunct.Fragment)
			}
		}
	}
}

func testQuantaRangeBoundLeaf(index, field string, op QuantaBSIOp, value int64) QuantaFilterExpression {
	return QuantaFilterExpression{
		Operation: QuantaFilterLeaf,
		Fragment: QuantaQueryFragment{
			Index:                index,
			Field:                field,
			Operation:            QuantaOperationIntersect,
			BSIOp:                op,
			Value:                big.NewInt(value),
			HasLiteral:           true,
			Literal:              Literal(ValueInt, value),
			RangeCoalesceAllowed: true,
		},
	}
}

func TestQuantaFilterExpressionDomainSummary(t *testing.T) {
	filter := QuantaFilterExpression{
		Operation: QuantaFilterUnion,
		Children: []QuantaFilterExpression{
			{
				Operation: QuantaFilterIntersect,
				Children: []QuantaFilterExpression{
					{Operation: QuantaFilterLeaf, Fragment: QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
					{Operation: QuantaFilterLeaf, Fragment: QuantaQueryFragment{Index: "part", Field: "p_brand"}},
				},
			},
			{Operation: QuantaFilterLeaf, Fragment: QuantaQueryFragment{Index: "lineitem", Field: "l_shipmode"}},
		},
	}

	summary := filter.DomainSummary()
	if !summary.Mixed() {
		t.Fatalf("summary = %#v, want mixed domains", summary)
	}
	if len(summary.Domains) != 2 || summary.Domains[0] != "lineitem" || summary.Domains[1] != "part" {
		t.Fatalf("domains = %#v, want sorted lineitem/part", summary.Domains)
	}
	if domain, ok := summary.Single(); ok || domain != "" {
		t.Fatalf("single = %q, %t, want none", domain, ok)
	}
	requirement := summary.TranslationRequirement("lineitem")
	if !requirement.Required {
		t.Fatalf("requirement = %#v, want translation required", requirement)
	}
	if requirement.TargetDomain != "lineitem" {
		t.Fatalf("target domain = %q, want lineitem", requirement.TargetDomain)
	}
	if len(requirement.SourceDomains) != 2 || requirement.SourceDomains[0] != "lineitem" || requirement.SourceDomains[1] != "part" {
		t.Fatalf("source domains = %#v, want sorted lineitem/part", requirement.SourceDomains)
	}
	if len(requirement.Strategies) != 1 || requirement.Strategies[0] != PhysicalStrategyRelationshipVectorNormalization {
		t.Fatalf("strategies = %#v, want relationship vector normalization", requirement.Strategies)
	}
	plan := requirement.NormalizationPlan(FilterDomainNormalizeGroupedFilter)
	if !plan.Required() {
		t.Fatalf("normalization plan = %#v, want required", plan)
	}
	if plan.Operation != FilterDomainNormalizeGroupedFilter {
		t.Fatalf("operation = %q, want grouped_filter", plan.Operation)
	}
	if len(plan.Requests) != 1 {
		t.Fatalf("requests = %#v, want one source-to-target request", plan.Requests)
	}
	request := plan.Requests[0]
	if request.SourceDomain != "part" || request.TargetDomain != "lineitem" {
		t.Fatalf("request domains = %s -> %s, want part -> lineitem", request.SourceDomain, request.TargetDomain)
	}
	if request.Source.Index != "part" {
		t.Fatalf("source candidate index = %q, want part", request.Source.Index)
	}
	if request.Strategy != PhysicalStrategyRelationshipVectorNormalization {
		t.Fatalf("request strategy = %q, want relationship vector normalization", request.Strategy)
	}
	if _, ok := request.RelationshipVectorRequest(QuantaQueryFragment{Index: "part", Field: "p_brand"}, QuantaCandidateSet{Index: "part"}); ok {
		t.Fatalf("relationship vector request should not be derivable without a relationship path")
	}
	if len(request.RelationshipPath) != 0 {
		t.Fatalf("relationship path = %#v, want empty without relationship metadata", request.RelationshipPath)
	}
	relationshipPlan := PlanRelationshipJoins([]JoinEdge{{
		Left: FieldRef{
			Table: TableInstance{Table: "lineitem", Alias: "l"},
			Name:  "l_partkey",
		},
		Right: FieldRef{
			Table: TableInstance{Table: "part", Alias: "p"},
			Name:  "p_partkey",
		},
		Kind:     JoinKindInner,
		Encoding: RelationshipEncodingProfile{Kind: RelationshipEncodingVector},
	}})
	plan = requirement.NormalizationPlan(FilterDomainNormalizeGroupedFilter, relationshipPlan)
	request = plan.Requests[0]
	if len(request.RelationshipPath) != 1 {
		t.Fatalf("relationship path = %#v, want one-hop path", request.RelationshipPath)
	}
	if request.RelationshipPath[0].ExecutionKind != RelationshipJoinExecutionVector {
		t.Fatalf("path execution kind = %q, want vector", request.RelationshipPath[0].ExecutionKind)
	}
	vectorRequest, ok := request.RelationshipVectorRequest(
		QuantaQueryFragment{Index: "part", Field: "p_brand"},
		QuantaCandidateSet{Rownums: []QuantaRownum{7, 8}},
	)
	if !ok {
		t.Fatalf("relationship vector request not derived from one-hop path")
	}
	if vectorRequest.SourceDomain != "part" || vectorRequest.TargetDomain != "lineitem" {
		t.Fatalf("vector request domains = %s -> %s, want part -> lineitem", vectorRequest.SourceDomain, vectorRequest.TargetDomain)
	}
	if vectorRequest.SourceCandidates.Index != "part" {
		t.Fatalf("source candidates index = %q, want part", vectorRequest.SourceCandidates.Index)
	}
	if vectorRequest.Direction != FilterDomainRelationshipVectorDirectionRightToLeft {
		t.Fatalf("direction = %q, want right_to_left", vectorRequest.Direction)
	}
	if vectorRequest.LeafName() != "part.p_brand" {
		t.Fatalf("leaf name = %q, want part.p_brand", vectorRequest.LeafName())
	}
	if vectorRequest.Edge.ExecutionKind != RelationshipJoinExecutionVector {
		t.Fatalf("edge execution kind = %q, want vector", vectorRequest.Edge.ExecutionKind)
	}
	vectorResult := FilterDomainRelationshipVectorResult{
		Request:          vectorRequest,
		TargetCandidates: QuantaCandidateSet{Index: "lineitem", Rownums: []QuantaRownum{10, 11}},
	}
	if vectorResult.TargetCandidates.Index != "lineitem" || len(vectorResult.TargetCandidates.Rownums) != 2 {
		t.Fatalf("vector result = %#v, want lineitem candidates", vectorResult)
	}
}

func TestQuantaFilterExpressionDomainSummarySingleDomain(t *testing.T) {
	filter := QuantaFilterExpression{
		Operation: QuantaFilterIntersect,
		Children: []QuantaFilterExpression{
			{Operation: QuantaFilterLeaf, Fragment: QuantaQueryFragment{Index: "orders", Field: "o_orderkey"}},
			{Operation: QuantaFilterLeaf, Fragment: QuantaQueryFragment{Index: "orders", Field: "o_custkey"}},
		},
	}

	summary := filter.DomainSummary()
	if summary.Mixed() {
		t.Fatalf("summary = %#v, want single domain", summary)
	}
	if domain, ok := summary.Single(); !ok || domain != "orders" {
		t.Fatalf("single = %q, %t, want orders", domain, ok)
	}
	requirement := summary.TranslationRequirement("orders")
	if requirement.Required {
		t.Fatalf("requirement = %#v, want no translation required", requirement)
	}
	if requirement.TargetDomain != "orders" {
		t.Fatalf("target domain = %q, want orders", requirement.TargetDomain)
	}
	if len(requirement.Strategies) != 0 {
		t.Fatalf("strategies = %#v, want none", requirement.Strategies)
	}
}

func TestFilterDomainRewriteResultAppliesCandidateSetLeaves(t *testing.T) {
	source := QuantaQueryFragment{Index: "part", Field: "p_brand", Operation: QuantaOperationIntersect}
	filter := QuantaFilterExpression{
		Operation: QuantaFilterIntersect,
		Children: []QuantaFilterExpression{
			{Operation: QuantaFilterLeaf, Fragment: source},
			{Operation: QuantaFilterLeaf, Fragment: QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
		},
	}
	rewrite := FilterDomainRewriteResult{
		TargetDomain: "lineitem",
		Leaves: []FilterDomainNormalizedLeaf{{
			OriginalFragment: source,
			SourceDomain:     "part",
			TargetDomain:     "lineitem",
			CandidateSet:     QuantaCandidateSet{Index: "lineitem", Rownums: []QuantaRownum{4, 7}},
		}},
	}

	rewritten := rewrite.Apply(filter)
	if len(rewritten.Children) != 2 {
		t.Fatalf("children = %#v, want two", rewritten.Children)
	}
	if !rewritten.Children[0].CandidateSetLeaf() {
		t.Fatalf("first child = %#v, want candidate-set leaf", rewritten.Children[0])
	}
	if rewritten.Children[0].CandidateSet.Index != "lineitem" {
		t.Fatalf("candidate index = %q, want lineitem", rewritten.Children[0].CandidateSet.Index)
	}
	if !rewritten.Children[1].Leaf() {
		t.Fatalf("second child = %#v, want original leaf", rewritten.Children[1])
	}
	summary := rewritten.DomainSummary()
	if summary.Mixed() {
		t.Fatalf("summary = %#v, want single target domain", summary)
	}
	if domain, ok := summary.Single(); !ok || domain != "lineitem" {
		t.Fatalf("domain = %q, %t, want lineitem", domain, ok)
	}
}

func TestFilterDomainRewriteResultMatchesRepeatedFieldLeavesByPredicatePayload(t *testing.T) {
	brand45 := QuantaQueryFragment{
		Index:      "part",
		Field:      "p_brand",
		Operation:  QuantaOperationIntersect,
		Value:      big.NewInt(45),
		HasLiteral: true,
		Literal:    LiteralExpr{Kind: ValueString, Value: "Brand#45"},
	}
	brand34 := QuantaQueryFragment{
		Index:      "part",
		Field:      "p_brand",
		Operation:  QuantaOperationIntersect,
		Value:      big.NewInt(34),
		HasLiteral: true,
		Literal:    LiteralExpr{Kind: ValueString, Value: "Brand#34"},
	}
	filter := QuantaFilterExpression{
		Operation: QuantaFilterUnion,
		Children: []QuantaFilterExpression{
			{Operation: QuantaFilterLeaf, Fragment: brand45},
			{Operation: QuantaFilterLeaf, Fragment: brand34},
		},
	}
	rewrite := FilterDomainRewriteResult{
		TargetDomain: "lineitem",
		Leaves: []FilterDomainNormalizedLeaf{
			{
				OriginalFragment: brand45,
				TargetDomain:     "lineitem",
				CandidateSet:     QuantaCandidateSet{Index: "lineitem", Rownums: []QuantaRownum{45}},
			},
			{
				OriginalFragment: brand34,
				TargetDomain:     "lineitem",
				CandidateSet:     QuantaCandidateSet{Index: "lineitem", Rownums: []QuantaRownum{34}},
			},
		},
	}

	rewritten := rewrite.Apply(filter)
	if len(rewritten.Children) != 2 {
		t.Fatalf("children = %#v, want two", rewritten.Children)
	}
	first := rewritten.Children[0].CandidateSet.Rownums
	second := rewritten.Children[1].CandidateSet.Rownums
	if len(first) != 1 || first[0] != 45 {
		t.Fatalf("first candidate rows = %#v, want [45]", first)
	}
	if len(second) != 1 || second[0] != 34 {
		t.Fatalf("second candidate rows = %#v, want [34]", second)
	}
}

func TestFilterDomainRewriteResultFactorsCommonUnionConjuncts(t *testing.T) {
	brand12 := QuantaQueryFragment{Index: "part", Field: "p_brand", HasLiteral: true, Literal: LiteralExpr{Kind: ValueString, Value: "Brand#12"}}
	brand23 := QuantaQueryFragment{Index: "part", Field: "p_brand", HasLiteral: true, Literal: LiteralExpr{Kind: ValueString, Value: "Brand#23"}}
	shipmode := QuantaQueryFragment{Index: "lineitem", Field: "l_shipmode", HasLiteral: true, Literal: LiteralExpr{Kind: ValueString, Value: "AIR"}}
	shipinstruct := QuantaQueryFragment{Index: "lineitem", Field: "l_shipinstruct", HasLiteral: true, Literal: LiteralExpr{Kind: ValueString, Value: "DELIVER IN PERSON"}}
	quantityLow := QuantaQueryFragment{Index: "lineitem", Field: "l_quantity", BSIOp: QuantaBSIOpLE, HasLiteral: true, Literal: LiteralExpr{Kind: ValueInt, Value: int64(11)}}
	quantityHigh := QuantaQueryFragment{Index: "lineitem", Field: "l_quantity", BSIOp: QuantaBSIOpLE, HasLiteral: true, Literal: LiteralExpr{Kind: ValueInt, Value: int64(20)}}
	filter := QuantaFilterExpression{
		Operation: QuantaFilterUnion,
		Children: []QuantaFilterExpression{
			{
				Operation: QuantaFilterIntersect,
				Children: []QuantaFilterExpression{
					{Operation: QuantaFilterLeaf, Fragment: brand12},
					{Operation: QuantaFilterLeaf, Fragment: quantityLow},
					{Operation: QuantaFilterLeaf, Fragment: shipmode},
					{Operation: QuantaFilterLeaf, Fragment: shipinstruct},
				},
			},
			{
				Operation: QuantaFilterIntersect,
				Children: []QuantaFilterExpression{
					{Operation: QuantaFilterLeaf, Fragment: brand23},
					{Operation: QuantaFilterLeaf, Fragment: quantityHigh},
					{Operation: QuantaFilterLeaf, Fragment: shipmode},
					{Operation: QuantaFilterLeaf, Fragment: shipinstruct},
				},
			},
		},
	}
	rewrite := FilterDomainRewriteResult{
		TargetDomain: "lineitem",
		Leaves: []FilterDomainNormalizedLeaf{
			{
				OriginalFragment: brand12,
				TargetDomain:     "lineitem",
				CandidateSet:     QuantaCandidateSet{Index: "lineitem", Rownums: []QuantaRownum{12}},
			},
			{
				OriginalFragment: brand23,
				TargetDomain:     "lineitem",
				CandidateSet:     QuantaCandidateSet{Index: "lineitem", Rownums: []QuantaRownum{23}},
			},
		},
	}

	rewritten := rewrite.Apply(filter)
	if rewritten.Operation != QuantaFilterIntersect {
		t.Fatalf("operation = %s, want intersect: %#v", rewritten.Operation, rewritten)
	}
	if len(rewritten.Children) != 3 {
		t.Fatalf("children = %#v, want two factored leaves plus union", rewritten.Children)
	}
	if !filterDomainExpressionMatches(rewritten.Children[0], QuantaFilterExpression{Operation: QuantaFilterLeaf, Fragment: shipmode}) {
		t.Fatalf("first child = %#v, want shipmode leaf", rewritten.Children[0])
	}
	if !filterDomainExpressionMatches(rewritten.Children[1], QuantaFilterExpression{Operation: QuantaFilterLeaf, Fragment: shipinstruct}) {
		t.Fatalf("second child = %#v, want shipinstruct leaf", rewritten.Children[1])
	}
	union := rewritten.Children[2]
	if union.Operation != QuantaFilterUnion || len(union.Children) != 2 {
		t.Fatalf("factored child = %#v, want two-branch union", union)
	}
	for i, child := range union.Children {
		if child.Operation != QuantaFilterIntersect || len(child.Children) != 2 {
			t.Fatalf("union child %d = %#v, want candidate plus branch-specific leaf", i, child)
		}
		if !child.Children[0].CandidateSetLeaf() {
			t.Fatalf("union child %d first expression = %#v, want candidate leaf", i, child.Children[0])
		}
		if !child.Children[1].Leaf() || child.Children[1].Fragment.Field != "l_quantity" {
			t.Fatalf("union child %d second expression = %#v, want quantity leaf", i, child.Children[1])
		}
	}
}

func TestQuantaAggregateRequestsFromPhysicalNodeDerivesTopNProjectorRank(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}

	result := planner.Plan("select topn(l.l_shipmode) as shipmode_topn from lineitem as l")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("plan diagnostics: %#v", result.Diagnostics)
	}
	aggregate, ok := findPhysicalAggregateNode(result.Physical.Root)
	if !ok {
		t.Fatalf("expected physical aggregate node")
	}

	requests, diagnostics := QuantaAggregateRequestsFromPhysicalNode(aggregate)
	if diagnostics.BlocksNative() {
		t.Fatalf("aggregate request diagnostics: %#v", diagnostics)
	}
	if len(requests) != 1 {
		t.Fatalf("aggregate requests = %d, want 1: %#v", len(requests), requests)
	}
	request := requests[0]
	if request.Operation != QuantaAggregateProjectorRank {
		t.Fatalf("operation = %s, want %s", request.Operation, QuantaAggregateProjectorRank)
	}
	if got := request.RuntimeTarget(); got != QuantaRuntimeTargetProjectorRank {
		t.Fatalf("runtime target = %q, want %q", got, QuantaRuntimeTargetProjectorRank)
	}
	if request.Function != "topn" || request.Alias != "shipmode_topn" {
		t.Fatalf("request function/alias = %q/%q, want topn/shipmode_topn", request.Function, request.Alias)
	}
	if request.Input.Index != "lineitem" || request.Input.Field != "l_shipmode" || request.Input.Type != DataTypeString {
		t.Fatalf("input = %#v, want lineitem.l_shipmode string", request.Input)
	}
	if !request.Input.Visible {
		t.Fatalf("topn input should be visible in the aggregate request: %#v", request.Input)
	}
	if !physicalStrategiesContain(request.Strategies, PhysicalStrategyQuantaTopN) {
		t.Fatalf("strategies = %#v, want %s", request.Strategies, PhysicalStrategyQuantaTopN)
	}
}

func TestQuantaProjectedRowSetValidatesColumnarShape(t *testing.T) {
	rowSet := QuantaProjectedRowSet{
		Index:        "orders",
		LogicalShard: "orders:2023-06",
		Replica:      "node-a",
		Rownums:      []QuantaRownum{1001, 1002},
		ProjectionVectors: []QuantaProjectionVector{
			{
				Field: QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: DataTypeInt, Visible: true},
				Values: []ResultCell{
					{Kind: ValueInt, Value: int64(1001)},
					{Kind: ValueInt, Value: int64(1002)},
				},
			},
			{
				Field: QuantaProjectionField{Index: "orders", Field: "o_orderstatus", Type: DataTypeString, Visible: true},
				Values: []ResultCell{
					{Kind: ValueString, Value: "O"},
					{Kind: ValueString, Value: "F"},
				},
			},
		},
	}

	if got := rowSet.CandidateCount(); got != 2 {
		t.Fatalf("candidate count = %d, want 2", got)
	}
	if got := rowSet.ProjectionCount(); got != 2 {
		t.Fatalf("projection count = %d, want 2", got)
	}
	if diagnostics := rowSet.ValidateShape(); diagnostics.BlocksNative() {
		t.Fatalf("valid row set diagnostics: %#v", diagnostics)
	}
}

func TestQuantaProjectedRowSetRejectsMismatchedVectorShape(t *testing.T) {
	rowSet := QuantaProjectedRowSet{
		Index:   "orders",
		Rownums: []QuantaRownum{1001, 1002},
		ProjectionVectors: []QuantaProjectionVector{
			{
				Field: QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: DataTypeInt, Visible: true},
				Values: []ResultCell{
					{Kind: ValueInt, Value: int64(1001)},
				},
			},
		},
	}

	diagnostics := rowSet.ValidateShape()
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected shape diagnostic, got %#v", diagnostics)
	}
	if diagnostics[0].Code != DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %s, want %s", diagnostics[0].Code, DiagnosticInternalInvariant)
	}
}

func TestQuantaProjectedRowSetToResultChunkZipsVisibleVectors(t *testing.T) {
	rowSet := QuantaProjectedRowSet{
		Index:   "orders",
		Rownums: []QuantaRownum{1001, 1002},
		ProjectionVectors: []QuantaProjectionVector{
			{
				Field: QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: DataTypeInt, Visible: true},
				Values: []ResultCell{
					{Kind: ValueInt, Value: int64(1001)},
					{Kind: ValueInt, Value: int64(1002)},
				},
			},
			{
				Field: QuantaProjectionField{Index: "orders", Field: "o_totalprice", Type: DataTypeFloat, Visible: false},
				Values: []ResultCell{
					{Kind: ValueFloat, Value: float64(10.5)},
					{Kind: ValueFloat, Value: float64(20.5)},
				},
			},
			{
				Field: QuantaProjectionField{Index: "orders", Field: "o_orderstatus", Type: DataTypeString, Visible: true},
				Values: []ResultCell{
					{Kind: ValueString, Value: "O"},
					{Kind: ValueString, Value: "F"},
				},
			},
		},
	}

	chunk, diagnostics := rowSet.ToResultChunk(3, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics: %#v", diagnostics)
	}
	if chunk.Sequence != 3 || !chunk.Final {
		t.Fatalf("chunk metadata = sequence %d final %v, want 3 true", chunk.Sequence, chunk.Final)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(chunk.Rows))
	}
	if len(chunk.Rows[0]) != 2 || len(chunk.Rows[1]) != 2 {
		t.Fatalf("visible columns per row = %d,%d, want 2,2", len(chunk.Rows[0]), len(chunk.Rows[1]))
	}
	if got := chunk.Rows[0][0].Value; got != int64(1001) {
		t.Fatalf("row 0 column 0 = %v, want 1001", got)
	}
	if got := chunk.Rows[0][1].Value; got != "O" {
		t.Fatalf("row 0 column 1 = %v, want O", got)
	}
	if got := chunk.Rows[1][0].Value; got != int64(1002) {
		t.Fatalf("row 1 column 0 = %v, want 1002", got)
	}
	if got := chunk.Rows[1][1].Value; got != "F" {
		t.Fatalf("row 1 column 1 = %v, want F", got)
	}
}

func TestQuantaProjectedRowSetToResultChunkRejectsInvalidShape(t *testing.T) {
	rowSet := QuantaProjectedRowSet{
		Index:   "orders",
		Rownums: []QuantaRownum{1001, 1002},
		ProjectionVectors: []QuantaProjectionVector{
			{
				Field: QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: DataTypeInt, Visible: true},
				Values: []ResultCell{
					{Kind: ValueInt, Value: int64(1001)},
				},
			},
		},
	}

	chunk, diagnostics := rowSet.ToResultChunk(1, false)
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected shape diagnostic, got %#v", diagnostics)
	}
	if len(chunk.Rows) != 0 {
		t.Fatalf("chunk rows = %d, want 0 on invalid shape", len(chunk.Rows))
	}
}

func TestQuantaRownumBitmapColumnIDConversion(t *testing.T) {
	rownum := QuantaRownum(42)
	columnID := rownum.BitmapColumnID()
	if columnID != QuantaBitmapColumnID(42) {
		t.Fatalf("bitmap column id = %d, want 42", columnID)
	}
	if got := QuantaRownumFromBitmapColumnID(columnID); got != rownum {
		t.Fatalf("rownum = %d, want %d", got, rownum)
	}
}

func TestQuantaRownumBitmapColumnIDSlicesAreCopied(t *testing.T) {
	rownums := []QuantaRownum{7, 9}
	columnIDs := BitmapColumnIDsFromRownums(rownums)
	if len(columnIDs) != 2 || columnIDs[0] != 7 || columnIDs[1] != 9 {
		t.Fatalf("bitmap column ids = %#v, want 7,9", columnIDs)
	}
	columnIDs[0] = 100
	if rownums[0] != 7 {
		t.Fatalf("source rownums mutated: %#v", rownums)
	}

	roundTrip := RownumsFromBitmapColumnIDs(columnIDs)
	if len(roundTrip) != 2 || roundTrip[0] != 100 || roundTrip[1] != 9 {
		t.Fatalf("round trip rownums = %#v, want 100,9", roundTrip)
	}
	columnIDs[1] = 200
	if roundTrip[1] != 9 {
		t.Fatalf("converted rownums alias source column ids: %#v", roundTrip)
	}
}

func TestQuantaCandidateSetBuildsMaterializationRequest(t *testing.T) {
	candidates := QuantaCandidateSet{
		Index:        "orders",
		LogicalShard: "orders:2023-06",
		Replica:      "node-a",
		Rownums:      []QuantaRownum{1001, 1002},
	}
	fields := []QuantaProjectionField{
		{Index: "orders", Field: "o_orderkey", Type: DataTypeInt, Visible: true},
		{Index: "orders", Field: "o_totalprice", Type: DataTypeFloat, Visible: true},
	}

	request := candidates.MaterializationRequest(fields)
	if request.Index != "orders" || request.LogicalShard != "orders:2023-06" || request.Replica != "node-a" {
		t.Fatalf("request location = %#v, want candidate location", request)
	}
	if request.CandidateCount() != 2 || request.ProjectionCount() != 2 {
		t.Fatalf("request counts = %d/%d, want 2/2", request.CandidateCount(), request.ProjectionCount())
	}
	if request.Rownums[0] != 1001 || request.ProjectionFields[1].Field != "o_totalprice" {
		t.Fatalf("request materialization payload = %#v", request)
	}
	request.ProjectionExpressions = append(request.ProjectionExpressions, QuantaProjectionExpression{
		Expr:   Call("year", Field(FieldRef{Table: TableInstance{Table: "orders", Alias: "o"}, Name: "o_orderdate", Type: DataTypeTime})),
		Output: QuantaProjectionField{Index: "orders", Field: "year_o_orderdate", Type: DataTypeInt},
	})
	if request.ProjectionCount() != 3 {
		t.Fatalf("projection count with derived expression = %d, want 3", request.ProjectionCount())
	}
}

func TestQuantaCandidateSetMaterializationRequestCopiesSlices(t *testing.T) {
	candidates := QuantaCandidateSet{
		Index:   "orders",
		Rownums: []QuantaRownum{1001},
	}
	fields := []QuantaProjectionField{
		{Index: "orders", Field: "o_orderkey", Type: DataTypeInt, Visible: true},
	}

	request := candidates.MaterializationRequest(fields)
	candidates.Rownums[0] = 9999
	fields[0].Field = "mutated"
	if request.Rownums[0] != 1001 {
		t.Fatalf("request rownums alias candidate set: %#v", request.Rownums)
	}
	if request.ProjectionFields[0].Field != "o_orderkey" {
		t.Fatalf("request projection fields alias source fields: %#v", request.ProjectionFields)
	}
}

func TestQuantaIntermediateLowererLowersParameterizedBSIPredicate(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o where o.o_orderkey >= ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, int64(8)),
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.Index != "orders" {
		t.Fatalf("index = %q, want orders", fragment.Index)
	}
	if fragment.Field != "o_orderkey" {
		t.Fatalf("field = %q, want o_orderkey", fragment.Field)
	}
	if fragment.Operation != QuantaOperationIntersect {
		t.Fatalf("operation = %v, want INTERSECT", fragment.Operation)
	}
	if fragment.BSIOp != QuantaBSIOpGE {
		t.Fatalf("bsi op = %v, want GE", fragment.BSIOp)
	}
	if got := fragment.Value.Int64(); got != 8 {
		t.Fatalf("value = %d, want 8", got)
	}
}

func TestQuantaIntermediateLowererCoalescesInclusiveBSIRange(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o where o.o_orderkey >= ? and o.o_orderkey <= ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, int64(8)),
		IndexedParameterValue(2, ValueInt, int64(12)),
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.Index != "orders" {
		t.Fatalf("index = %q, want orders", fragment.Index)
	}
	if fragment.Field != "o_orderkey" {
		t.Fatalf("field = %q, want o_orderkey", fragment.Field)
	}
	if fragment.Operation != QuantaOperationIntersect {
		t.Fatalf("operation = %v, want INTERSECT", fragment.Operation)
	}
	if fragment.BSIOp != QuantaBSIOpRange {
		t.Fatalf("bsi op = %v, want RANGE", fragment.BSIOp)
	}
	if fragment.Value != nil {
		t.Fatalf("value = %v, want nil for range", fragment.Value)
	}
	if got := fragment.Begin.Int64(); got != 8 {
		t.Fatalf("begin = %d, want 8", got)
	}
	if got := fragment.End.Int64(); got != 12 {
		t.Fatalf("end = %d, want 12", got)
	}
}

func TestQuantaIntermediateNormalizeDiscreteTimeComparisonUsesNextBoundary(t *testing.T) {
	field := FieldRef{Encoding: LegacyEncodingProfile("SysMillisBSI", LegacyEncodingOptions{})}
	encoded := Literal(ValueInt, int64(1000))
	op, value := quantaIntermediateNormalizeDiscreteTimeComparison(BinaryOpLessEqual, field, Literal(ValueTime, time.UnixMilli(1000).UTC()), encoded)
	if op != BinaryOpLess || value.Value.(int64) != 1001 {
		t.Fatalf("<= normalized to %s %#v, want < 1001", op, value)
	}
	op, value = quantaIntermediateNormalizeDiscreteTimeComparison(BinaryOpGreater, field, Literal(ValueTime, time.UnixMilli(1000).UTC()), encoded)
	if op != BinaryOpGreaterEqual || value.Value.(int64) != 1001 {
		t.Fatalf("> normalized to %s %#v, want >= 1001", op, value)
	}

	dateLiteral := Literal(ValueString, "1996-12-31")
	dateEncoded := Literal(ValueInt, time.Date(1996, 12, 31, 0, 0, 0, 0, time.UTC).UnixMilli())
	op, value = quantaIntermediateNormalizeDiscreteTimeComparison(BinaryOpLessEqual, field, dateLiteral, dateEncoded)
	wantNextDay := time.Date(1997, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	if op != BinaryOpLess || value.Value.(int64) != wantNextDay {
		t.Fatalf("date <= normalized to %s %#v, want < %d", op, value, wantNextDay)
	}
}

func TestQuantaIntermediateLowererCoalescesDatetimeBoundsAfterNormalization(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o where o.o_orderdate >= '1995-01-01' and o.o_orderdate <= '1996-12-31'"},
		ExecutionOptions{},
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want one coalesced datetime range: %#v", len(intermediate.Fragments), intermediate.Fragments)
	}
	fragment := intermediate.Fragments[0]
	if fragment.BSIOp != QuantaBSIOpRange {
		t.Fatalf("bsi op = %s, want RANGE", fragment.BSIOp)
	}
	wantBegin := time.Date(1995, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	if got := fragment.Begin.Int64(); got != wantBegin {
		t.Fatalf("begin = %d, want %d", got, wantBegin)
	}
	wantUpper := time.Date(1997, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	if got := fragment.End.Int64(); got != wantUpper-1 {
		t.Fatalf("end = %d, want inclusive upper bound %d", got, wantUpper-1)
	}
}

func TestQuantaIntermediateLowererCoalescesBetweenToBSIRange(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o where o.o_orderkey between ? and ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, int64(8)),
		IndexedParameterValue(2, ValueInt, int64(12)),
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.BSIOp != QuantaBSIOpRange {
		t.Fatalf("bsi op = %v, want RANGE", fragment.BSIOp)
	}
	if got := fragment.Begin.Int64(); got != 8 {
		t.Fatalf("begin = %d, want 8", got)
	}
	if got := fragment.End.Int64(); got != 12 {
		t.Fatalf("end = %d, want 12", got)
	}
}

func TestQuantaIntermediateParseRelativeNowSupportsPositiveOffset(t *testing.T) {
	before := time.Now().UTC().Add(30 * time.Second)
	value, ok := quantaIntermediateParseRelativeNowTime("now+1m")
	after := time.Now().UTC().Add(90 * time.Second)
	if !ok {
		t.Fatal("expected now+1m to parse")
	}
	if value.Before(before) || value.After(after) {
		t.Fatalf("value = %s, want within [%s, %s]", value, before, after)
	}
}

func TestQuantaIntermediateLowererLowersNotBetweenToBSIDifferenceRange(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o where o.o_orderkey not between ? and ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, int64(8)),
		IndexedParameterValue(2, ValueInt, int64(12)),
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.Operation != QuantaOperationDifference {
		t.Fatalf("operation = %v, want DIFFERENCE", fragment.Operation)
	}
	if fragment.BSIOp != QuantaBSIOpRange {
		t.Fatalf("bsi op = %v, want RANGE", fragment.BSIOp)
	}
	if fragment.Negate {
		t.Fatalf("negate = true, want operation-level difference")
	}
	if got := fragment.Begin.Int64(); got != 8 {
		t.Fatalf("begin = %d, want 8", got)
	}
	if got := fragment.End.Int64(); got != 12 {
		t.Fatalf("end = %d, want 12", got)
	}
}

func TestQuantaIntermediateLowererLowersStringLexBSIBetweenPredicate(t *testing.T) {
	field := FieldRef{
		Table:    TableInstance{Table: "customers_qa"},
		Name:     "cust_id",
		Index:    IndexBSI,
		Encoding: LegacyEncodingProfile("StringLexBSI", LegacyEncodingOptions{PrefixLength: 0}),
	}
	predicate := Predicate{Expr: Binary(
		BinaryOpBetween,
		Field(field),
		List(Literal(ValueString, "105"), Literal(ValueString, "108")),
	)}

	fragment, diagnostics, ok := QuantaIntermediateLowerer{}.lowerBetweenPredicate(predicate, ParameterBindingSet{})
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v ok=%v", diagnostics, ok)
	}
	if fragment.BSIOp != QuantaBSIOpRange || fragment.Operation != QuantaOperationIntersect {
		t.Fatalf("fragment = %#v, want StringLexBSI RANGE intersect", fragment)
	}
	if fragment.Begin.Cmp(quantaIntermediateStringLexBSIValue("105", 0)) != 0 ||
		fragment.End.Cmp(quantaIntermediateStringLexBSIValue("108", 0)) != 0 {
		t.Fatalf("range = %v..%v, want lex-encoded string bounds", fragment.Begin, fragment.End)
	}
}

func TestQuantaIntermediateLowererLowersStringLexBSINotBetweenPredicate(t *testing.T) {
	field := FieldRef{
		Table:    TableInstance{Table: "customers_qa"},
		Name:     "cust_id",
		Index:    IndexBSI,
		Encoding: LegacyEncodingProfile("StringLexBSI", LegacyEncodingOptions{PrefixLength: 0}),
	}
	predicate := Predicate{Expr: Binary(
		BinaryOpNotBetween,
		Field(field),
		List(Literal(ValueString, "104"), Literal(ValueString, "105")),
	)}

	fragment, diagnostics, ok := QuantaIntermediateLowerer{}.lowerBetweenPredicate(predicate, ParameterBindingSet{})
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v ok=%v", diagnostics, ok)
	}
	if fragment.BSIOp != QuantaBSIOpRange || fragment.Operation != QuantaOperationDifference {
		t.Fatalf("fragment = %#v, want StringLexBSI RANGE difference", fragment)
	}
	if fragment.Begin.Cmp(quantaIntermediateStringLexBSIValue("104", 0)) != 0 ||
		fragment.End.Cmp(quantaIntermediateStringLexBSIValue("105", 0)) != 0 {
		t.Fatalf("range = %v..%v, want lex-encoded string bounds", fragment.Begin, fragment.End)
	}
}

func TestQuantaIntermediateLowererLowersBSIInPredicate(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o where o.o_orderkey in (?, ?)"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, int64(7)),
		IndexedParameterValue(2, ValueInt, int64(9)),
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.Index != "orders" {
		t.Fatalf("index = %q, want orders", fragment.Index)
	}
	if fragment.Field != "o_orderkey" {
		t.Fatalf("field = %q, want o_orderkey", fragment.Field)
	}
	if fragment.BSIOp != QuantaBSIOpBatchEQ {
		t.Fatalf("bsi op = %v, want BATCH_EQ", fragment.BSIOp)
	}
	if len(fragment.Values) != 2 {
		t.Fatalf("values = %d, want 2", len(fragment.Values))
	}
	if got := fragment.Values[0].Int64(); got != 7 {
		t.Fatalf("values[0] = %d, want 7", got)
	}
	if got := fragment.Values[1].Int64(); got != 9 {
		t.Fatalf("values[1] = %d, want 9", got)
	}
}

func TestQuantaIntermediateLowererLowersRownumEqualityAsBitmapValue(t *testing.T) {
	field := FieldRef{
		Table:        TableInstance{Schema: "quanta", Table: "customers_qa"},
		Name:         "rownum",
		Type:         DataTypeInt,
		Encoding:     LegacyEncodingProfile("IntDirect", LegacyEncodingOptions{}),
		PhysicalName: "rownum",
	}
	fragment, diagnostics, ok := quantaIntermediateLiteralComparisonFragment(BinaryOpEqual, field, LiteralExpr{Kind: ValueInt, Value: int64(3030)})
	if diagnostics.BlocksNative() || !ok {
		t.Fatalf("lower diagnostics=%#v ok=%v", diagnostics, ok)
	}
	if fragment.Field != "rownum" || fragment.Operation != QuantaOperationIntersect || fragment.BSIOp != QuantaBSIOpNone {
		t.Fatalf("fragment = %#v, want rownum standard-bitmap intersect", fragment)
	}
	if len(fragment.Values) != 1 || fragment.Values[0].Int64() != 3030 {
		t.Fatalf("values = %#v, want 3030", fragment.Values)
	}
	if !fragment.HasLiteral || fragment.Literal.Kind != ValueInt {
		t.Fatalf("literal metadata = %#v, want original int literal retained", fragment.Literal)
	}
}

func TestQuantaIntermediateLowererLowersStringEnumEqualityPredicate(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select l.l_shipmode as shipmode from lineitem as l where l.l_shipmode = ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueString, "AIR"),
	)

	intermediate, diagnostics := (QuantaIntermediateLowerer{Dictionaries: quantaIntermediateTestDictionaries()}).LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.Index != "lineitem" {
		t.Fatalf("index = %q, want lineitem", fragment.Index)
	}
	if fragment.Field != "l_shipmode" {
		t.Fatalf("field = %q, want l_shipmode", fragment.Field)
	}
	if fragment.BSIOp != QuantaBSIOpNone {
		t.Fatalf("bsi op = %v, want none for StringEnum bitmap", fragment.BSIOp)
	}
	if len(fragment.Values) != 1 {
		t.Fatalf("values = %d, want 1", len(fragment.Values))
	}
	if got := fragment.Values[0].Uint64(); got != 7 {
		t.Fatalf("values[0] = %d, want 7", got)
	}
}

func TestQuantaIntermediateLowererLowersStringEnumNotEqualPredicate(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select l.l_shipmode as shipmode from lineitem as l where l.l_shipmode != ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueString, "AIR"),
	)

	intermediate, diagnostics := (QuantaIntermediateLowerer{Dictionaries: quantaIntermediateTestDictionaries()}).LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.Operation != QuantaOperationDifference {
		t.Fatalf("operation = %v, want DIFFERENCE", fragment.Operation)
	}
	if fragment.BSIOp != QuantaBSIOpNone {
		t.Fatalf("bsi op = %v, want none for StringEnum bitmap", fragment.BSIOp)
	}
	if len(fragment.Values) != 1 {
		t.Fatalf("values = %d, want 1", len(fragment.Values))
	}
	if got := fragment.Values[0].Uint64(); got != 7 {
		t.Fatalf("values[0] = %d, want 7", got)
	}
}

func TestQuantaIntermediateLowererLowersStringEnumInPredicate(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select l.l_shipmode as shipmode from lineitem as l where l.l_shipmode in ('AIR', 'MAIL')"},
		ExecutionOptions{},
	)

	intermediate, diagnostics := (QuantaIntermediateLowerer{Dictionaries: quantaIntermediateTestDictionaries()}).LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.BSIOp != QuantaBSIOpNone {
		t.Fatalf("bsi op = %v, want none for StringEnum bitmap", fragment.BSIOp)
	}
	if len(fragment.Values) != 2 {
		t.Fatalf("values = %d, want 2", len(fragment.Values))
	}
	if got := fragment.Values[0].Uint64(); got != 7 {
		t.Fatalf("values[0] = %d, want 7", got)
	}
	if got := fragment.Values[1].Uint64(); got != 8 {
		t.Fatalf("values[1] = %d, want 8", got)
	}
}

func TestQuantaIntermediateLowererLowersStringEnumNotLikePrefixPredicate(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select count(*) as line_count from lineitem as l where l.l_shipmode not like 'A%'"},
		ExecutionOptions{},
	)
	if request.Diagnostics.BlocksNative() {
		t.Fatalf("request diagnostics: %#v", request.Diagnostics)
	}
	if len(request.Bound.Prepared.Query.Predicates) != 1 {
		t.Fatalf("predicates = %d, want 1", len(request.Bound.Prepared.Query.Predicates))
	}
	predicate := request.Bound.Prepared.Query.Predicates[0]
	if predicate.Placement != PredicatePushdown {
		t.Fatalf("predicate placement = %s, want pushdown: %#v", predicate.Placement, predicate)
	}
	if !predicateHasCapability(predicate, CapabilityStringEnumPrefixLike) || !predicateHasCapability(predicate, CapabilityBitmapDifference) {
		t.Fatalf("predicate capabilities = %#v, want StringEnumPrefixLike and BitmapDifference", predicate.Capabilities)
	}

	intermediate, diagnostics := (QuantaIntermediateLowerer{Dictionaries: quantaIntermediateTestDictionaries()}).LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1: %#v", len(intermediate.Fragments), intermediate.Fragments)
	}
	fragment := intermediate.Fragments[0]
	if fragment.Operation != QuantaOperationDifference {
		t.Fatalf("operation = %s, want DIFFERENCE", fragment.Operation)
	}
	if len(fragment.Values) != 1 || fragment.Values[0].Uint64() != 7 {
		t.Fatalf("values = %#v, want AIR id 7", fragment.Values)
	}
}

func TestQuantaIntermediateLowererKeepsQ16NotLikeResidual(t *testing.T) {
	partBrandRef := DictionaryRef{Schema: "quanta", Table: "part", Field: "p_brand"}
	service := NewPlanningService(Planner{
		Parser:        SimpleParserBridge{},
		DefaultSchema: "quanta",
		Catalog: MemoryCatalog{
			Tables: []TableDefinition{{
				Schema: "quanta",
				Name:   "part",
				Fields: []FieldDefinition{
					{Name: "p_partkey", Type: DataTypeInt, Index: IndexBSI},
					{
						Name:     "p_brand",
						Type:     DataTypeString,
						Index:    IndexStringEnum,
						Encoding: LegacyEncodingProfile("StringEnum", LegacyEncodingOptions{}),
						Dictionary: DictionaryDefinition{
							Ref:          partBrandRef,
							Version:      "v1",
							Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
						},
					},
					{Name: "p_type", Type: DataTypeString, Index: IndexStringEnum, Encoding: LegacyEncodingProfile("StringEnum", LegacyEncodingOptions{})},
					{Name: "p_size", Type: DataTypeInt, Index: IndexBSI},
				},
			}},
			Functions: []FunctionDefinition{{Name: "count", Kind: FunctionAggregate, ReturnType: DataTypeInt, Native: true}},
		},
	}, nil)
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: `select count(*) as part_count
from part
where p_brand <> 'Brand#45'
  and p_type not like 'MEDIUM POLISHED%'
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9)`},
		ExecutionOptions{},
	)
	if request.Diagnostics.BlocksNative() {
		t.Fatalf("request diagnostics: %#v", request.Diagnostics)
	}
	if len(request.Bound.Prepared.Query.Predicates) != 3 {
		t.Fatalf("predicates = %d, want 3: %#v", len(request.Bound.Prepared.Query.Predicates), request.Bound.Prepared.Query.Predicates)
	}
	residuals := 0
	for _, predicate := range request.Bound.Prepared.Query.Predicates {
		if predicate.Placement == PredicateResidualScan {
			residuals++
		}
	}
	if residuals != 1 {
		t.Fatalf("residual predicates = %d, want 1: %#v", residuals, request.Bound.Prepared.Query.Predicates)
	}

	intermediate, diagnostics := (QuantaIntermediateLowerer{Dictionaries: MemoryDictionaryResolver{
		Dictionaries: []DictionaryDefinition{{
			Ref:          partBrandRef,
			Version:      "v1",
			Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
		}},
		Entries: []DictionaryEntry{{Ref: partBrandRef, Label: "Brand#45", ID: 45, Version: "v1"}},
	}}).LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 2 {
		t.Fatalf("fragments = %d, want 2: %#v", len(intermediate.Fragments), intermediate.Fragments)
	}
	fragmentByField := make(map[string]QuantaQueryFragment, len(intermediate.Fragments))
	for _, fragment := range intermediate.Fragments {
		fragmentByField[fragment.Field] = fragment
	}
	brandFragment, ok := fragmentByField["p_brand"]
	if !ok {
		t.Fatalf("missing p_brand fragment: %#v", intermediate.Fragments)
	}
	if brandFragment.Operation != QuantaOperationDifference || brandFragment.Negate {
		t.Fatalf("p_brand fragment = %#v, want bitmap DIFFERENCE without Negate", brandFragment)
	}
	projectionFields := make(map[string]QuantaProjectionField, len(intermediate.ProjectionFields))
	for _, field := range intermediate.ProjectionFields {
		projectionFields[field.Field] = field
	}
	for _, field := range []string{"p_brand", "p_type", "p_size"} {
		projection, ok := projectionFields[field]
		if !ok {
			t.Fatalf("projection fields missing %s: %#v", field, intermediate.ProjectionFields)
		}
		if projection.Visible {
			t.Fatalf("projection field %s visible = true, want hidden residual/input field", field)
		}
	}
}

func TestQuantaIntermediateLowererLowersStringLexEqualityPredicate(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select c.c_custkey as customer_id from customer as c where c.c_name = 'Annie'"},
		ExecutionOptions{},
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.BSIOp != QuantaBSIOpEQ || fragment.Value == nil {
		t.Fatalf("fragment = %#v, want StringLexBSI BSI equality payload", fragment)
	}
	if want := quantaIntermediateStringLexBSIValue("Annie", 10); fragment.Value.Cmp(want) != 0 {
		t.Fatalf("value = %v, want lexical prefix %v", fragment.Value, want)
	}
	if fragment.HasLiteral {
		t.Fatalf("fragment = %#v, did not expect raw literal payload for StringLexBSI", fragment)
	}
}

func TestQuantaIntermediateLowererLowersStringLexInequalityPredicate(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select c.c_custkey as customer_id from customer as c where c.c_name != 'Annie'"},
		ExecutionOptions{},
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.BSIOp != QuantaBSIOpEQ || fragment.Operation != QuantaOperationDifference || fragment.Negate || fragment.Value == nil {
		t.Fatalf("fragment = %#v, want StringLexBSI equality payload as bitmap DIFFERENCE", fragment)
	}
}

func TestQuantaIntermediateLowererLowersStringLexBSIEqualityPredicate(t *testing.T) {
	field := FieldRef{
		Table:    TableInstance{Table: "part"},
		Name:     "p_brand",
		Index:    IndexBSI,
		Encoding: LegacyEncodingProfile("StringLexBSI", LegacyEncodingOptions{PrefixLength: 10, MaxLength: 10}),
	}

	fragment, diagnostics, ok := quantaIntermediateStringLexBSIComparisonFragment(BinaryOpEqual, field, Literal(ValueString, "Brand#45"))
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v ok=%v", diagnostics, ok)
	}
	if fragment.BSIOp != QuantaBSIOpEQ || fragment.Value == nil || fragment.HasLiteral {
		t.Fatalf("fragment = %#v, want StringLexBSI BSI equality payload", fragment)
	}
	if want := quantaIntermediateStringLexBSIValue("Brand#45", 10); fragment.Value.Cmp(want) != 0 {
		t.Fatalf("value = %v, want lex value %v", fragment.Value, want)
	}
}

func TestQuantaIntermediateLowererLowersStringLexBSIInPredicate(t *testing.T) {
	field := FieldRef{
		Table:    TableInstance{Table: "part"},
		Name:     "p_brand",
		Index:    IndexBSI,
		Encoding: LegacyEncodingProfile("StringLexBSI", LegacyEncodingOptions{PrefixLength: 10, MaxLength: 10}),
	}

	fragment, diagnostics, ok := QuantaIntermediateLowerer{}.lowerStringLexBSIInPredicate(
		field,
		List(Literal(ValueString, "Brand#45"), Literal(ValueString, "Brand#12")),
		ParameterBindingSet{},
		false,
	)
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v ok=%v", diagnostics, ok)
	}
	if fragment.BSIOp != QuantaBSIOpBatchEQ || fragment.Operation != QuantaOperationIntersect {
		t.Fatalf("fragment = %#v, want StringLexBSI BATCH_EQ intersect", fragment)
	}
	if len(fragment.Values) != 2 {
		t.Fatalf("values = %d, want 2", len(fragment.Values))
	}
	if fragment.Values[0].Cmp(quantaIntermediateStringLexBSIValue("Brand#45", 10)) != 0 ||
		fragment.Values[1].Cmp(quantaIntermediateStringLexBSIValue("Brand#12", 10)) != 0 {
		t.Fatalf("values = %#v, want lex-encoded brands", fragment.Values)
	}
}

func TestQuantaIntermediateLowererBlocksStringLexBSIRemainderEqualityPredicate(t *testing.T) {
	field := FieldRef{
		Table:    TableInstance{Table: "lineitem"},
		Name:     "l_comment",
		Index:    IndexBSI,
		Encoding: LegacyEncodingProfile("StringLexBSI", LegacyEncodingOptions{PrefixLength: 8, MaxLength: 256}),
	}

	_, diagnostics, ok := quantaIntermediateStringLexBSIComparisonFragment(BinaryOpEqual, field, Literal(ValueString, "carefully pending packages"))
	if ok || !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v ok=%v, want prefix+remainder StringLex equality blocked", diagnostics, ok)
	}
}

func TestQuantaIntermediateLowererBlocksStringLexBSIRemainderInPredicate(t *testing.T) {
	field := FieldRef{
		Table:    TableInstance{Table: "lineitem"},
		Name:     "l_comment",
		Index:    IndexBSI,
		Encoding: LegacyEncodingProfile("StringLexBSI", LegacyEncodingOptions{PrefixLength: 8, MaxLength: 256}),
	}

	_, diagnostics, ok := QuantaIntermediateLowerer{}.lowerStringLexBSIInPredicate(
		field,
		List(Literal(ValueString, "carefully pending packages")),
		ParameterBindingSet{},
		false,
	)
	if ok || !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v ok=%v, want prefix+remainder StringLex IN blocked", diagnostics, ok)
	}
}

func TestQuantaIntermediateLowererLowersLiteralInPredicate(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select c.c_custkey as customer_id from customer as c where c.c_name in ('Annie', 'Bob')"},
		ExecutionOptions{},
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.Field != "c_name" {
		t.Fatalf("field = %q, want c_name", fragment.Field)
	}
	if fragment.BSIOp != QuantaBSIOpBatchEQ {
		t.Fatalf("BSIOp = %q, want %q", fragment.BSIOp, QuantaBSIOpBatchEQ)
	}
	if len(fragment.Values) != 2 {
		t.Fatalf("values = %d, want 2", len(fragment.Values))
	}
	if fragment.Values[0].Cmp(quantaIntermediateStringLexBSIValue("Annie", 10)) != 0 ||
		fragment.Values[1].Cmp(quantaIntermediateStringLexBSIValue("Bob", 10)) != 0 {
		t.Fatalf("values = %#v, want Annie/Bob lexical prefixes", fragment.Values)
	}
}

func TestQuantaIntermediateLowererLowersLiteralNotInPredicate(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select c.c_custkey as customer_id from customer as c where c.c_name not in ('Annie', 'Bob')"},
		ExecutionOptions{},
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
	}
	fragment := intermediate.Fragments[0]
	if fragment.Negate {
		t.Fatalf("fragment.Negate = true, want false")
	}
	if fragment.Operation != QuantaOperationDifference {
		t.Fatalf("operation = %q, want %q", fragment.Operation, QuantaOperationDifference)
	}
	if fragment.Field != "c_name" {
		t.Fatalf("field = %q, want c_name", fragment.Field)
	}
	if fragment.BSIOp != QuantaBSIOpBatchEQ {
		t.Fatalf("BSIOp = %q, want %q", fragment.BSIOp, QuantaBSIOpBatchEQ)
	}
	if len(fragment.Values) != 2 {
		t.Fatalf("values = %d, want 2", len(fragment.Values))
	}
}

func TestQuantaIntermediateNormalizeTimeValueUsesFieldGranularity(t *testing.T) {
	field := FieldRef{
		Table:    TableInstance{Table: "orders_qa"},
		Name:     "order_date",
		Index:    IndexDateTime,
		Encoding: LegacyEncodingProfile("SysMicroBSI", LegacyEncodingOptions{}),
	}

	normalized, diagnostics, ok := quantaIntermediateNormalizeTimeValue(field, Literal(ValueString, "2023-06-02T03:00:00"))
	if !ok || diagnostics.BlocksNative() {
		t.Fatalf("normalize diagnostics = %#v ok=%v", diagnostics, ok)
	}
	want := time.Date(2023, 6, 2, 3, 0, 0, 0, time.UTC).UnixMicro()
	if got, ok := normalized.Value.(int64); !ok || got != want {
		t.Fatalf("normalized = %#v, want epoch micros %d", normalized, want)
	}
}

func TestQuantaIntermediateLowererLowersNullPredicates(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		negate bool
	}{
		{
			name: "equals null",
			sql:  "select c.c_custkey as customer_id from customer as c where c.c_name = null",
		},
		{
			name:   "is not null",
			sql:    "select c.c_custkey as customer_id from customer as c where c.c_name is not null",
			negate: true,
		},
		{
			name: "string enum is null",
			sql:  "select count(*) from lineitem where l_shipmode is null",
		},
		{
			name:   "string enum is not null",
			sql:    "select count(*) from lineitem where l_shipmode is not null",
			negate: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := simpleRunnerPlanningService()
			_, request := service.PrepareExecutionRequest(
				PlanRequest{SQL: test.sql},
				ExecutionOptions{},
			)

			intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
			if diagnostics.BlocksNative() {
				t.Fatalf("lower diagnostics: %#v", diagnostics)
			}
			if len(intermediate.Fragments) != 1 {
				t.Fatalf("fragments = %d, want 1", len(intermediate.Fragments))
			}
			fragment := intermediate.Fragments[0]
			if !fragment.NullCheck {
				t.Fatalf("fragment.NullCheck = false, want true")
			}
			if fragment.Negate != test.negate {
				t.Fatalf("fragment.Negate = %v, want %v", fragment.Negate, test.negate)
			}
		})
	}
}

func TestQuantaIntermediateLowererLowersOrPredicatesToUnionFragments(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select c.c_custkey as customer_id from customer as c where c.c_name = 'Annie' or c.c_name = 'Bob'"},
		ExecutionOptions{},
	)

	intermediate, diagnostics := QuantaIntermediateLowerer{}.LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 2 {
		t.Fatalf("fragments = %d, want 2", len(intermediate.Fragments))
	}
	for index, fragment := range intermediate.Fragments {
		if fragment.Operation != QuantaOperationUnion {
			t.Fatalf("fragment %d operation = %q, want %q", index, fragment.Operation, QuantaOperationUnion)
		}
	}
}

func TestQuantaIntermediateLowererTreatsMissingStringEnumEqualityAsEmptySet(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select count(*) from lineitem where l_shipmode = 'BOAT'"},
		ExecutionOptions{},
	)
	if request.Diagnostics.BlocksNative() {
		t.Fatalf("request diagnostics: %#v", request.Diagnostics)
	}

	intermediate, diagnostics := (QuantaIntermediateLowerer{Dictionaries: quantaIntermediateTestDictionaries()}).LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1: %#v", len(intermediate.Fragments), intermediate.Fragments)
	}
	fragment := intermediate.Fragments[0]
	if fragment.Field != "l_shipmode" || fragment.Operation != QuantaOperationIntersect {
		t.Fatalf("fragment = %#v, want l_shipmode INTERSECT", fragment)
	}
	if len(fragment.Values) != 1 || fragment.Values[0].Cmp(quantaIntermediateImpossibleStringEnumID()) != 0 {
		t.Fatalf("fragment values = %#v, want impossible StringEnum id", fragment.Values)
	}
}

func TestQuantaIntermediateLowererTreatsMissingStringEnumInequalityAsFullSetDifference(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select count(*) from lineitem where l_shipmode != 'BOAT'"},
		ExecutionOptions{},
	)
	if request.Diagnostics.BlocksNative() {
		t.Fatalf("request diagnostics: %#v", request.Diagnostics)
	}

	intermediate, diagnostics := (QuantaIntermediateLowerer{Dictionaries: quantaIntermediateTestDictionaries()}).LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1: %#v", len(intermediate.Fragments), intermediate.Fragments)
	}
	fragment := intermediate.Fragments[0]
	if fragment.Field != "l_shipmode" || fragment.Operation != QuantaOperationDifference {
		t.Fatalf("fragment = %#v, want l_shipmode DIFFERENCE", fragment)
	}
	if len(fragment.Values) != 1 || fragment.Values[0].Cmp(quantaIntermediateImpossibleStringEnumID()) != 0 {
		t.Fatalf("fragment values = %#v, want impossible StringEnum id", fragment.Values)
	}
}

func TestQuantaIntermediateLowererSkipsMissingStringEnumInValues(t *testing.T) {
	service := simpleRunnerPlanningService()
	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select count(*) from lineitem where l_shipmode in ('AIR', 'BOAT')"},
		ExecutionOptions{},
	)
	if request.Diagnostics.BlocksNative() {
		t.Fatalf("request diagnostics: %#v", request.Diagnostics)
	}

	intermediate, diagnostics := (QuantaIntermediateLowerer{Dictionaries: quantaIntermediateTestDictionaries()}).LowerExecutionRequest(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics: %#v", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1: %#v", len(intermediate.Fragments), intermediate.Fragments)
	}
	fragment := intermediate.Fragments[0]
	if len(fragment.Values) != 1 || fragment.Values[0].Int64() != 7 {
		t.Fatalf("fragment values = %#v, want only AIR id 7", fragment.Values)
	}
	if len(fragment.Literals) != 1 || fragment.Literals[0].Value != "AIR" {
		t.Fatalf("fragment literals = %#v, want only AIR", fragment.Literals)
	}
}

func predicateHasCapability(predicate Predicate, capability PlanCapability) bool {
	for _, existing := range predicate.Capabilities {
		if existing == capability {
			return true
		}
	}
	return false
}

func quantaIntermediateTestDictionaries() MemoryDictionaryResolver {
	ref := DictionaryRef{Schema: "quanta", Table: "lineitem", Field: "l_shipmode"}
	return MemoryDictionaryResolver{
		Dictionaries: []DictionaryDefinition{{
			Ref:          ref,
			Version:      "v1",
			Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs, DictionaryCapabilityPrefixMatch},
		}},
		Entries: []DictionaryEntry{
			{Ref: ref, Label: "AIR", ID: 7, Version: "v1"},
			{Ref: ref, Label: "MAIL", ID: 8, Version: "v1"},
		},
	}
}
