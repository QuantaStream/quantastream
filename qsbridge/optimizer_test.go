package qsbridge

import "testing"

func TestOptimizationTraceRecordsAppliedAdvisoryAndBlockedRewrites(t *testing.T) {
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	partName := FieldRef{Table: part, Name: "p_name", Index: IndexBackingString}
	trace := NewOptimizationTrace()
	trace.Add(RewriteAppliedRecord(
		RewritePredicatePushdown,
		"lowered StringEnum prefix LIKE into bitmap predicate",
		"filter(residual)",
		"filter(pushdown)",
	))
	trace.Add(RewriteAdvisoryRecord(
		RewritePredicatePushdown,
		"predicate could be faster with a searchable index",
		partName,
	))
	trace.Add(RewriteBlockedRecord(
		RewriteOuterJoinBoundary,
		"cannot move WHERE predicate across null-extension boundary",
		DiagnosticSet{ErrorDiagnostic(DiagnosticOuterJoin, PhasePlan, "outer join boundary")},
		partName,
	))

	if trace.Supported {
		t.Fatalf("expected blocked rewrite diagnostic to mark trace unsupported")
	}
	if len(trace.Applied()) != 1 {
		t.Fatalf("applied rewrites = %#v, want one", trace.Applied())
	}
	if trace.Applied()[0].Category != RewriteCategoryCompatibility || trace.Applied()[0].Impact != RewriteImpactLogicalShape {
		t.Fatalf("applied rewrite = %#v, want compatibility logical-shape metadata", trace.Applied()[0])
	}
	if len(trace.Advisories()) != 1 {
		t.Fatalf("advisories = %#v, want one", trace.Advisories())
	}
	if trace.Advisories()[0].Category != RewriteCategoryPerformance || trace.Advisories()[0].Impact != RewriteImpactDiagnosticsOnly {
		t.Fatalf("advisory rewrite = %#v, want performance diagnostics-only metadata", trace.Advisories()[0])
	}
	blocked := trace.Blocked()
	if len(blocked) != 1 {
		t.Fatalf("blocked rewrites = %#v, want one", blocked)
	}
	if blocked[0].Rule != RewriteOuterJoinBoundary {
		t.Fatalf("blocked rule = %q, want outer join boundary", blocked[0].Rule)
	}
	if blocked[0].Category != RewriteCategorySafety || blocked[0].Impact != RewriteImpactNone {
		t.Fatalf("blocked rewrite = %#v, want safety/no-impact metadata", blocked[0])
	}
	if len(trace.Diagnostics) != 1 || trace.Diagnostics[0].Code != DiagnosticOuterJoin {
		t.Fatalf("trace diagnostics = %#v, want outer join diagnostic", trace.Diagnostics)
	}
	summary := trace.Summary()
	if summary.Supported || summary.Total != 3 || summary.Applied != 1 || summary.Advisory != 1 || summary.Blocked != 1 {
		t.Fatalf("summary = %#v, want aggregate rewrite counts", summary)
	}
	if summary.Compatibility != 1 || summary.Performance != 1 || summary.Safety != 1 {
		t.Fatalf("summary = %#v, want category counts", summary)
	}
	if summary.LogicalImpact != 1 || summary.DiagnosticOnly != 1 || summary.NoImpact != 1 {
		t.Fatalf("summary = %#v, want impact counts", summary)
	}
}

func TestOptimizationTraceCloneDoesNotAliasSlices(t *testing.T) {
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	partName := FieldRef{Table: part, Name: "p_name", Index: IndexBackingString}
	trace := NewOptimizationTrace()
	trace.Add(RewriteAdvisoryRecord("rule", "reason", partName))

	cloned := trace.Clone()
	cloned.Rewrites[0].Fields[0].Name = "mutated"
	cloned.Rewrites[0].Category = RewriteCategoryPhysical
	if trace.Rewrites[0].Fields[0].Name != "p_name" {
		t.Fatalf("clone mutation leaked into original trace")
	}
	if trace.Rewrites[0].Category != RewriteCategoryPerformance {
		t.Fatalf("clone category mutation leaked into original trace")
	}
}

func TestAnalyzeOptimizationCandidatesAdvisesTopNGroupedCount(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipMode := FieldRef{Table: lineitem, Name: "l_shipmode", Index: IndexStringEnum}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{lineitem},
		GroupBy: []Expr{Field(shipMode)},
		Aggregates: []Aggregate{{
			Function: "count",
			Alias:    "line_count",
		}},
		Projection: []ProjectionColumn{
			{Expr: Field(shipMode)},
			{Expr: AggregateRef("line_count", 0), Alias: "line_count", Type: DataTypeInt},
		},
		OrderBy: []SortSpec{{Expr: AggregateRef("line_count", 0), Direction: SortDescending}},
		Result:  ResultShape{Kind: ResultQuery, Limit: 10},
	}

	trace := AnalyzeOptimizationCandidates(query)
	if !trace.Supported {
		t.Fatalf("trace = %#v, want supported advisory trace", trace)
	}
	record, ok := optimizationRecordByRule(trace, RewriteTopNGroupedCount)
	if !ok {
		t.Fatalf("rewrites = %#v, want topn advisory", trace.Rewrites)
	}
	if record.Rule != RewriteTopNGroupedCount || record.Status != RewriteAdvisory {
		t.Fatalf("record = %#v, want topn advisory", record)
	}
	if record.Category != RewriteCategoryPerformance || record.Impact != RewriteImpactDiagnosticsOnly {
		t.Fatalf("record = %#v, want performance diagnostics-only advisory", record)
	}
	if record.Before == "" || record.After != "native_topn(field)" {
		t.Fatalf("record before/after = %q/%q, want native topn candidate", record.Before, record.After)
	}
	if len(record.Capabilities) != 1 || record.Capabilities[0] != CapabilityNativeTopN {
		t.Fatalf("record capabilities = %#v, want NativeTopN", record.Capabilities)
	}
	if len(record.Fields) != 1 || record.Fields[0].QualifiedName() != "l.l_shipmode" {
		t.Fatalf("record fields = %#v, want l.l_shipmode", record.Fields)
	}
}

func TestAnalyzeOptimizationCandidatesSkipsNonTopNGroupedCount(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipMode := FieldRef{Table: lineitem, Name: "l_shipmode", Index: IndexStringEnum}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{lineitem},
		GroupBy: []Expr{Field(shipMode)},
		Aggregates: []Aggregate{{
			Function: "count",
			Alias:    "line_count",
		}},
		OrderBy: []SortSpec{{Expr: AggregateRef("line_count", 0), Direction: SortAscending}},
		Result:  ResultShape{Kind: ResultQuery, Limit: 10},
	}

	trace := AnalyzeOptimizationCandidates(query)
	if _, ok := optimizationRecordByRule(trace, RewriteTopNGroupedCount); ok {
		t.Fatalf("rewrites = %#v, want no topn advisory for ascending count order", trace.Rewrites)
	}
}

func TestAnalyzeOptimizationCandidatesAdvisesPlannerHardeningShapes(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Type: DataTypeInt}
	orderDate := FieldRef{Table: orders, Name: "o_orderdate", Type: DataTypeTime, Index: IndexDateTime}
	orderCustKey := FieldRef{Table: orders, Name: "o_custkey", Type: DataTypeInt}
	customerKey := FieldRef{Table: customer, Name: "c_custkey", Type: DataTypeInt}

	fullScan := QueryIR{
		Kind:       QueryKindSelect,
		Sources:    []TableInstance{orders},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
	}
	trace := AnalyzeOptimizationCandidates(fullScan)
	if _, ok := optimizationRecordByRule(trace, RewriteFullTableScan); !ok {
		t.Fatalf("rewrites = %#v, want full-table scan advisory", trace.Rewrites)
	}

	joined := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders, customer},
		Joins: []JoinEdge{{
			Left:      orderCustKey,
			Right:     customerKey,
			Kind:      JoinKindInner,
			Direction: JoinChildToParent,
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{
					RelationshipCapabilityParentLookup,
					RelationshipCapabilityJoinReduction,
				},
			},
			Legal: true,
		}},
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpGreaterEqual, Field(orderDate), Literal(ValueString, "1995-01-01")),
			Placement: PredicatePushdown,
			Scope:     PredicateScopeWhere,
		}},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
	}
	trace = AnalyzeOptimizationCandidates(joined)
	if record, ok := optimizationRecordByRule(trace, RewriteRelationshipVectorStrategy); !ok || record.After != "relationship_vector_reduction" {
		t.Fatalf("rewrites = %#v, want relationship-vector strategy advisory", trace.Rewrites)
	}
	if record, ok := optimizationRecordByRule(trace, RewriteShardTimeWindowAwareness); !ok || len(record.Fields) != 1 || record.Fields[0].QualifiedName() != "o.o_orderdate" {
		t.Fatalf("rewrites = %#v, want shard-window advisory for o.o_orderdate", trace.Rewrites)
	}
}

func TestExplainOptimizedLogicalPlanIncludesOptimizationTrace(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipMode := FieldRef{Table: lineitem, Name: "l_shipmode", Index: IndexStringEnum}
	query := QueryIR{
		Kind:       QueryKindSelect,
		Sources:    []TableInstance{lineitem},
		Projection: []ProjectionColumn{{Expr: Field(shipMode)}},
		Result:     ResultShape{Kind: ResultQuery, Columns: []FieldRef{shipMode}},
	}
	logical := BuildLogicalPlan(query)
	trace := NewOptimizationTrace()
	trace.Add(RewriteAppliedRecord(
		RewriteHiddenProjection,
		"added hidden field for downstream operator",
		"project(columns=1,hidden=0)",
		"project(columns=1,hidden=1)",
	))

	explanation := ExplainOptimizedLogicalPlan(logical, trace)
	if !explanation.Supported {
		t.Fatalf("expected supported optimized explanation")
	}
	if len(explanation.Optimization.Rewrites) != 1 {
		t.Fatalf("optimization rewrites = %#v, want one", explanation.Optimization.Rewrites)
	}
	if explanation.Optimization.Rewrites[0].Rule != RewriteHiddenProjection {
		t.Fatalf("rewrite rule = %q, want hidden projection", explanation.Optimization.Rewrites[0].Rule)
	}
}

func TestExplainOptimizedLogicalPlanReflectsBlockedOptimization(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipMode := FieldRef{Table: lineitem, Name: "l_shipmode", Index: IndexStringEnum}
	query := QueryIR{
		Kind:       QueryKindSelect,
		Sources:    []TableInstance{lineitem},
		Projection: []ProjectionColumn{{Expr: Field(shipMode)}},
		Result:     ResultShape{Kind: ResultQuery, Columns: []FieldRef{shipMode}},
	}
	logical := BuildLogicalPlan(query)
	trace := NewOptimizationTrace()
	trace.Add(RewriteBlockedRecord(
		RewriteJoinReorder,
		"join order is fixed by outer join boundary",
		DiagnosticSet{ErrorDiagnostic(DiagnosticOuterJoin, PhasePlan, "outer join boundary")},
	))

	explanation := ExplainOptimizedLogicalPlan(logical, trace)
	if explanation.Supported {
		t.Fatalf("expected blocked optimization to make explanation unsupported")
	}
	if len(explanation.Optimization.Diagnostics) != 1 || explanation.Optimization.Diagnostics[0].Code != DiagnosticOuterJoin {
		t.Fatalf("optimization diagnostics = %#v, want outer join diagnostic", explanation.Optimization.Diagnostics)
	}
}

func optimizationRecordByRule(trace OptimizationTrace, rule RewriteRuleID) (RewriteRecord, bool) {
	for _, record := range trace.Rewrites {
		if record.Rule == rule {
			return record, true
		}
	}
	return RewriteRecord{}, false
}
