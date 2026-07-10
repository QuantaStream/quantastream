package qsbridge

import "testing"

func TestSameRowBSIComparisonPredicateRecognizesSameTableDateFields(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem_1", Table: "lineitem", Alias: "l"}
	commitDate := FieldRef{Table: lineitem, Name: "l_commitdate", Type: DataTypeTime, Index: IndexDateTime}
	receiptDate := FieldRef{Table: lineitem, Name: "l_receiptdate", Type: DataTypeTime, Index: IndexDateTime}
	predicate := Predicate{
		Expr:      Binary(BinaryOpLess, Field(commitDate), Field(receiptDate)),
		Placement: PredicateResidualScan,
		Scope:     PredicateScopeWhere,
	}

	left, right, op, ok := SameRowBSIComparisonPredicate(predicate)

	if !ok {
		t.Fatalf("predicate was not recognized as same-row BSI comparison")
	}
	if left.Name != "l_commitdate" || right.Name != "l_receiptdate" || op != BinaryOpLess {
		t.Fatalf("comparison = %s %s %s, want l_commitdate < l_receiptdate", left.Name, op, right.Name)
	}
}

func TestSameRowBSIComparisonPredicateRejectsCrossTableFields(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem_1", Table: "lineitem", Alias: "l"}
	orders := TableInstance{ID: "orders_1", Table: "orders", Alias: "o"}
	predicate := Predicate{
		Expr: Binary(
			BinaryOpLess,
			Field(FieldRef{Table: lineitem, Name: "l_commitdate", Type: DataTypeTime, Index: IndexDateTime}),
			Field(FieldRef{Table: orders, Name: "o_orderdate", Type: DataTypeTime, Index: IndexDateTime}),
		),
		Placement: PredicateResidualJoin,
		Scope:     PredicateScopeWhere,
	}

	if _, _, _, ok := SameRowBSIComparisonPredicate(predicate); ok {
		t.Fatalf("cross-table predicate should not be same-row BSI comparison")
	}
}

func TestSameRowBSIComparisonPredicateRecognizesNumericBSITypeWithoutIndexHint(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem_1", Table: "lineitem", Alias: "l"}
	quantity := FieldRef{Table: lineitem, Name: "l_quantity", Type: DataTypeInt}
	linenumber := FieldRef{Table: lineitem, Name: "l_linenumber", Type: DataTypeInt}
	predicate := Predicate{
		Expr:      Binary(BinaryOpGreaterEqual, Field(quantity), Field(linenumber)),
		Placement: PredicateResidualScan,
		Scope:     PredicateScopeWhere,
	}

	left, right, op, ok := SameRowBSIComparisonPredicate(predicate)

	if !ok {
		t.Fatalf("numeric same-row predicate without index hint was not recognized")
	}
	if left.Name != "l_quantity" || right.Name != "l_linenumber" || op != BinaryOpGreaterEqual {
		t.Fatalf("comparison = %s %s %s, want l_quantity >= l_linenumber", left.Name, op, right.Name)
	}
}

func TestSameRowComparisonPlanBuildsKernelRequest(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem_1", Table: "lineitem", Alias: "l"}
	commitDate := FieldRef{Table: lineitem, Name: "l_commitdate", Type: DataTypeTime, Index: IndexDateTime}
	receiptDate := FieldRef{Table: lineitem, Name: "l_receiptdate", Type: DataTypeTime, Index: IndexDateTime}
	query := QueryIR{
		Kind: QueryKindSelect,
		Predicates: []Predicate{{
			Expr:         Binary(BinaryOpLess, Field(commitDate), Field(receiptDate)),
			Placement:    PredicateResidualScan,
			Scope:        PredicateScopeWhere,
			Capabilities: []PlanCapability{CapabilityNativeSameRowBSIComparison},
		}},
	}

	plans := SameRowComparisonPlans(query)

	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one same-row comparison plan", plans)
	}
	plan := plans[0]
	if plan.ID != "same_row_comparison.1.l.l_commitdate.l.l_receiptdate" || plan.ProbeName != "same_row_comparison_1_l_l_commitdate_l_l_receiptdate" {
		t.Fatalf("plan identity = %#v, want stable same-row identity", plan)
	}
	if plan.Kind != SameRowComparisonBSI || plan.Domain.Name() != "l" {
		t.Fatalf("plan kind/domain = %q/%q, want BSI over l", plan.Kind, plan.Domain.Name())
	}

	request := plan.Request([]QuantaRownum{7, 8})
	if request.ID != plan.ID || request.ProbePrefix != plan.ProbeName+"_" {
		t.Fatalf("request identity = %#v, want plan-derived identity", request)
	}
	if request.CandidateCount() != 2 || !request.Cacheable {
		t.Fatalf("request candidates/cacheable = %d/%v, want two cacheable candidates", request.CandidateCount(), request.Cacheable)
	}
	if request.Left.Name != "l_commitdate" || request.Right.Name != "l_receiptdate" || request.Operator != BinaryOpLess {
		t.Fatalf("request comparison = %#v, want commitdate < receiptdate", request)
	}
}

func TestSameRowComparisonPlanSketchesExecutionLifecycle(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem_1", Table: "lineitem", Alias: "l"}
	commitDate := FieldRef{Table: lineitem, Name: "l_commitdate", Type: DataTypeTime, Index: IndexDateTime}
	receiptDate := FieldRef{Table: lineitem, Name: "l_receiptdate", Type: DataTypeTime, Index: IndexDateTime}
	plans := SameRowComparisonPlans(QueryIR{
		Kind: QueryKindSelect,
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpLess, Field(commitDate), Field(receiptDate)),
			Placement: PredicateResidualScan,
			Scope:     PredicateScopeWhere,
		}},
	})

	execution := plans[0].ExecutionPlan([]QuantaRownum{10, 11, 12})

	if len(execution.Stages) != 3 {
		t.Fatalf("stages = %#v, want seed/compare/return", execution.Stages)
	}
	if execution.Stages[0].Kind != SameRowComparisonStageSeedCandidates ||
		execution.Stages[1].Kind != SameRowComparisonStageCompareBSIFields ||
		execution.Stages[2].Kind != SameRowComparisonStageReturnRownums {
		t.Fatalf("stage kinds = %#v, want seed compare return", execution.Stages)
	}
	compare := execution.Stages[1]
	if compare.Request.Kind != SameRowComparisonBSI || compare.Request.CandidateCount() != 3 {
		t.Fatalf("compare request = %#v, want BSI request over three rownums", compare.Request)
	}
	if compare.Request.Left.Name != "l_commitdate" || compare.Request.Right.Name != "l_receiptdate" || compare.Request.Operator != BinaryOpLess {
		t.Fatalf("compare fields = %#v, want commitdate < receiptdate", compare.Request)
	}
	if execution.Stages[0].Request.CandidateCount() != 0 || execution.Stages[2].Request.CandidateCount() != 0 {
		t.Fatalf("non-compare stages should not carry materialization/comparison requests: %#v", execution.Stages)
	}
}
