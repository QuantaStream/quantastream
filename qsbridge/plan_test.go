package qsbridge

import "testing"

func TestBuildLogicalPlanCreatesConventionalSingleTableShape(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipMode := FieldRef{Table: lineitem, Name: "l_shipmode", Index: IndexStringEnum}
	quantity := FieldRef{Table: lineitem, Name: "l_quantity", Index: IndexBSI}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{lineitem},
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpGreater, Field(quantity), Literal(ValueInt, 10)),
			Placement: PredicateResidualScan,
		}},
		GroupBy: []Expr{Field(shipMode)},
		Aggregates: []Aggregate{{
			Function: "count",
			Alias:    "line_count",
		}},
		Projection: []ProjectionColumn{{Expr: Field(shipMode)}},
		OrderBy:    []SortSpec{{Expr: AggregateRef("line_count", 0), Direction: SortDescending}},
		Result: ResultShape{
			Kind:    ResultQuery,
			Columns: []FieldRef{shipMode},
			Limit:   10,
		},
	}

	plan := BuildLogicalPlan(query)
	if plan.Root.NodeKind() != PlanNodeLimit {
		t.Fatalf("root kind = %q, want %q", plan.Root.NodeKind(), PlanNodeLimit)
	}

	kinds := collectPlanKinds(plan.Root)
	want := []PlanNodeKind{
		PlanNodeLimit,
		PlanNodeSort,
		PlanNodeProject,
		PlanNodeAggregate,
		PlanNodeFilter,
		PlanNodeScan,
	}
	assertPlanKinds(t, kinds, want)
}

func TestBuildLogicalPlanCreatesJoinTreeInSourceOrder(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Index: IndexBSI}
	lineOrderKey := FieldRef{Table: lineitem, Name: "l_orderkey", Index: IndexBSI}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders, lineitem},
		Joins: []JoinEdge{{
			Left:      lineOrderKey,
			Right:     orderKey,
			Direction: JoinChildToParent,
			Legal:     true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
	}

	plan := BuildLogicalPlan(query)
	project, ok := plan.Root.(ProjectNode)
	if !ok {
		t.Fatalf("root = %T, want ProjectNode", plan.Root)
	}
	join, ok := project.Input.(JoinNode)
	if !ok {
		t.Fatalf("project input = %T, want JoinNode", project.Input)
	}
	if join.Left.NodeKind() != PlanNodeScan || join.Right.NodeKind() != PlanNodeScan {
		t.Fatalf("expected scan children")
	}
}

func TestBuildLogicalPlanPreservesMembershipNode(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	orderCustKey := FieldRef{Table: orders, Name: "o_custkey"}
	customerKey := FieldRef{Table: customer, Name: "c_custkey"}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders},
		Memberships: []MembershipEdge{{
			Left:  orderCustKey,
			Right: customerKey,
			Kind:  MembershipAnti,
			Legal: true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(orderCustKey)}},
	}

	plan := BuildLogicalPlan(query)
	kinds := collectPlanKinds(plan.Root)
	want := []PlanNodeKind{
		PlanNodeProject,
		PlanNodeMembership,
		PlanNodeScan,
	}
	assertPlanKinds(t, kinds, want)

	project := plan.Root.(ProjectNode)
	membership := project.Input.(MembershipNode)
	if len(membership.Memberships) != 1 || membership.Memberships[0].Kind != MembershipAnti {
		t.Fatalf("membership node = %#v, want one anti membership", membership)
	}
}

func TestBuildLogicalPlanCarriesScalarSubqueryPlaceholder(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderPriority := FieldRef{Table: orders, Name: "o_orderpriority", Index: IndexStringEnum}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders},
		Subqueries: []SubqueryPlanIntent{{
			Kind:       SubqueryIntentScalar,
			Capability: CapabilityScalarSubquery,
			Scalar: &ScalarSubqueryIntent{
				SubquerySQL: "select count(*) from orders",
				OutputName:  "scalar_subquery_value",
				Scope:       PredicateScopeHaving,
			},
		}},
		GroupBy:    []Expr{Field(orderPriority)},
		Aggregates: []Aggregate{{Function: "count", Alias: "order_count"}},
		Projection: []ProjectionColumn{{Expr: Field(orderPriority)}},
	}

	plan := BuildLogicalPlan(query)
	kinds := collectPlanKinds(plan.Root)
	want := []PlanNodeKind{
		PlanNodeProject,
		PlanNodeAggregate,
		PlanNodeScalarSubquery,
		PlanNodeScan,
	}
	assertPlanKinds(t, kinds, want)

	project := plan.Root.(ProjectNode)
	aggregate := project.Input.(AggregateNode)
	scalar := aggregate.Input.(ScalarSubqueryNode)
	if got, want := scalar.ScalarOutputNames(), []string{"scalar_subquery_value"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("scalar outputs = %#v, want %#v", got, want)
	}
}

func TestBuildLogicalPlanCarriesCorrelatedAggregateSubqueryPlaceholder(t *testing.T) {
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	partKey := FieldRef{Table: part, Name: "p_partkey", Index: IndexBSI}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{part},
		Subqueries: []SubqueryPlanIntent{{
			Kind:       SubqueryIntentCorrelatedAggregate,
			Capability: CapabilityScalarSubquery,
			HelperIntents: []SubqueryHelperIntent{{
				Name:    "correlated_average_thresholds",
				Kind:    "aggregate_threshold_lookup",
				Outputs: []string{"p.p_partkey", "threshold"},
			}},
			CorrelatedAggregate: &CorrelatedAggregateSubqueryIntent{
				AggregateFunction: "avg",
				InnerKeyRef:       "l2.l_partkey",
				OuterKeyRef:       "p.p_partkey",
			},
		}},
		Projection: []ProjectionColumn{{Expr: Field(partKey)}},
	}

	plan := BuildLogicalPlan(query)
	kinds := collectPlanKinds(plan.Root)
	want := []PlanNodeKind{
		PlanNodeProject,
		PlanNodeCorrelatedAggregateSubquery,
		PlanNodeScan,
	}
	assertPlanKinds(t, kinds, want)

	project := plan.Root.(ProjectNode)
	correlated := project.Input.(CorrelatedAggregateSubqueryNode)
	if got, want := correlated.CorrelatedAggregateFunctions(), []string{"avg"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("correlated aggregate functions = %#v, want %#v", got, want)
	}
	inner, outer := correlated.CorrelatedAggregateKeyRefs()
	if len(inner) != 1 || inner[0] != "l2.l_partkey" || len(outer) != 1 || outer[0] != "p.p_partkey" {
		t.Fatalf("correlated key refs inner=%#v outer=%#v", inner, outer)
	}
}

func TestBuildLogicalPlanCreatesStatementNode(t *testing.T) {
	query := QueryIR{
		Kind: QueryKindInsert,
		Result: ResultShape{
			Kind:      ResultStatement,
			Statement: StatementResult{AffectedRows: 2, LastInsertID: 10},
		},
		Mutation: MutationShape{
			Kind:   MutationInsert,
			Target: TableInstance{ID: "orders", Table: "orders"},
			Rows: []MutationRow{
				{Values: []Expr{Literal(ValueInt, 1)}},
				{Values: []Expr{Literal(ValueInt, 2)}},
			},
		},
	}

	plan := BuildLogicalPlan(query)
	statement, ok := plan.Root.(StatementNode)
	if !ok {
		t.Fatalf("root = %T, want StatementNode", plan.Root)
	}
	if statement.Kind != QueryKindInsert || statement.Result.AffectedRows != 2 || statement.Result.LastInsertID != 10 {
		t.Fatalf("statement node = %#v, want insert OK metadata", statement)
	}
	if statement.Mutation.Kind != MutationInsert || statement.Mutation.Target.Table != "orders" || len(statement.Mutation.Rows) != 2 {
		t.Fatalf("statement mutation = %#v, want insert mutation shape", statement.Mutation)
	}
}

func TestBuildLogicalPlanWrapsUnsupportedDiagnostics(t *testing.T) {
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	partKey := FieldRef{Table: part, Name: "p_partkey", Index: IndexBSI}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{part},
		Predicates: []Predicate{{
			Expr:        Binary(BinaryOpEqual, Field(partKey), Literal(ValueInt, 1)),
			Placement:   PredicateUnsupported,
			Unsupported: "unsupported predicate",
		}},
	}

	plan := BuildLogicalPlan(query)
	if plan.Root.NodeKind() != PlanNodeUnsupported {
		t.Fatalf("root kind = %q, want %q", plan.Root.NodeKind(), PlanNodeUnsupported)
	}
	diagnostics := LogicalPlanDiagnostics(plan.Root)
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected unsupported plan diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != DiagnosticUnsupportedPredicate {
		t.Fatalf("diagnostic code = %q, want %q", got, DiagnosticUnsupportedPredicate)
	}
}

func TestBuildLogicalPlanAssignsScanFieldsBySource(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Index: IndexBSI}
	lineOrderKey := FieldRef{Table: lineitem, Name: "l_orderkey", Index: IndexBSI}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders, lineitem},
		Joins: []JoinEdge{{
			Left:      lineOrderKey,
			Right:     orderKey,
			Direction: JoinChildToParent,
			Legal:     true,
		}},
		Projection: []ProjectionColumn{{Expr: Binary(BinaryOpEqual, Field(orderKey), Field(lineOrderKey))}},
	}

	plan := BuildLogicalPlan(query)
	var scans []ScanNode
	WalkLogicalPlan(plan.Root, func(node LogicalNode) bool {
		if scan, ok := node.(ScanNode); ok {
			scans = append(scans, scan)
		}
		return true
	})
	if len(scans) != 2 {
		t.Fatalf("found %d scans, want 2", len(scans))
	}
	if len(scans[0].Fields) != 1 || scans[0].Fields[0].QualifiedName() != "o.o_orderkey" {
		t.Fatalf("unexpected first scan fields: %#v", scans[0].Fields)
	}
	if len(scans[1].Fields) != 1 || scans[1].Fields[0].QualifiedName() != "l.l_orderkey" {
		t.Fatalf("unexpected second scan fields: %#v", scans[1].Fields)
	}
}

func TestWalkLogicalPlanCanStopTraversal(t *testing.T) {
	root := FilterNode{Input: ScanNode{Source: TableInstance{ID: "customer", Table: "customer"}}}
	var visited []PlanNodeKind
	WalkLogicalPlan(root, func(node LogicalNode) bool {
		visited = append(visited, node.NodeKind())
		return false
	})

	assertPlanKinds(t, visited, []PlanNodeKind{PlanNodeFilter})
}

func collectPlanKinds(root LogicalNode) []PlanNodeKind {
	kinds := make([]PlanNodeKind, 0)
	WalkLogicalPlan(root, func(node LogicalNode) bool {
		kinds = append(kinds, node.NodeKind())
		return true
	})
	return kinds
}

func assertPlanKinds(t *testing.T, got []PlanNodeKind, want []PlanNodeKind) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got kinds %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got kinds %#v, want %#v", got, want)
		}
	}
}
