package qsbridge

import "testing"

func TestExplainLogicalPlanReturnsStructuredNodesAndText(t *testing.T) {
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

	explanation := ExplainLogicalPlan(BuildLogicalPlan(query))
	if !explanation.Supported {
		t.Fatalf("expected supported explanation")
	}
	if len(explanation.Nodes) != 6 {
		t.Fatalf("node count = %d, want 6", len(explanation.Nodes))
	}
	wantText := "limit(limit=10,offset=0) -> sort(keys=1) -> project(columns=1,hidden=0) -> aggregate(group=1,aggregates=1,having=0) -> filter(predicates=1,pushdown=0,residual_scan=1,residual_join=0) -> scan(l,fields=2)"
	if got := explanation.Text(); got != wantText {
		t.Fatalf("Text() = %q, want %q", got, wantText)
	}

	filter := explanation.Nodes[4]
	if filter.Kind != PlanNodeFilter {
		t.Fatalf("node[4] kind = %q, want filter", filter.Kind)
	}
	if filter.Predicates.ResidualScan != 1 || filter.Predicates.Total != 1 {
		t.Fatalf("filter predicates = %#v, want one residual scan predicate", filter.Predicates)
	}

	scan := explanation.Nodes[5]
	if scan.Source != "l" {
		t.Fatalf("scan source = %q, want l", scan.Source)
	}
	wantFields := []string{"l.l_quantity", "l.l_shipmode"}
	if len(scan.Fields) != len(wantFields) {
		t.Fatalf("scan fields = %#v, want %#v", scan.Fields, wantFields)
	}
	for i, want := range wantFields {
		if scan.Fields[i] != want {
			t.Fatalf("scan.Fields[%d] = %q, want %q", i, scan.Fields[i], want)
		}
	}
}

func TestExplainLogicalPlanIncludesScalarSubqueryPlaceholderOutputs(t *testing.T) {
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
				OutputName:  "scalar_order_count",
				Scope:       PredicateScopeHaving,
			},
		}},
		GroupBy:    []Expr{Field(orderPriority)},
		Aggregates: []Aggregate{{Function: "count", Alias: "order_count"}},
		Projection: []ProjectionColumn{{Expr: Field(orderPriority)}},
	}

	explanation := ExplainLogicalPlan(BuildLogicalPlan(query))
	if got := explanation.Text(); got != "project(columns=1,hidden=0) -> aggregate(group=1,aggregates=1,having=0) -> scalar_subquery(outputs=1) -> scan(o,fields=1)" {
		t.Fatalf("Text() = %q, want scalar placeholder in logical spine", got)
	}
	scalar := explanation.Nodes[2]
	if scalar.Kind != PlanNodeScalarSubquery {
		t.Fatalf("node[2] kind = %q, want scalar subquery", scalar.Kind)
	}
	if scalar.ScalarSubquery.Total != 1 {
		t.Fatalf("scalar summary = %#v, want one placeholder", scalar.ScalarSubquery)
	}
	if len(scalar.ScalarSubquery.OutputNames) != 1 || scalar.ScalarSubquery.OutputNames[0] != "scalar_order_count" {
		t.Fatalf("scalar output names = %#v, want scalar_order_count", scalar.ScalarSubquery.OutputNames)
	}
	if len(scalar.ScalarSubquery.HelperPlans) != 1 || scalar.ScalarSubquery.HelperPlans[0].Kind != SubqueryHelperPlanScalarSubquery {
		t.Fatalf("scalar helper plans = %#v, want scalar helper sketch", scalar.ScalarSubquery.HelperPlans)
	}
	if scalar.ScalarSubquery.HelperPlans[0].Lifecycle != SubqueryStepNativeReady || scalar.ScalarSubquery.HelperPlans[0].NativeStep == nil {
		t.Fatalf("scalar helper lifecycle/native step = %#v", scalar.ScalarSubquery.HelperPlans[0])
	}
	if len(scalar.ScalarSubquery.NativeSteps) != 1 || scalar.ScalarSubquery.NativeSteps[0].Kind != NativeSubqueryStepScalarMaterialization {
		t.Fatalf("scalar native steps = %#v, want scalar materialization", scalar.ScalarSubquery.NativeSteps)
	}

	report := InspectQuery(query, PhysicalScope{})
	if len(report.Logical.Nodes) != len(explanation.Nodes) {
		t.Fatalf("inspection logical nodes = %#v, want explain node count", report.Logical.Nodes)
	}
	if report.Logical.Nodes[2].ScalarSubquery.Total != 1 {
		t.Fatalf("inspection scalar summary = %#v, want one placeholder", report.Logical.Nodes[2].ScalarSubquery)
	}
}

func TestExplainLogicalPlanIncludesCorrelatedAggregateSubqueryPlaceholder(t *testing.T) {
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

	explanation := ExplainLogicalPlan(BuildLogicalPlan(query))
	if got := explanation.Text(); got != "project(columns=1,hidden=0) -> correlated_aggregate_subquery(intents=1,helpers=1) -> scan(p,fields=1)" {
		t.Fatalf("Text() = %q, want correlated aggregate placeholder in logical spine", got)
	}
	correlated := explanation.Nodes[1]
	if correlated.Kind != PlanNodeCorrelatedAggregateSubquery {
		t.Fatalf("node[1] kind = %q, want correlated aggregate subquery", correlated.Kind)
	}
	if correlated.CorrelatedAggregate.Total != 1 {
		t.Fatalf("correlated summary = %#v, want one placeholder", correlated.CorrelatedAggregate)
	}
	if len(correlated.CorrelatedAggregate.AggregateFunctions) != 1 || correlated.CorrelatedAggregate.AggregateFunctions[0] != "avg" {
		t.Fatalf("correlated aggregate functions = %#v, want avg", correlated.CorrelatedAggregate.AggregateFunctions)
	}
	if len(correlated.CorrelatedAggregate.HelperKinds) != 1 || correlated.CorrelatedAggregate.HelperKinds[0] != "aggregate_threshold_lookup" {
		t.Fatalf("correlated helper kinds = %#v, want aggregate threshold helper", correlated.CorrelatedAggregate.HelperKinds)
	}
	if len(correlated.CorrelatedAggregate.HelperPlans) != 1 || correlated.CorrelatedAggregate.HelperPlans[0].Kind != SubqueryHelperPlanAggregateThresholdLookup {
		t.Fatalf("correlated helper plans = %#v, want aggregate threshold helper sketch", correlated.CorrelatedAggregate.HelperPlans)
	}
}

func TestExplainOptionsDefaultAndVerboseSections(t *testing.T) {
	if !(ExplainOptions{}).Empty() {
		t.Fatalf("zero explain options should be empty before defaults")
	}
	defaults := (ExplainOptions{}).Effective()
	if !defaults.IncludeLogical || defaults.IncludePhysical || defaults.IncludeOptimizer {
		t.Fatalf("default options = %#v, want compact logical explain", defaults)
	}
	if !(ExplainOptions{}).Includes(ExplainSectionLogical) {
		t.Fatalf("zero options should include logical section after defaults")
	}
	if (ExplainOptions{}).Includes(ExplainSectionPhysical) {
		t.Fatalf("zero options should not include physical section")
	}

	verbose := VerboseExplainOptions()
	for _, section := range []ExplainSection{
		ExplainSectionLogical,
		ExplainSectionPhysical,
		ExplainSectionOptimizer,
		ExplainSectionOptimizerSummary,
		ExplainSectionDiagnostics,
		ExplainSectionFunctions,
		ExplainSectionNativeBlockers,
	} {
		if !verbose.Includes(section) {
			t.Fatalf("verbose options should include %s", section)
		}
	}
	if got, want := len(verbose.Sections()), 7; got != want {
		t.Fatalf("verbose sections = %d, want %d", got, want)
	}
}

func TestExplainInspectionReportSelectsRequestedSections(t *testing.T) {
	report := sampleInspectionReportForExplainOptions()

	compact := ExplainInspectionReport(report, ExplainOptions{})
	if !compact.Supported {
		t.Fatalf("compact bundle = %#v, want supported", compact)
	}
	if len(compact.Sections) != 1 || compact.Sections[0] != ExplainSectionLogical {
		t.Fatalf("compact sections = %#v, want logical only", compact.Sections)
	}
	if compact.AccessIntent != PhysicalAccessRead {
		t.Fatalf("compact access intent = %q, want read", compact.AccessIntent)
	}
	if compact.Lifecycle != ClientPlanLifecycleSelect || compact.LifecycleSteps != 7 {
		t.Fatalf("compact lifecycle = %q/%d, want select lifecycle", compact.Lifecycle, compact.LifecycleSteps)
	}
	if len(compact.Logical.Nodes) != 1 {
		t.Fatalf("compact logical = %#v, want logical node", compact.Logical)
	}
	if len(compact.Physical.Nodes) != 0 || len(compact.Optimization.Rewrites) != 0 || len(compact.Diagnostics) != 0 {
		t.Fatalf("compact bundle included unrequested sections: %#v", compact)
	}

	verbose := ExplainInspectionReport(report, VerboseExplainOptions())
	if len(verbose.Sections) != 7 {
		t.Fatalf("verbose sections = %#v, want all sections", verbose.Sections)
	}
	if len(verbose.Logical.Nodes) != 1 || len(verbose.Physical.Nodes) != 1 {
		t.Fatalf("verbose plan nodes = logical:%#v physical:%#v, want both", verbose.Logical.Nodes, verbose.Physical.Nodes)
	}
	if len(verbose.Optimization.Rewrites) != 1 || verbose.Optimization.Rewrites[0].Rule != RewritePredicatePushdown {
		t.Fatalf("verbose optimization = %#v, want predicate pushdown rewrite", verbose.Optimization)
	}
	if !verbose.OptimizationSummary.Supported || verbose.OptimizationSummary.Total != 1 || verbose.OptimizationSummary.Applied != 1 {
		t.Fatalf("verbose optimizer summary = %#v, want applied rewrite summary", verbose.OptimizationSummary)
	}
	if len(verbose.Diagnostics) != 1 || verbose.Diagnostics[0].Code != DiagnosticNativeBlocker {
		t.Fatalf("verbose diagnostics = %#v, want native blocker diagnostic", verbose.Diagnostics)
	}
	if len(verbose.FunctionUsages) != 1 || verbose.FunctionUsages[0].Name != "topn" {
		t.Fatalf("verbose functions = %#v, want topn usage", verbose.FunctionUsages)
	}
	if len(verbose.NativeBlockers) != 1 || verbose.NativeBlockers[0].Reason != "blocked" {
		t.Fatalf("verbose blockers = %#v, want native blocker", verbose.NativeBlockers)
	}
}

func TestExplainInspectionReportReturnsIndependentSectionCopies(t *testing.T) {
	report := sampleInspectionReportForExplainOptions()
	bundle := ExplainInspectionReport(report, VerboseExplainOptions())

	bundle.Logical.Nodes[0].Summary = "mutated"
	bundle.Physical.Nodes[0].Summary = "mutated"
	bundle.Optimization.Rewrites[0].Reason = "mutated"
	bundle.Diagnostics[0].Message = "mutated"
	bundle.FunctionUsages[0].Name = "mutated"
	bundle.NativeBlockers[0].Reason = "mutated"

	again := ExplainInspectionReport(report, VerboseExplainOptions())
	if again.Logical.Nodes[0].Summary != "logical" || again.Physical.Nodes[0].Summary != "physical" {
		t.Fatalf("plan explanation mutation leaked: %#v/%#v", again.Logical.Nodes, again.Physical.Nodes)
	}
	if again.Optimization.Rewrites[0].Reason != "moved predicate" {
		t.Fatalf("optimization mutation leaked: %#v", again.Optimization.Rewrites)
	}
	if again.Diagnostics[0].Message != "blocked" {
		t.Fatalf("diagnostics mutation leaked: %#v", again.Diagnostics)
	}
	if again.FunctionUsages[0].Name != "topn" {
		t.Fatalf("function usage mutation leaked: %#v", again.FunctionUsages)
	}
	if again.NativeBlockers[0].Reason != "blocked" {
		t.Fatalf("native blocker mutation leaked: %#v", again.NativeBlockers)
	}
}

func sampleInspectionReportForExplainOptions() InspectionReport {
	trace := NewOptimizationTrace()
	trace.Add(RewriteAppliedRecord(RewritePredicatePushdown, "moved predicate", "filter(residual)", "filter(pushdown)"))
	return InspectionReport{
		Supported: true,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticNativeBlocker, PhaseClassify, "blocked"),
		},
		Optimization: trace,
		Logical: PlanExplanation{
			Supported: true,
			Nodes: []PlanNodeExplanation{{
				ID:      1,
				Kind:    PlanNodeScan,
				Summary: "logical",
			}},
		},
		Physical: PhysicalPlanExplanation{
			Supported: true,
			Nodes: []PhysicalNodeExplanation{{
				ID:      1,
				Kind:    PhysicalNodeScan,
				Summary: "physical",
			}},
		},
		Query: QueryInspection{
			Kind: QueryKindSelect,
			FunctionUsages: []FunctionUsage{{
				Name:      "topn",
				Origin:    FunctionOriginQuantaCustom,
				Placement: FunctionPlacementAggregate,
				Context:   FunctionUsageAggregate,
			}},
			Blockers: []NativeBlocker{{
				Code:   DiagnosticNativeBlocker,
				Reason: "blocked",
				Phase:  PhaseClassify,
			}},
		},
	}
}

func TestExplainLogicalPlanIncludesPredicateCapabilities(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipMode := FieldRef{
		Table: lineitem,
		Name:  "l_shipmode",
		Index: IndexStringEnum,
		Dictionary: DictionaryDefinition{
			Ref:          DictionaryRef{Table: "lineitem", Field: "l_shipmode"},
			Version:      "v1",
			Capabilities: DictionaryCapabilities{DictionaryCapabilityPrefixMatch},
		},
	}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{lineitem},
		Predicates: []Predicate{{
			Expr:         Binary(BinaryOpLike, Field(shipMode), Literal(ValueString, "AIR%")),
			Placement:    PredicatePushdown,
			Capabilities: []PlanCapability{CapabilityBitmapPushdown},
		}},
		Projection: []ProjectionColumn{{Expr: Field(shipMode)}},
		Result:     ResultShape{Kind: ResultQuery, Columns: []FieldRef{shipMode}},
	}

	explanation := ExplainLogicalPlan(BuildLogicalPlan(query))
	if !explanation.HasCapability(CapabilityStringEnumPrefixLike) {
		t.Fatalf("explanation capabilities = %#v, want StringEnum prefix LIKE", explanation.Capabilities)
	}
	var filter PlanNodeExplanation
	for _, node := range explanation.Nodes {
		if node.Kind == PlanNodeFilter {
			filter = node
			break
		}
	}
	if filter.Kind != PlanNodeFilter {
		t.Fatalf("expected filter node in explanation")
	}
	if !predicateSummaryHasCapability(filter.Predicates, CapabilityBitmapPushdown) {
		t.Fatalf("predicate capabilities = %#v, want bitmap pushdown", filter.Predicates.Capabilities)
	}
	if !predicateSummaryHasCapability(filter.Predicates, CapabilityStringEnumPrefixLike) {
		t.Fatalf("predicate capabilities = %#v, want StringEnum prefix LIKE", filter.Predicates.Capabilities)
	}
	if filter.Predicates.Pushdown != 1 {
		t.Fatalf("filter predicates = %#v, want one pushdown predicate", filter.Predicates)
	}
}

func TestExplainLogicalPlanIncludesEncodingPredicateCapabilities(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	quantity := FieldRef{
		Table: lineitem,
		Name:  "l_quantity",
		Encoding: EncodingProfile{
			Kind: EncodingNumericBSI,
			PredicateCapabilities: PredicateCapabilities{
				PredicateCapabilityRange,
			},
		},
	}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{lineitem},
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpGreater, Field(quantity), Literal(ValueInt, 10)),
			Placement: PredicatePushdown,
		}},
		Projection: []ProjectionColumn{{Expr: Field(quantity)}},
		Result:     ResultShape{Kind: ResultQuery, Columns: []FieldRef{quantity}},
	}

	explanation := ExplainLogicalPlan(BuildLogicalPlan(query))
	if !explanation.HasCapability(CapabilityEncodingRange) {
		t.Fatalf("explanation capabilities = %#v, want encoding range", explanation.Capabilities)
	}
	var filter PlanNodeExplanation
	for _, node := range explanation.Nodes {
		if node.Kind == PlanNodeFilter {
			filter = node
			break
		}
	}
	if filter.Kind != PlanNodeFilter {
		t.Fatalf("expected filter node in explanation")
	}
	if !predicateSummaryHasCapability(filter.Predicates, CapabilityEncodingRange) {
		t.Fatalf("predicate capabilities = %#v, want encoding range", filter.Predicates.Capabilities)
	}
}

func TestExplainLogicalPlanIncludesJoinPredicates(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Index: IndexBSI}
	lineOrderKey := FieldRef{Table: lineitem, Name: "l_orderkey", Index: IndexBSI}
	quantity := FieldRef{Table: lineitem, Name: "l_quantity", Index: IndexBSI}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders, lineitem},
		Joins: []JoinEdge{{
			Left:      lineOrderKey,
			Right:     orderKey,
			Kind:      JoinKindInner,
			On:        []Predicate{{Expr: Binary(BinaryOpGreater, Field(quantity), Literal(ValueInt, 0)), Placement: PredicateResidualJoin, Scope: PredicateScopeOn}},
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
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
	}

	explanation := ExplainLogicalPlan(BuildLogicalPlan(query))
	wantText := "project(columns=1,hidden=0) -> join(inner,l.l_orderkey=o.o_orderkey,on=1) -> scan(o,fields=1) -> scan(l,fields=2)"
	if got := explanation.Text(); got != wantText {
		t.Fatalf("Text() = %q, want %q", got, wantText)
	}
	if len(explanation.Nodes) != 4 {
		t.Fatalf("node count = %d, want 4", len(explanation.Nodes))
	}
	join := explanation.Nodes[1]
	if join.Join.Left != "l.l_orderkey" || join.Join.Right != "o.o_orderkey" {
		t.Fatalf("join summary = %#v, want lineitem to orders edge", join.Join)
	}
	if join.Join.On.ResidualJoin != 1 {
		t.Fatalf("join ON predicates = %#v, want one residual join predicate", join.Join.On)
	}
	if !explainPlanCapabilitiesContain(join.Join.Capabilities, CapabilityRelationshipParentLookup) {
		t.Fatalf("join capabilities = %#v, want relationship parent lookup", join.Join.Capabilities)
	}
	if !explainPlanCapabilitiesContain(join.Join.Capabilities, CapabilityRelationshipJoinReduction) {
		t.Fatalf("join capabilities = %#v, want relationship join reduction", join.Join.Capabilities)
	}
}

func TestExplainLogicalPlanIncludesMembershipSummary(t *testing.T) {
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
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{
					RelationshipCapabilityAntiJoinDifference,
				},
			},
			Legal: true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(orderCustKey)}},
	}

	explanation := ExplainLogicalPlan(BuildLogicalPlan(query))
	wantText := "project(columns=1,hidden=0) -> membership(total=1,semi=0,anti=1) -> scan(o,fields=1)"
	if got := explanation.Text(); got != wantText {
		t.Fatalf("Text() = %q, want %q", got, wantText)
	}
	for _, node := range explanation.Nodes {
		if node.Kind != PlanNodeMembership {
			continue
		}
		if node.Membership.Total != 1 || node.Membership.Anti != 1 || node.Membership.Legal != 1 {
			t.Fatalf("membership summary = %#v, want one legal anti membership", node.Membership)
		}
		if !explainPlanCapabilitiesContain(node.Membership.Capabilities, CapabilityAntiMembership) {
			t.Fatalf("membership capabilities = %#v, want anti membership", node.Membership.Capabilities)
		}
		if !explainPlanCapabilitiesContain(node.Membership.Capabilities, CapabilityRelationshipAntiJoinDifference) {
			t.Fatalf("membership capabilities = %#v, want relationship anti-join difference", node.Membership.Capabilities)
		}
		return
	}
	t.Fatalf("expected membership node")
}

func TestExplainPlansIncludeStatementSummary(t *testing.T) {
	query := QueryIR{
		Kind: QueryKindInsert,
		Result: ResultShape{
			Kind:      ResultStatement,
			Statement: StatementResult{AffectedRows: 3, LastInsertID: 99, Status: "Records: 3"},
		},
	}
	logical := ExplainLogicalPlan(BuildLogicalPlan(query))
	if got, want := logical.Text(), "statement(insert,affected=3,warnings=0)"; got != want {
		t.Fatalf("logical Text() = %q, want %q", got, want)
	}
	if len(logical.Nodes) != 1 || logical.Nodes[0].Statement.LastInsertID != 99 {
		t.Fatalf("logical statement summary = %#v, want insert id", logical.Nodes)
	}

	physical := ExplainPhysicalPlan(BuildPhysicalPlan(BuildLogicalPlan(query), PhysicalScope{}))
	if len(physical.Nodes) != 1 || physical.Nodes[0].Statement.AffectedRows != 3 {
		t.Fatalf("physical statement summary = %#v, want affected rows", physical.Nodes)
	}
}

func TestExplainPlansIncludeMutationSummary(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	query := QueryIR{
		Kind: QueryKindDelete,
		Result: ResultShape{
			Kind:      ResultStatement,
			Statement: StatementResult{AffectedRows: 1},
		},
		Mutation: MutationShape{
			Kind:   MutationDelete,
			Target: orders,
			Predicates: []Predicate{{
				Expr:      Binary(BinaryOpEqual, Field(orderKey), Literal(ValueInt, 1)),
				Placement: PredicatePushdown,
				Scope:     PredicateScopeWhere,
			}},
		},
	}

	logical := ExplainLogicalPlan(BuildLogicalPlan(query))
	wantLogical := "statement(delete,affected=1,warnings=0,mutation=delete,rows=0,assignments=0,predicates=1)"
	if got := logical.Text(); got != wantLogical {
		t.Fatalf("logical Text() = %q, want %q", got, wantLogical)
	}
	if logical.Nodes[0].Statement.Mutation.Kind != MutationDelete || logical.Nodes[0].Statement.Mutation.Predicates != 1 {
		t.Fatalf("logical mutation summary = %#v, want delete predicate", logical.Nodes[0].Statement.Mutation)
	}

	physical := ExplainPhysicalPlan(BuildPhysicalPlan(BuildLogicalPlan(query), PhysicalScope{Placement: PlacementPrimary}))
	wantPhysical := "physical_statement(delete,affected=1,warnings=0,mutation=delete,rows=0,assignments=0,predicates=1,shards=0,placement=primary)"
	if got := physical.Text(); got != wantPhysical {
		t.Fatalf("physical Text() = %q, want %q", got, wantPhysical)
	}
	if physical.Nodes[0].Statement.Mutation.Target != "quanta.orders" {
		t.Fatalf("physical mutation summary = %#v, want quanta.orders target", physical.Nodes[0].Statement.Mutation)
	}
}

func TestExplainLogicalPlanCarriesUnsupportedDiagnostics(t *testing.T) {
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

	explanation := ExplainLogicalPlan(BuildLogicalPlan(query))
	if explanation.Supported {
		t.Fatalf("expected unsupported explanation")
	}
	if len(explanation.Diagnostics) != 1 || explanation.Diagnostics[0].Code != DiagnosticUnsupportedPredicate {
		t.Fatalf("diagnostics = %#v, want unsupported predicate", explanation.Diagnostics)
	}
	if explanation.Nodes[0].Kind != PlanNodeUnsupported {
		t.Fatalf("root kind = %q, want unsupported", explanation.Nodes[0].Kind)
	}
	if len(explanation.Nodes[0].Diagnostics) != 1 || explanation.Nodes[0].Diagnostics[0] != DiagnosticUnsupportedPredicate {
		t.Fatalf("root diagnostics = %#v, want unsupported predicate", explanation.Nodes[0].Diagnostics)
	}
}

func TestExplainPhysicalPlanReturnsScopeAndText(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipMode := FieldRef{Table: lineitem, Name: "l_shipmode", Index: IndexStringEnum}
	query := QueryIR{
		Kind:       QueryKindSelect,
		Sources:    []TableInstance{lineitem},
		Projection: []ProjectionColumn{{Expr: Field(shipMode)}},
		Result: ResultShape{
			Kind:    ResultQuery,
			Columns: []FieldRef{shipMode},
			Limit:   5,
		},
	}
	logical := BuildLogicalPlan(query)
	scope := PhysicalScope{
		Shards:    ShardSet{Shards: []ShardID{"lineitem-0001", "lineitem-0002"}},
		Replicas:  []ReplicaID{"replica-a"},
		Routing:   "l_shipmode",
		Placement: PlacementLocal,
		Cache:     CacheQuery,
	}

	explanation := ExplainPhysicalPlan(BuildPhysicalPlan(logical, scope))
	if !explanation.Supported {
		t.Fatalf("expected supported physical explanation")
	}
	if len(explanation.Nodes) != 3 {
		t.Fatalf("node count = %d, want 3", len(explanation.Nodes))
	}
	wantText := "physical_limit(shards=2,replicas=1,routing=l_shipmode,placement=local,cache=query) -> physical_project(shards=2,replicas=1,routing=l_shipmode,placement=local,cache=query) -> physical_scan(l,fields=1,shards=2,replicas=1,routing=l_shipmode,placement=local,cache=query)"
	if got := explanation.Text(); got != wantText {
		t.Fatalf("Text() = %q, want %q", got, wantText)
	}

	scan := explanation.Nodes[2]
	if scan.Kind != PhysicalNodeScan {
		t.Fatalf("scan kind = %q, want physical scan", scan.Kind)
	}
	if got := scan.Scope.Shards; len(got) != 2 || got[0] != "lineitem-0001" || got[1] != "lineitem-0002" {
		t.Fatalf("scan scope shards = %#v, want two explicit shards", got)
	}
	if scan.Scope.Placement != PlacementLocal || scan.Scope.Cache != CacheQuery {
		t.Fatalf("scan scope = %#v, want local query scope", scan.Scope)
	}
}

func TestExplainPhysicalPlanIncludesFilterStrategies(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	name := FieldRef{
		Table: lineitem,
		Name:  "l_comment",
		Encoding: EncodingProfile{
			Kind: EncodingStringLexBSI,
			PredicateCapabilities: PredicateCapabilities{
				PredicateCapabilityPrefix,
			},
		},
	}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{lineitem},
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpLike, Field(name), Literal(ValueString, "green%")),
			Placement: PredicatePushdown,
		}},
		Projection: []ProjectionColumn{{Expr: Field(name)}},
		Result:     ResultShape{Kind: ResultQuery, Columns: []FieldRef{name}},
	}

	explanation := ExplainPhysicalPlan(BuildPhysicalPlan(BuildLogicalPlan(query), PhysicalScope{}))
	for _, node := range explanation.Nodes {
		if node.Kind != PhysicalNodeFilter {
			continue
		}
		if !physicalStrategiesContain(node.Strategies, PhysicalStrategyEncodingPrefix) {
			t.Fatalf("strategies = %#v, want encoding prefix", node.Strategies)
		}
		return
	}
	t.Fatalf("expected physical filter node")
}

func TestExplainPhysicalPlanIncludesMembershipStrategies(t *testing.T) {
	partsupp := TableInstance{ID: "partsupp", Table: "partsupp", Alias: "ps"}
	supplier := TableInstance{ID: "supplier", Table: "supplier", Alias: "s"}
	partSuppKey := FieldRef{Table: partsupp, Name: "ps_suppkey"}
	suppKey := FieldRef{Table: supplier, Name: "s_suppkey"}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{partsupp},
		Memberships: []MembershipEdge{{
			Left:  partSuppKey,
			Right: suppKey,
			Kind:  MembershipAnti,
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{
					RelationshipCapabilityAntiJoinDifference,
				},
			},
			Legal: true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(partSuppKey)}},
	}

	explanation := ExplainPhysicalPlan(BuildPhysicalPlan(BuildLogicalPlan(query), PhysicalScope{}))
	for _, node := range explanation.Nodes {
		if node.Kind != PhysicalNodeMembership {
			continue
		}
		if !physicalStrategiesContain(node.Strategies, PhysicalStrategyRelationshipAntiJoinDifference) {
			t.Fatalf("strategies = %#v, want relationship anti-join difference", node.Strategies)
		}
		return
	}
	t.Fatalf("expected physical membership node")
}

func TestExplainPhysicalPlanIncludesRelationshipJoinStrategies(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders, lineitem},
		Joins: []JoinEdge{{
			Left:  FieldRef{Table: lineitem, Name: "l_orderkey"},
			Right: FieldRef{Table: orders, Name: "o_orderkey"},
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{
					RelationshipCapabilityParentLookup,
					RelationshipCapabilityAntiJoinDifference,
				},
			},
			Legal: true,
		}},
	}

	explanation := ExplainPhysicalPlan(BuildPhysicalPlan(BuildLogicalPlan(query), PhysicalScope{}))
	for _, node := range explanation.Nodes {
		if node.Kind != PhysicalNodeJoin {
			continue
		}
		if !physicalStrategiesContain(node.Strategies, PhysicalStrategyRelationshipParentLookup) {
			t.Fatalf("strategies = %#v, want relationship parent lookup", node.Strategies)
		}
		if !physicalStrategiesContain(node.Strategies, PhysicalStrategyRelationshipAntiJoinDifference) {
			t.Fatalf("strategies = %#v, want relationship anti-join difference", node.Strategies)
		}
		return
	}
	t.Fatalf("expected physical join node")
}

func TestExplainPhysicalPlanIncludesJoinAndDiagnostics(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	lineOrderKey := FieldRef{Table: lineitem, Name: "l_orderkey"}
	quantity := FieldRef{Table: lineitem, Name: "l_quantity"}
	logical := BuildLogicalPlan(QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders, lineitem},
		Joins: []JoinEdge{{
			Left:      lineOrderKey,
			Right:     orderKey,
			Kind:      JoinKindInner,
			On:        []Predicate{{Expr: Binary(BinaryOpGreater, Field(quantity), Literal(ValueInt, 0)), Placement: PredicateResidualJoin, Scope: PredicateScopeOn}},
			Direction: JoinChildToParent,
			Legal:     true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
	})

	explanation := ExplainPhysicalPlan(BuildPhysicalPlan(logical, PhysicalScope{Shards: ShardSet{All: true}, Placement: PlacementAny}))
	if len(explanation.Nodes) != 4 {
		t.Fatalf("node count = %d, want 4", len(explanation.Nodes))
	}
	join := explanation.Nodes[1]
	if join.Kind != PhysicalNodeJoin {
		t.Fatalf("node[1] kind = %q, want physical join", join.Kind)
	}
	if join.Join.Left != "l.l_orderkey" || join.Join.Right != "o.o_orderkey" {
		t.Fatalf("join summary = %#v, want lineitem to orders edge", join.Join)
	}
	if join.Join.On.ResidualJoin != 1 {
		t.Fatalf("join ON predicates = %#v, want one residual join predicate", join.Join.On)
	}
	if !join.Scope.AllShards {
		t.Fatalf("join scope = %#v, want all shards", join.Scope)
	}
}

func TestExplainPhysicalPlanCarriesUnsupportedDiagnostics(t *testing.T) {
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	partKey := FieldRef{Table: part, Name: "p_partkey"}
	logical := BuildLogicalPlan(QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{part},
		Predicates: []Predicate{{
			Expr:        Field(partKey),
			Placement:   PredicateUnsupported,
			Unsupported: "unsupported predicate",
		}},
	})

	explanation := ExplainPhysicalPlan(BuildPhysicalPlan(logical, PhysicalScope{}))
	if explanation.Supported {
		t.Fatalf("expected unsupported physical explanation")
	}
	if len(explanation.Diagnostics) != 1 || explanation.Diagnostics[0].Code != DiagnosticUnsupportedPredicate {
		t.Fatalf("diagnostics = %#v, want unsupported predicate", explanation.Diagnostics)
	}
	if len(explanation.Nodes[0].Diagnostics) != 1 || explanation.Nodes[0].Diagnostics[0] != DiagnosticUnsupportedPredicate {
		t.Fatalf("root diagnostics = %#v, want unsupported predicate", explanation.Nodes[0].Diagnostics)
	}
}

func predicateSummaryHasCapability(summary PredicateSummary, capability PlanCapability) bool {
	return explainPlanCapabilitiesContain(summary.Capabilities, capability)
}

func explainPlanCapabilitiesContain(capabilities []PlanCapability, capability PlanCapability) bool {
	for _, current := range capabilities {
		if current == capability {
			return true
		}
	}
	return false
}
