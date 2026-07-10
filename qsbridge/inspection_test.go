package qsbridge

import "testing"

func TestInspectQueryBundlesClassificationLogicalAndPhysicalExplain(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipMode := FieldRef{Table: lineitem, Name: "l_shipmode", Index: IndexStringEnum}
	quantity := FieldRef{Table: lineitem, Name: "l_quantity", Index: IndexBSI}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{lineitem},
		Predicates: []Predicate{{
			Expr:         Binary(BinaryOpGreater, Field(quantity), Literal(ValueInt, 10)),
			Placement:    PredicatePushdown,
			Capabilities: []PlanCapability{CapabilityBSIPushdown},
			Scope:        PredicateScopeWhere,
		}},
		Projection: []ProjectionColumn{{Expr: Field(shipMode)}},
		Result: ResultShape{
			Kind:    ResultQuery,
			Columns: []FieldRef{shipMode},
			Limit:   5,
		},
	}
	scope := PhysicalScope{
		Shards:    ShardSet{Shards: []ShardID{"lineitem-0001"}},
		Placement: PlacementLocal,
		Cache:     CacheQuery,
	}

	report := InspectQuery(query, scope)
	if !report.Supported {
		t.Fatalf("expected supported report")
	}
	if report.Query.Kind != QueryKindSelect {
		t.Fatalf("query kind = %q, want select", report.Query.Kind)
	}
	if len(report.Query.Sources) != 1 || report.Query.Sources[0] != "lineitem.as l" {
		t.Fatalf("sources = %#v, want lineitem alias", report.Query.Sources)
	}
	if report.Query.Predicates != 1 || report.Query.GroupBy != 0 || report.Query.OrderBy != 0 {
		t.Fatalf("query summary = %#v, want one predicate only", report.Query)
	}
	if !report.Classification.HasCapability(CapabilityBSIPushdown) {
		t.Fatalf("classification capabilities = %#v, want BSI pushdown", report.Classification.Capabilities)
	}
	if len(report.Logical.Nodes) == 0 {
		t.Fatalf("expected logical explanation nodes")
	}
	if len(report.Physical.Nodes) == 0 {
		t.Fatalf("expected physical explanation nodes")
	}
	if got := report.Physical.Nodes[0].Scope.Shards; len(got) != 1 || got[0] != "lineitem-0001" {
		t.Fatalf("physical root shards = %#v, want lineitem-0001", got)
	}
}

func TestInspectOptimizedQueryCarriesRewriteAudit(t *testing.T) {
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	partName := FieldRef{Table: part, Name: "p_name", Index: IndexBackingString}
	query := QueryIR{
		Kind:       QueryKindSelect,
		Sources:    []TableInstance{part},
		Projection: []ProjectionColumn{{Expr: Field(partName)}},
		Result:     ResultShape{Kind: ResultQuery, Columns: []FieldRef{partName}},
	}
	trace := NewOptimizationTrace()
	trace.Add(RewriteAdvisoryRecord(
		RewritePredicatePushdown,
		"backing string predicate may need a searchable index",
		partName,
	))

	report := InspectOptimizedQuery(query, trace, PhysicalScope{Placement: PlacementAny})
	if !report.Supported {
		t.Fatalf("expected advisory-only report to remain supported")
	}
	if len(report.Optimization.Rewrites) != 1 {
		t.Fatalf("rewrite count = %d, want one", len(report.Optimization.Rewrites))
	}
	if report.Optimization.Rewrites[0].Status != RewriteAdvisory {
		t.Fatalf("rewrite status = %q, want advisory", report.Optimization.Rewrites[0].Status)
	}
}

func TestInspectQueryExposesSubqueryIntentReports(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderPriority := FieldRef{Table: orders, Name: "o_orderpriority", Index: IndexStringEnum}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders},
		Subqueries: []SubqueryPlanIntent{{
			Kind:       SubqueryIntentScalar,
			Capability: CapabilityScalarSubquery,
			HelperIntents: []SubqueryHelperIntent{{
				Name:    "scalar_subquery_value",
				Kind:    "scalar_subquery",
				Outputs: []string{"scalar_subquery_value"},
			}},
			Scalar: &ScalarSubqueryIntent{
				SubquerySQL: "select count(*) from orders",
				OutputName:  "scalar_subquery_value",
				Scope:       PredicateScopeHaving,
			},
		}, {
			Kind:       SubqueryIntentCorrelatedAggregate,
			Capability: CapabilityScalarSubquery,
			HelperIntents: []SubqueryHelperIntent{{
				Name:    "correlated_average_thresholds",
				Kind:    "aggregate_threshold_lookup",
				Outputs: []string{"o.o_orderkey", "threshold"},
			}},
			CorrelatedAggregate: &CorrelatedAggregateSubqueryIntent{
				AggregateFunction: "avg",
				InnerKeyRef:       "l2.l_orderkey",
				OuterKeyRef:       "o.o_orderkey",
			},
		}},
		GroupBy:    []Expr{Field(orderPriority)},
		Aggregates: []Aggregate{{Function: "count", Alias: "order_count"}},
		Projection: []ProjectionColumn{{Expr: Field(orderPriority)}},
	}

	report := InspectQuery(query, PhysicalScope{})
	if got, want := len(report.Query.SubqueryIntents), 2; got != want {
		t.Fatalf("subquery intents = %d, want %d: %#v", got, want, report.Query.SubqueryIntents)
	}
	if report.Query.SubqueryIntents[0].Scalar == nil || report.Query.SubqueryIntents[0].Scalar.OutputName != "scalar_subquery_value" {
		t.Fatalf("scalar intent report = %#v", report.Query.SubqueryIntents[0])
	}
	if report.Query.SubqueryIntents[1].CorrelatedAggregate == nil || report.Query.SubqueryIntents[1].CorrelatedAggregate.AggregateFunction != "avg" {
		t.Fatalf("correlated intent report = %#v", report.Query.SubqueryIntents[1])
	}
	if got, want := len(report.Query.SubqueryHelperPlans), 2; got != want {
		t.Fatalf("subquery helper plans = %d, want %d: %#v", got, want, report.Query.SubqueryHelperPlans)
	}
	if report.Query.SubqueryHelperPlans[0].Kind != SubqueryHelperPlanAggregateThresholdLookup && report.Query.SubqueryHelperPlans[1].Kind != SubqueryHelperPlanAggregateThresholdLookup {
		t.Fatalf("subquery helper plans = %#v, want aggregate threshold helper sketch", report.Query.SubqueryHelperPlans)
	}
	if got, want := len(report.Query.NativeSubquerySteps), 2; got != want {
		t.Fatalf("native subquery steps = %d, want %d: %#v", got, want, report.Query.NativeSubquerySteps)
	}
	if report.Query.NativeSubquerySteps[0].Kind != NativeSubqueryStepAggregateThresholdLookup || report.Query.NativeSubquerySteps[1].Kind != NativeSubqueryStepScalarMaterialization {
		t.Fatalf("native subquery steps = %#v, want aggregate threshold then scalar", report.Query.NativeSubquerySteps)
	}
	if report.Query.NativeSubquerySteps[0].Lifecycle != SubqueryStepNativeReady || report.Query.NativeSubquerySteps[1].Lifecycle != SubqueryStepNativeReady {
		t.Fatalf("native subquery steps = %#v", report.Query.NativeSubquerySteps)
	}
}

func TestInspectQueryExposesResultColumnMetadata(t *testing.T) {
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	name := FieldRef{Table: customer, Name: "c_name", Type: DataTypeString, Nullable: false}
	acctbal := FieldRef{Table: customer, Name: "c_acctbal", Type: DataTypeFloat, Nullable: true}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{customer},
		Projection: []ProjectionColumn{
			{Expr: Field(name), Alias: "customer_name"},
			{Expr: Field(acctbal)},
		},
	}

	report := InspectQuery(query, PhysicalScope{})
	if len(report.Query.ResultColumns) != 2 {
		t.Fatalf("result columns = %#v, want two columns", report.Query.ResultColumns)
	}
	first := report.Query.ResultColumns[0]
	if first.Name != "customer_name" || first.Type != DataTypeString || first.Nullable || first.Source != "c.c_name" {
		t.Fatalf("first result column = %#v, want customer_name string source", first)
	}
	second := report.Query.ResultColumns[1]
	if second.Name != "c_acctbal" || second.Type != DataTypeFloat || !second.Nullable || second.Source != "c.c_acctbal" {
		t.Fatalf("second result column = %#v, want nullable c_acctbal float source", second)
	}
}

func TestInspectQueryExposesPreparedStatementParameters(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	totalPrice := FieldRef{Table: orders, Name: "o_totalprice", Type: DataTypeFloat}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders},
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpGreater, Field(totalPrice), Parameter(1, DataTypeFloat)),
			Placement: PredicatePushdown,
		}},
		Projection: []ProjectionColumn{{Expr: Field(totalPrice)}},
	}

	report := InspectQuery(query, PhysicalScope{})
	if len(report.Query.Parameters) != 1 {
		t.Fatalf("parameters = %#v, want one parameter", report.Query.Parameters)
	}
	parameter := report.Query.Parameters[0]
	if parameter.Index != 1 || parameter.Type != DataTypeFloat || !parameter.Nullable {
		t.Fatalf("parameter = %#v, want nullable float parameter 1", parameter)
	}
}

func TestInspectQueryExposesStatementResultMetadata(t *testing.T) {
	query := QueryIR{
		Kind: QueryKindDelete,
		Result: ResultShape{
			Kind:      ResultStatement,
			Statement: StatementResult{AffectedRows: 7, Warnings: 2, Status: "Rows matched: 7"},
		},
	}

	report := InspectQuery(query, PhysicalScope{})
	if len(report.Query.ResultColumns) != 0 {
		t.Fatalf("result columns = %#v, want none for statement", report.Query.ResultColumns)
	}
	if report.Query.Statement.AffectedRows != 7 || report.Query.Statement.Warnings != 2 || report.Query.Statement.Status != "Rows matched: 7" {
		t.Fatalf("statement metadata = %#v, want OK metadata", report.Query.Statement)
	}
}

func TestInspectQueryExposesMutationMetadata(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Type: DataTypeInt, Roles: FieldRoleMutationTarget}
	totalPrice := FieldRef{Table: orders, Name: "o_totalprice", Type: DataTypeFloat, Roles: FieldRoleMutationTarget}
	query := QueryIR{
		Kind: QueryKindInsert,
		Result: ResultShape{
			Kind:      ResultStatement,
			Statement: StatementResult{AffectedRows: 2, Status: "Records: 2"},
		},
		Mutation: MutationShape{
			Kind:    MutationInsert,
			Target:  orders,
			Columns: []FieldRef{orderKey, totalPrice},
			Rows: []MutationRow{
				{Values: []Expr{Literal(ValueInt, 1), Literal(ValueFloat, 10.5)}},
				{Values: []Expr{Literal(ValueInt, 2), Literal(ValueFloat, 11.5)}},
			},
		},
	}

	report := InspectQuery(query, PhysicalScope{})
	if report.Query.Mutation.Kind != MutationInsert {
		t.Fatalf("mutation kind = %q, want insert", report.Query.Mutation.Kind)
	}
	if report.Query.Mutation.Target != "quanta.orders" {
		t.Fatalf("mutation target = %q, want quanta.orders", report.Query.Mutation.Target)
	}
	if report.Query.Mutation.Rows != 2 {
		t.Fatalf("mutation rows = %d, want 2", report.Query.Mutation.Rows)
	}
	if len(report.Query.Mutation.Columns) != 2 || report.Query.Mutation.Columns[0] != "orders.o_orderkey" || report.Query.Mutation.Columns[1] != "orders.o_totalprice" {
		t.Fatalf("mutation columns = %#v, want order columns", report.Query.Mutation.Columns)
	}
	if len(report.Query.Fields) != 2 {
		t.Fatalf("required fields = %#v, want mutation columns", report.Query.Fields)
	}

	update := QueryIR{
		Kind: QueryKindUpdate,
		Result: ResultShape{
			Kind: ResultStatement,
		},
		Mutation: MutationShape{
			Kind:    MutationUpdate,
			Target:  orders,
			Columns: []FieldRef{totalPrice},
			Assignments: []MutationAssignment{{
				Field: totalPrice,
				Value: Literal(ValueFloat, 99.5),
			}},
			Predicates: []Predicate{{
				Expr:      Binary(BinaryOpEqual, Field(orderKey), Literal(ValueInt, 1)),
				Placement: PredicatePushdown,
				Scope:     PredicateScopeWhere,
			}},
		},
	}
	report = InspectQuery(update, PhysicalScope{})
	if report.Query.Mutation.Kind != MutationUpdate || report.Query.Mutation.Assignments != 1 || report.Query.Mutation.Predicates != 1 {
		t.Fatalf("update mutation = %#v, want one assignment and one predicate", report.Query.Mutation)
	}

	deleteQuery := QueryIR{
		Kind: QueryKindDelete,
		Result: ResultShape{
			Kind: ResultStatement,
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
	report = InspectQuery(deleteQuery, PhysicalScope{})
	if report.Query.Mutation.Kind != MutationDelete || report.Query.Mutation.Assignments != 0 || report.Query.Mutation.Predicates != 1 {
		t.Fatalf("delete mutation = %#v, want one predicate and no assignments", report.Query.Mutation)
	}
}

func TestInspectQueryDeduplicatesDiagnosticsAcrossPlanLayers(t *testing.T) {
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	partKey := FieldRef{Table: part, Name: "p_partkey"}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{part},
		Predicates: []Predicate{{
			Expr:        Field(partKey),
			Placement:   PredicateUnsupported,
			Unsupported: "unsupported predicate",
		}},
	}

	report := InspectQuery(query, PhysicalScope{})
	if report.Supported {
		t.Fatalf("expected unsupported report")
	}
	if len(report.Diagnostics) != 1 {
		t.Fatalf("diagnostics = %#v, want one deduplicated diagnostic", report.Diagnostics)
	}
	if report.Diagnostics[0].Code != DiagnosticUnsupportedPredicate {
		t.Fatalf("diagnostic code = %q, want unsupported predicate", report.Diagnostics[0].Code)
	}
	if report.Logical.Supported {
		t.Fatalf("expected logical explanation to be unsupported")
	}
	if report.Physical.Supported {
		t.Fatalf("expected physical explanation to be unsupported")
	}
}

func TestInspectQueryExposesStringEnumPredicateCapabilities(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipMode := FieldRef{
		Table: lineitem,
		Name:  "l_shipmode",
		Index: IndexStringEnum,
		Dictionary: DictionaryDefinition{
			Ref:          DictionaryRef{Table: "lineitem", Field: "l_shipmode"},
			Version:      "v1",
			Capabilities: DictionaryCapabilities{DictionaryCapabilityContainsMatch},
		},
	}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{lineitem},
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpLike, Field(shipMode), Literal(ValueString, "%AIR%")),
			Placement: PredicatePushdown,
		}},
		Projection: []ProjectionColumn{{Expr: Field(shipMode)}},
		Result:     ResultShape{Kind: ResultQuery, Columns: []FieldRef{shipMode}},
	}

	report := InspectQuery(query, PhysicalScope{Placement: PlacementAny})
	if !report.Classification.HasCapability(CapabilityStringEnumContainsLike) {
		t.Fatalf("classification capabilities = %#v, want StringEnum contains LIKE", report.Classification.Capabilities)
	}
	for _, node := range report.Logical.Nodes {
		if node.Kind != PlanNodeFilter {
			continue
		}
		if !predicateSummaryHasCapability(node.Predicates, CapabilityStringEnumContainsLike) {
			t.Fatalf("predicate capabilities = %#v, want StringEnum contains LIKE", node.Predicates.Capabilities)
		}
		return
	}
	t.Fatalf("expected filter node in logical explanation")
}

func TestInspectQueryExposesFieldEncodingSummaries(t *testing.T) {
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	phoneKind := FieldRef{
		Table: customer,
		Name:  "phone_kind",
		Index: IndexBitmap,
		Encoding: EncodingProfile{
			Kind:         EncodingBitmap,
			Multiplicity: MultiplicitySet,
			Rehydration:  RehydrationProfile{Kind: RehydrationInline},
			PredicateCapabilities: PredicateCapabilities{
				PredicateCapabilityEquality,
				PredicateCapabilityMembership,
			},
			ProjectionCapabilities: ProjectionCapabilities{
				ProjectionCapabilityInline,
			},
		},
	}
	comment := FieldRef{
		Table: customer,
		Name:  "comment",
		Index: IndexBackingString,
		Encoding: EncodingProfile{
			Kind:         EncodingBackingString,
			LegacyName:   "StringHashBSI",
			Multiplicity: MultiplicityScalar,
			Search:       SearchProfile{Enabled: true, Mode: "text"},
			Rehydration:  RehydrationProfile{Kind: RehydrationLookup, Store: "kv"},
			PredicateCapabilities: PredicateCapabilities{
				PredicateCapabilityEquality,
			},
			ProjectionCapabilities: ProjectionCapabilities{
				ProjectionCapabilityLookup,
				ProjectionCapabilityOriginalValue,
			},
		},
	}
	query := QueryIR{
		Kind:       QueryKindSelect,
		Sources:    []TableInstance{customer},
		Projection: []ProjectionColumn{{Expr: Field(phoneKind)}, {Expr: Field(comment)}},
		Result:     ResultShape{Kind: ResultQuery, Columns: []FieldRef{phoneKind, comment}},
	}

	report := InspectQuery(query, PhysicalScope{Placement: PlacementAny})
	if got, want := len(report.Query.FieldEncodings), 2; got != want {
		t.Fatalf("field encoding count = %d, want %d", got, want)
	}

	phoneSummary := fieldEncodingSummary(report.Query.FieldEncodings, "c.phone_kind")
	if phoneSummary.Kind != EncodingBitmap {
		t.Fatalf("phone kind encoding = %q, want bitmap", phoneSummary.Kind)
	}
	if phoneSummary.Multiplicity != MultiplicitySet {
		t.Fatalf("phone multiplicity = %q, want set", phoneSummary.Multiplicity)
	}
	if phoneSummary.RequiresLookup {
		t.Fatalf("inline bitmap projection should not require lookup")
	}

	commentSummary := fieldEncodingSummary(report.Query.FieldEncodings, "c.comment")
	if commentSummary.Kind != EncodingBackingString {
		t.Fatalf("comment encoding = %q, want backing string", commentSummary.Kind)
	}
	if !commentSummary.RequiresLookup || !commentSummary.Searchable {
		t.Fatalf("comment summary = %#v, want searchable lookup-backed string", commentSummary)
	}
	if !predicateCapabilitiesContain(commentSummary.PredicateCapabilities, PredicateCapabilityEquality) {
		t.Fatalf("predicate capabilities = %#v, want equality", commentSummary.PredicateCapabilities)
	}
}

func TestInspectQueryExposesMembershipEdges(t *testing.T) {
	partsupp := TableInstance{ID: "partsupp", Table: "partsupp", Alias: "ps"}
	supplier := TableInstance{ID: "supplier", Table: "supplier", Alias: "s"}
	partSuppKey := FieldRef{Table: partsupp, Name: "ps_suppkey"}
	suppKey := FieldRef{Table: supplier, Name: "s_suppkey"}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{partsupp},
		Memberships: []MembershipEdge{{
			Left:        partSuppKey,
			Right:       suppKey,
			Kind:        MembershipAnti,
			Direction:   JoinChildToParent,
			Cardinality: "many_to_one",
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{
					RelationshipCapabilityParentLookup,
					RelationshipCapabilityAntiJoinDifference,
				},
			},
			Legal: true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(partSuppKey)}},
	}

	report := InspectQuery(query, PhysicalScope{})
	if report.Query.Memberships != 1 {
		t.Fatalf("membership count = %d, want 1", report.Query.Memberships)
	}
	if len(report.Query.MembershipEdges) != 1 {
		t.Fatalf("membership summaries = %#v, want one edge", report.Query.MembershipEdges)
	}
	edge := report.Query.MembershipEdges[0]
	if edge.Kind != MembershipAnti || edge.Left != "ps.ps_suppkey" || edge.Right != "s.s_suppkey" {
		t.Fatalf("membership edge = %#v, want anti ps->s edge", edge)
	}
	if edge.EncodingKind != RelationshipEncodingVector {
		t.Fatalf("encoding kind = %q, want relation vector", edge.EncodingKind)
	}
	if !relationshipCapabilitiesContain(edge.Capabilities, RelationshipCapabilityAntiJoinDifference) {
		t.Fatalf("relationship capabilities = %#v, want anti-join difference", edge.Capabilities)
	}
}

func TestInspectQueryExposesJoinEdges(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	orderCustKey := FieldRef{Table: orders, Name: "o_custkey"}
	customerKey := FieldRef{Table: customer, Name: "c_custkey"}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders, customer},
		Joins: []JoinEdge{{
			Left:        orderCustKey,
			Right:       customerKey,
			Kind:        JoinKindInner,
			Direction:   JoinChildToParent,
			Cardinality: "many_to_one",
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{
					RelationshipCapabilityParentLookup,
					RelationshipCapabilityJoinReduction,
				},
			},
			Legal: true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(orderCustKey)}},
	}

	report := InspectQuery(query, PhysicalScope{})
	if report.Query.Joins != 1 {
		t.Fatalf("join count = %d, want 1", report.Query.Joins)
	}
	if len(report.Query.JoinEdges) != 1 {
		t.Fatalf("join summaries = %#v, want one edge", report.Query.JoinEdges)
	}
	edge := report.Query.JoinEdges[0]
	if edge.Kind != JoinKindInner || edge.Left != "o.o_custkey" || edge.Right != "c.c_custkey" {
		t.Fatalf("join edge = %#v, want inner o->c edge", edge)
	}
	if edge.Direction != JoinChildToParent || edge.Cardinality != "many_to_one" {
		t.Fatalf("join edge = %#v, want child-to-parent many-to-one", edge)
	}
	if edge.EncodingKind != RelationshipEncodingVector {
		t.Fatalf("encoding kind = %q, want relation vector", edge.EncodingKind)
	}
	if !relationshipCapabilitiesContain(edge.Capabilities, RelationshipCapabilityJoinReduction) {
		t.Fatalf("relationship capabilities = %#v, want join reduction", edge.Capabilities)
	}
	if edge.Strategy != RelationshipStrategyVectorReduction || !edge.UsesRelationshipVector {
		t.Fatalf("join strategy = %#v, want relationship-vector reduction", edge)
	}
}

func TestInspectQueryExposesScanAndShardWindowMetadata(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Type: DataTypeInt}
	orderDate := FieldRef{Table: orders, Name: "o_orderdate", Type: DataTypeTime, Index: IndexDateTime}
	fullScan := QueryIR{
		Kind:       QueryKindSelect,
		Sources:    []TableInstance{orders},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
	}

	report := InspectQuery(fullScan, PhysicalScope{})
	if !report.Query.Scan.FullTable || report.Query.Scan.Strategy != ScanStrategyFullTable {
		t.Fatalf("scan = %#v, want full-table scan", report.Query.Scan)
	}
	if len(report.Query.Scan.Tables) != 1 || report.Query.Scan.Tables[0] != "orders.as o" {
		t.Fatalf("scan tables = %#v, want orders alias", report.Query.Scan.Tables)
	}

	filtered := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders},
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpGreaterEqual, Field(orderDate), Literal(ValueString, "1995-01-01")),
			Placement: PredicatePushdown,
			Scope:     PredicateScopeWhere,
		}},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
	}
	report = InspectQuery(filtered, PhysicalScope{})
	if report.Query.Scan.FullTable || report.Query.Scan.Strategy != ScanStrategyFiltered {
		t.Fatalf("scan = %#v, want filtered scan", report.Query.Scan)
	}
	if report.Query.ShardWindow.CandidatePredicates != 1 {
		t.Fatalf("shard window = %#v, want one time predicate", report.Query.ShardWindow)
	}
	if len(report.Query.ShardWindow.Fields) != 1 || report.Query.ShardWindow.Fields[0] != "o.o_orderdate" {
		t.Fatalf("shard window fields = %#v, want o.o_orderdate", report.Query.ShardWindow.Fields)
	}
	if len(report.Query.ShardWindow.Notes) == 0 {
		t.Fatalf("shard window notes = %#v, want explanatory note", report.Query.ShardWindow.Notes)
	}
}

func fieldEncodingSummary(summaries []FieldEncodingInspection, field string) FieldEncodingInspection {
	for _, summary := range summaries {
		if summary.Field == field {
			return summary
		}
	}
	return FieldEncodingInspection{}
}

func predicateCapabilitiesContain(capabilities []PredicateCapability, want PredicateCapability) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}

func relationshipCapabilitiesContain(capabilities []RelationshipCapability, want RelationshipCapability) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}
