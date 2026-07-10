package qsbridge

import "testing"

func TestShardSetContainsExplicitAndAllShards(t *testing.T) {
	shards := ShardSet{Shards: []ShardID{"s1", "s2"}}
	if !shards.Contains("s1") {
		t.Fatalf("expected explicit shard to be contained")
	}
	if shards.Contains("s3") {
		t.Fatalf("did not expect missing shard to be contained")
	}

	all := ShardSet{All: true}
	if !all.Contains("anything") {
		t.Fatalf("expected all-shard set to contain arbitrary shard")
	}
}

func TestPhysicalScopeUnscoped(t *testing.T) {
	if !((PhysicalScope{}).Unscoped()) {
		t.Fatalf("empty physical scope should be unscoped")
	}
	scope := PhysicalScope{Shards: ShardSet{Shards: []ShardID{"s1"}}}
	if scope.Unscoped() {
		t.Fatalf("scope with shard should not be unscoped")
	}
}

func TestPhysicalIntentForQueryKindMapsReadsAndWrites(t *testing.T) {
	cases := []struct {
		name string
		kind QueryKind
		want PhysicalAccessIntent
	}{
		{name: "select", kind: QueryKindSelect, want: PhysicalAccessRead},
		{name: "session", kind: QueryKindSession, want: PhysicalAccessRead},
		{name: "insert", kind: QueryKindInsert, want: PhysicalAccessWrite},
		{name: "update", kind: QueryKindUpdate, want: PhysicalAccessWrite},
		{name: "delete", kind: QueryKindDelete, want: PhysicalAccessWrite},
		{name: "ddl", kind: QueryKindDDL, want: PhysicalAccessWrite},
		{name: "unknown", kind: QueryKind("unknown"), want: PhysicalAccessRead},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PhysicalIntentForQueryKind(tc.kind); got != tc.want {
				t.Fatalf("PhysicalIntentForQueryKind(%q) = %q, want %q", tc.kind, got, tc.want)
			}
		})
	}
}

func TestSelectPhysicalNodesReadChoosesOneOwner(t *testing.T) {
	selection := SelectPhysicalNodes(PhysicalNodeSelectionInput{
		Intent:             PhysicalAccessRead,
		Nodes:              []NodeID{"node-a", "node-b", "node-c"},
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
		Topology: ClusterTopologyProfile{
			Name:              "test",
			ReplicationFactor: 3,
		},
	})
	if selection.Intent != PhysicalAccessRead || !selection.Complete || len(selection.Nodes) != 1 {
		t.Fatalf("selection = %#v, want one complete read owner", selection)
	}
	if selection.Nodes[0] != selection.Placement.Owners[0] {
		t.Fatalf("selection = %#v, want first rendezvous owner", selection)
	}
}

func TestSelectPhysicalNodesDefaultsToReadIntent(t *testing.T) {
	selection := SelectPhysicalNodes(PhysicalNodeSelectionInput{
		Nodes:              []NodeID{"node-a", "node-b", "node-c"},
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
		Topology: ClusterTopologyProfile{
			Name:              "test",
			ReplicationFactor: 3,
		},
	})
	if selection.Intent != PhysicalAccessRead || len(selection.Nodes) != 1 {
		t.Fatalf("selection = %#v, want default read owner", selection)
	}
}

func TestSelectPhysicalNodesWriteChoosesReplicationOwners(t *testing.T) {
	selection := SelectPhysicalNodes(PhysicalNodeSelectionInput{
		Intent:             PhysicalAccessWrite,
		Nodes:              []NodeID{"node-a", "node-b", "node-c"},
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
		Topology: ClusterTopologyProfile{
			Name:              "test",
			ReplicationFactor: 2,
		},
	})
	if selection.Intent != PhysicalAccessWrite || !selection.Complete || len(selection.Nodes) != 2 {
		t.Fatalf("selection = %#v, want two complete write owners", selection)
	}
	if selection.Placement.ReplicationFactor != 2 {
		t.Fatalf("placement = %#v, want topology RF", selection.Placement)
	}
}

func TestSelectPhysicalNodesWriteReportsIncompleteReplication(t *testing.T) {
	selection := SelectPhysicalNodes(PhysicalNodeSelectionInput{
		Intent:             PhysicalAccessWrite,
		Nodes:              []NodeID{"node-a"},
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
		Topology: ClusterTopologyProfile{
			Name:              "test",
			ReplicationFactor: 3,
		},
	})
	if selection.Complete || len(selection.Nodes) != 1 || selection.Placement.Complete {
		t.Fatalf("selection = %#v, want incomplete write selection", selection)
	}
}

func TestBuildPhysicalPlanPreservesLogicalShapeAndScope(t *testing.T) {
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
		Shards:    ShardSet{Shards: []ShardID{"lineitem-0001"}},
		Replicas:  []ReplicaID{"replica-a"},
		Routing:   RoutingKey("l_shipmode"),
		Placement: PlacementLocal,
		Cache:     CacheQuery,
	}

	physical := BuildPhysicalPlan(logical, scope)
	if physical.Root.PhysicalKind() != PhysicalNodeLimit {
		t.Fatalf("root kind = %q, want %q", physical.Root.PhysicalKind(), PhysicalNodeLimit)
	}
	kinds := collectPhysicalKinds(physical.Root)
	want := []PhysicalNodeKind{PhysicalNodeLimit, PhysicalNodeProject, PhysicalNodeScan}
	assertPhysicalKinds(t, kinds, want)
	if !physical.Root.PhysicalScope().Shards.Contains("lineitem-0001") {
		t.Fatalf("expected physical root to carry default shard scope")
	}
}

func TestBuildPhysicalPlanCreatesPhysicalJoin(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	lineOrderKey := FieldRef{Table: lineitem, Name: "l_orderkey"}
	logical := BuildLogicalPlan(QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders, lineitem},
		Joins: []JoinEdge{{
			Left:      lineOrderKey,
			Right:     orderKey,
			Direction: JoinChildToParent,
			Legal:     true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
	})

	physical := BuildPhysicalPlan(logical, PhysicalScope{Placement: PlacementAny})
	kinds := collectPhysicalKinds(physical.Root)
	want := []PhysicalNodeKind{
		PhysicalNodeProject,
		PhysicalNodeJoin,
		PhysicalNodeScan,
		PhysicalNodeScan,
	}
	assertPhysicalKinds(t, kinds, want)
}

func TestBuildPhysicalPlanMarksPeerEqualityJoinStrategy(t *testing.T) {
	customer := TableInstance{ID: "customers_qa", Table: "customers_qa", Alias: "c"}
	orders := TableInstance{ID: "orders_qa", Table: "orders_qa", Alias: "o"}
	custID := FieldRef{Table: customer, Name: "cust_id"}
	orderCustID := FieldRef{Table: orders, Name: "cust_id"}
	logical := BuildLogicalPlan(QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{customer, orders},
		Joins: []JoinEdge{{
			Left:      custID,
			Right:     orderCustID,
			Kind:      JoinKindInner,
			Direction: JoinPeerEquality,
			Legal:     true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(custID)}},
	})

	physical := BuildPhysicalPlan(logical, PhysicalScope{})
	join, ok := findPhysicalJoinNode(physical.Root)
	if !ok {
		t.Fatalf("expected physical join node")
	}
	if !physicalStrategiesContain(join.Strategies, PhysicalStrategyPeerEqualityJoin) {
		t.Fatalf("strategies = %#v, want peer equality join", join.Strategies)
	}
	if physicalStrategiesContain(join.Strategies, PhysicalStrategyRelationshipJoinReduction) {
		t.Fatalf("strategies = %#v, did not expect relationship-vector reduction", join.Strategies)
	}
}

func TestBuildPhysicalPlanCreatesPhysicalStatement(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Type: DataTypeInt}
	logical := BuildLogicalPlan(QueryIR{
		Kind: QueryKindUpdate,
		Result: ResultShape{
			Kind:      ResultStatement,
			Statement: StatementResult{AffectedRows: 4, Warnings: 1},
		},
		Mutation: MutationShape{
			Kind:   MutationUpdate,
			Target: orders,
			Assignments: []MutationAssignment{{
				Field: FieldRef{Table: orders, Name: "o_totalprice"},
				Value: Literal(ValueFloat, 10.5),
			}},
			Predicates: []Predicate{{Expr: BinaryExpr{Op: BinaryOpEqual, Left: Field(orderKey), Right: Literal(ValueInt, int64(1))}}},
		},
	})

	physical := BuildPhysicalPlan(logical, PhysicalScope{Placement: PlacementPrimary})
	statement, ok := physical.Root.(PhysicalStatementNode)
	if !ok {
		t.Fatalf("root = %T, want PhysicalStatementNode", physical.Root)
	}
	if statement.Kind != QueryKindUpdate || statement.Result.AffectedRows != 4 || statement.Result.Warnings != 1 {
		t.Fatalf("physical statement = %#v, want update OK metadata", statement)
	}
	if statement.Mutation.Kind != MutationUpdate || len(statement.Mutation.Assignments) != 1 {
		t.Fatalf("physical mutation = %#v, want update mutation shape", statement.Mutation)
	}
	if statement.Scope.Placement != PlacementPrimary {
		t.Fatalf("scope = %#v, want primary placement", statement.Scope)
	}
}

func TestBuildPhysicalPlanRecordsRelationshipJoinStrategies(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	logical := BuildLogicalPlan(QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders, lineitem},
		Joins: []JoinEdge{{
			Left:      FieldRef{Table: lineitem, Name: "l_orderkey"},
			Right:     FieldRef{Table: orders, Name: "o_orderkey"},
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
	})

	physical := BuildPhysicalPlan(logical, PhysicalScope{})
	join, ok := findPhysicalJoinNode(physical.Root)
	if !ok {
		t.Fatalf("expected physical join node")
	}
	if !physicalStrategiesContain(join.Strategies, PhysicalStrategyRelationshipParentLookup) {
		t.Fatalf("strategies = %#v, want relationship parent lookup", join.Strategies)
	}
	if !physicalStrategiesContain(join.Strategies, PhysicalStrategyRelationshipJoinReduction) {
		t.Fatalf("strategies = %#v, want relationship join reduction", join.Strategies)
	}
	if physicalStrategiesContain(join.Strategies, PhysicalStrategyPeerEqualityJoin) {
		t.Fatalf("strategies = %#v, did not expect peer equality fallback", join.Strategies)
	}
}

func TestBuildPhysicalPlanRecordsOuterJoinStrategies(t *testing.T) {
	customer := TableInstance{ID: "customers_qa", Table: "customers_qa", Alias: "c"}
	orders := TableInstance{ID: "orders_qa", Table: "orders_qa", Alias: "o"}
	custID := FieldRef{Table: customer, Name: "cust_id"}
	orderCustID := FieldRef{Table: orders, Name: "cust_id"}
	logical := BuildLogicalPlan(QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{customer, orders},
		Joins: []JoinEdge{{
			Left:      custID,
			Right:     orderCustID,
			Kind:      JoinKindLeftOuter,
			Nulls:     NullExtensionRight,
			Direction: JoinPeerEquality,
			Legal:     true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(custID)}},
	})

	physical := BuildPhysicalPlan(logical, PhysicalScope{})
	join, ok := findPhysicalJoinNode(physical.Root)
	if !ok {
		t.Fatalf("expected physical join node")
	}
	if !physicalStrategiesContain(join.Strategies, PhysicalStrategyPeerEqualityJoin) {
		t.Fatalf("strategies = %#v, want peer equality join", join.Strategies)
	}
	if !physicalStrategiesContain(join.Strategies, PhysicalStrategyOuterNullExtension) {
		t.Fatalf("strategies = %#v, want outer null extension", join.Strategies)
	}
	if !physicalStrategiesContain(join.Strategies, PhysicalStrategyBitmapDifference) {
		t.Fatalf("strategies = %#v, want bitmap difference", join.Strategies)
	}
}

func TestBuildPhysicalPlanCreatesPhysicalMembership(t *testing.T) {
	partsupp := TableInstance{ID: "partsupp", Table: "partsupp", Alias: "ps"}
	supplier := TableInstance{ID: "supplier", Table: "supplier", Alias: "s"}
	partSuppKey := FieldRef{Table: partsupp, Name: "ps_suppkey"}
	suppKey := FieldRef{Table: supplier, Name: "s_suppkey"}
	logical := BuildLogicalPlan(QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{partsupp},
		Memberships: []MembershipEdge{{
			Left:  partSuppKey,
			Right: suppKey,
			Kind:  MembershipAnti,
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{
					RelationshipCapabilitySemiJoin,
					RelationshipCapabilityAntiJoinDifference,
				},
			},
			Legal: true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(partSuppKey)}},
	})

	physical := BuildPhysicalPlan(logical, PhysicalScope{})
	kinds := collectPhysicalKinds(physical.Root)
	want := []PhysicalNodeKind{
		PhysicalNodeProject,
		PhysicalNodeMembership,
		PhysicalNodeScan,
	}
	assertPhysicalKinds(t, kinds, want)

	membership, ok := findPhysicalUnaryNode(physical.Root, PhysicalNodeMembership)
	if !ok {
		t.Fatalf("expected physical membership node")
	}
	if !physicalStrategiesContain(membership.Strategies, PhysicalStrategyRelationshipAntiJoinDifference) {
		t.Fatalf("strategies = %#v, want relationship anti-join difference", membership.Strategies)
	}
	if !physicalStrategiesContain(membership.Strategies, PhysicalStrategyRelationshipSemiJoin) {
		t.Fatalf("strategies = %#v, want relationship semi-join", membership.Strategies)
	}
}

func TestBuildPhysicalPlanRecordsFilterStrategies(t *testing.T) {
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
			Expr:         Binary(BinaryOpGreater, Field(quantity), Literal(ValueInt, 10)),
			Placement:    PredicatePushdown,
			Capabilities: []PlanCapability{CapabilityBSIPushdown},
		}},
		Projection: []ProjectionColumn{{Expr: Field(quantity)}},
	}

	physical := BuildPhysicalPlan(BuildLogicalPlan(query), PhysicalScope{})
	filter, ok := findPhysicalUnaryNode(physical.Root, PhysicalNodeFilter)
	if !ok {
		t.Fatalf("expected physical filter node")
	}
	if !physicalStrategiesContain(filter.Strategies, PhysicalStrategyBSIPushdown) {
		t.Fatalf("strategies = %#v, want BSI pushdown", filter.Strategies)
	}
	if !physicalStrategiesContain(filter.Strategies, PhysicalStrategyEncodingRange) {
		t.Fatalf("strategies = %#v, want encoding range", filter.Strategies)
	}
}

func TestBuildPhysicalPlanMarksQuantaTopNAggregateIntent(t *testing.T) {
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
	if len(aggregate.Aggregates) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(aggregate.Aggregates))
	}
	if aggregate.Aggregates[0].Function != "topn" || aggregate.Aggregates[0].Origin != FunctionOriginQuantaCustom {
		t.Fatalf("aggregate = %#v, want Quanta custom topn", aggregate.Aggregates[0])
	}
	if !physicalStrategiesContain(aggregate.Strategies, PhysicalStrategyQuantaTopN) {
		t.Fatalf("strategies = %#v, want quanta topn", aggregate.Strategies)
	}
}

func TestPhysicalPlanDiagnosticsPreservesUnsupportedNode(t *testing.T) {
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

	physical := BuildPhysicalPlan(logical, PhysicalScope{})
	diagnostics := PhysicalPlanDiagnostics(physical.Root)
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected physical diagnostics to include unsupported logical diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != DiagnosticUnsupportedPredicate {
		t.Fatalf("diagnostic code = %q, want %q", got, DiagnosticUnsupportedPredicate)
	}
}

func TestWalkPhysicalPlanCanStopTraversal(t *testing.T) {
	root := PhysicalUnaryNode{
		Kind:  PhysicalNodeFilter,
		Input: PhysicalScanNode{Source: TableInstance{ID: "customer", Table: "customer"}},
	}
	var visited []PhysicalNodeKind
	WalkPhysicalPlan(root, func(node PhysicalNode) bool {
		visited = append(visited, node.PhysicalKind())
		return false
	})

	assertPhysicalKinds(t, visited, []PhysicalNodeKind{PhysicalNodeFilter})
}

func collectPhysicalKinds(root PhysicalNode) []PhysicalNodeKind {
	kinds := make([]PhysicalNodeKind, 0)
	WalkPhysicalPlan(root, func(node PhysicalNode) bool {
		kinds = append(kinds, node.PhysicalKind())
		return true
	})
	return kinds
}

func findPhysicalUnaryNode(root PhysicalNode, kind PhysicalNodeKind) (PhysicalUnaryNode, bool) {
	var found PhysicalUnaryNode
	var matched bool
	WalkPhysicalPlan(root, func(node PhysicalNode) bool {
		if node.PhysicalKind() != kind {
			return true
		}
		typed, ok := node.(PhysicalUnaryNode)
		if !ok {
			return true
		}
		found = typed
		matched = true
		return false
	})
	return found, matched
}

func findPhysicalJoinNode(root PhysicalNode) (PhysicalJoinNode, bool) {
	var found PhysicalJoinNode
	var matched bool
	WalkPhysicalPlan(root, func(node PhysicalNode) bool {
		typed, ok := node.(PhysicalJoinNode)
		if !ok {
			return true
		}
		found = typed
		matched = true
		return false
	})
	return found, matched
}

func findPhysicalAggregateNode(root PhysicalNode) (PhysicalAggregateNode, bool) {
	var found PhysicalAggregateNode
	var matched bool
	WalkPhysicalPlan(root, func(node PhysicalNode) bool {
		typed, ok := node.(PhysicalAggregateNode)
		if !ok {
			return true
		}
		found = typed
		matched = true
		return false
	})
	return found, matched
}

func physicalStrategiesContain(strategies []PhysicalStrategy, want PhysicalStrategy) bool {
	for _, strategy := range strategies {
		if strategy == want {
			return true
		}
	}
	return false
}

func assertPhysicalKinds(t *testing.T, got []PhysicalNodeKind, want []PhysicalNodeKind) {
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
