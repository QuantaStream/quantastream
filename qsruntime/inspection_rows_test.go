package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestExecutionInspectionRowsExposeRouteCallPlanStepsAndDiagnostics(t *testing.T) {
	inspection := ExecutionInspection{
		Route:            DirectQIABRoute(),
		SelectedExecutor: ExecutionInspectionExecutorDirect,
		RuntimeProfile:   FixtureRuntimeProfile("inspection-test"),
		Shape: ExecutionShapeInspection{
			GroupedAggregateTopNCandidate: true,
			GroupedAggregateTopNDetail:    "group_by=1 aggregates=1 having=1 order_by=1 limit=10",
			FoundsetFollowUpCandidate:     true,
			FoundsetFollowUpDetail:        "fragment=lineitem.l_shipdate edges=1",
		},
		FilterDomain: qsbridge.QuantaFilterDomainTranslation{
			Required:      true,
			SourceDomains: []string{"lineitem", "part"},
			TargetDomain:  "lineitem",
			Strategies:    []qsbridge.PhysicalStrategy{qsbridge.PhysicalStrategyRelationshipVectorNormalization},
		},
		FilterDomainPlan: qsbridge.FilterDomainNormalizationPlan{
			Operation: qsbridge.FilterDomainNormalizeGroupedFilter,
			Translation: qsbridge.QuantaFilterDomainTranslation{
				Required:      true,
				SourceDomains: []string{"lineitem", "part"},
				TargetDomain:  "lineitem",
				Strategies:    []qsbridge.PhysicalStrategy{qsbridge.PhysicalStrategyRelationshipVectorNormalization},
			},
			Requests: []qsbridge.FilterDomainNormalizationRequest{{
				SourceDomain: "part",
				TargetDomain: "lineitem",
				Strategy:     qsbridge.PhysicalStrategyRelationshipVectorNormalization,
				RelationshipPath: []qsbridge.RelationshipJoinPlanEdge{{
					Left: qsbridge.FieldRef{
						Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
						Name:  "l_partkey",
					},
					Right: qsbridge.FieldRef{
						Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
						Name:  "p_partkey",
					},
					ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
				}},
			}},
		},
		Materialization: ProjectionMaterializationCapabilityReport{
			NativeFieldCount:            1,
			CompatFallbackFieldCount:    1,
			RuntimeFallbackObserved:     true,
			FallbackDiagnosticCount:     2,
			LegacyMaterializerReachable: true,
			LegacyMaterializerUsed:      true,
			Fields: []ProjectionMaterializationFieldCapability{
				{
					Index:      "orders",
					Field:      "o_orderkey",
					Type:       qsbridge.DataTypeInt,
					Encoding:   qsbridge.EncodingNumericBSI,
					Status:     ProjectionMaterializationCapabilityNative,
					Source:     "native_bsi",
					ReasonCode: ProjectionMaterializationReasonNativeInlineBSI,
					Reason:     "scalar BSI-compatible projection",
				},
				{
					Index:      "orders",
					Field:      "o_orderpriority",
					Type:       qsbridge.DataTypeString,
					Encoding:   qsbridge.EncodingBackingString,
					Status:     ProjectionMaterializationCapabilityCompatFallback,
					LookupKind: NativeProjectionLookupBackingString,
					Source:     "kvstore_needed",
					ReasonCode: ProjectionMaterializationReasonBackingStringKV,
					Reason:     "string projection requires lookup rehydration",
				},
			},
		},
		CallPlan: (LegacyExecutionCallPlan{
			RootIndex:            "orders",
			Steps:                []LegacyExecutionCallStep{LegacyExecutionStepBorrowSession, LegacyExecutionStepQueryBitIndex},
			FragmentCount:        1,
			ProjectionCount:      2,
			SQLAggregateCount:    1,
			NativeAggregateCount: 0,
			HasMaterialization:   true,
			UsesSessionPool:      true,
			Notes:                []string{"assembly stays outside bitmap query"},
		}).WithRuntimeProfile(FixtureRuntimeProfile("inspection-test")),
		Diagnostics: qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseExecute, "bad option"),
		},
	}

	rows := inspection.Rows()
	if !inspectionRowsContain(rows, "route", "path", string(ExecutionPathDirectQIAB)) {
		t.Fatalf("rows missing direct route path: %#v", rows)
	}
	if !inspectionRowsContain(rows, "executor", "selected", string(ExecutionInspectionExecutorDirect)) {
		t.Fatalf("rows missing selected executor: %#v", rows)
	}
	if !inspectionRowsContain(rows, "runtime", "implementation", string(RuntimeImplementationFixture)) {
		t.Fatalf("rows missing runtime implementation: %#v", rows)
	}
	if !inspectionRowsContain(rows, "runtime", "detail", "inspection-test") {
		t.Fatalf("rows missing runtime detail: %#v", rows)
	}
	if !inspectionRowsContainDetail(rows, "execution_shape", "grouped_aggregate_topn_candidate", "group_by=1 aggregates=1 having=1 order_by=1 limit=10") {
		t.Fatalf("rows missing grouped aggregate top-N shape: %#v", rows)
	}
	if !inspectionRowsContainDetail(rows, "execution_shape", "foundset_followup_candidate", "fragment=lineitem.l_shipdate edges=1") {
		t.Fatalf("rows missing foundset follow-up shape: %#v", rows)
	}
	if !inspectionRowsContain(rows, "filter_domain", "translation_required", "true") {
		t.Fatalf("rows missing filter-domain translation requirement: %#v", rows)
	}
	if !inspectionRowsContain(rows, "filter_domain", "source_domains", "lineitem,part") {
		t.Fatalf("rows missing filter-domain sources: %#v", rows)
	}
	if !inspectionRowsContain(rows, "filter_domain", "target_domain", "lineitem") {
		t.Fatalf("rows missing filter-domain target: %#v", rows)
	}
	if !inspectionRowsContain(rows, "filter_domain", "strategies", string(qsbridge.PhysicalStrategyRelationshipVectorNormalization)) {
		t.Fatalf("rows missing filter-domain strategies: %#v", rows)
	}
	if !inspectionRowsContainDetail(rows, "filter_domain_normalization", "request_count", "operation=grouped_filter") {
		t.Fatalf("rows missing filter-domain normalization count: %#v", rows)
	}
	if !inspectionRowsContainDetail(rows, "filter_domain_normalization", "expected_replacements", "source-domain leaves require target-domain candidates") {
		t.Fatalf("rows missing filter-domain expected replacements: %#v", rows)
	}
	if !inspectionRowsContainDetail(rows, "filter_domain_normalization", "request_001", "path_len=1 strategy=relationship_vector_normalization replacement=expected") {
		t.Fatalf("rows missing filter-domain normalization request: %#v", rows)
	}
	if !inspectionRowsContainDetail(rows, "filter_domain_normalization", "vector_001", "source_leaf=pending source_candidates=part edge=l.l_partkey->p.p_partkey direction=right_to_left target=lineitem") {
		t.Fatalf("rows missing concrete vector normalization request: %#v", rows)
	}
	if !inspectionRowsContain(rows, "materialization_capability", "native_fields", "1") {
		t.Fatalf("rows missing native materialization field count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "materialization_capability", "compat_fallback_fields", "1") {
		t.Fatalf("rows missing fallback materialization field count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "materialization_capability", "runtime_fallback_observed", "true") {
		t.Fatalf("rows missing runtime fallback observation: %#v", rows)
	}
	if !inspectionRowsContain(rows, "materialization_capability", "fallback_diagnostic_count", "2") {
		t.Fatalf("rows missing fallback diagnostic count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "materialization_capability", "legacy_materializer_reachable", "true") {
		t.Fatalf("rows missing legacy materializer reachability: %#v", rows)
	}
	if !inspectionRowsContain(rows, "materialization_capability", "legacy_materializer_used", "true") {
		t.Fatalf("rows missing legacy materializer observed use: %#v", rows)
	}
	if !inspectionRowsContainDetail(rows, "materialization_field", "002", "field=orders.o_orderpriority type=string native=false encoding=backing_string lookup=backing_string source=kvstore_needed reason_code=backing_string_kvstore_needed reason=string projection requires lookup rehydration") {
		t.Fatalf("rows missing backing-string materialization lookup detail: %#v", rows)
	}
	if !inspectionRowsContain(rows, "call_plan", "root_index", "orders") {
		t.Fatalf("rows missing root index: %#v", rows)
	}
	if !inspectionRowsContain(rows, "step", "002", string(LegacyExecutionStepQueryBitIndex)) {
		t.Fatalf("rows missing second step: %#v", rows)
	}
	if !inspectionRowsContainDetail(rows, "step", "002", "status=fixture") {
		t.Fatalf("rows missing fixture step status: %#v", rows)
	}
	if !inspectionRowsContain(rows, "note", "001", "assembly stays outside bitmap query") {
		t.Fatalf("rows missing note: %#v", rows)
	}
	if !inspectionRowsContain(rows, "diagnostic", "001", string(qsbridge.DiagnosticInvalidExecutionOption)) {
		t.Fatalf("rows missing diagnostic: %#v", rows)
	}
}

func TestExecutionInspectionResultChunkReturnsProtocolNeutralRows(t *testing.T) {
	inspection := ExecutionInspection{
		Route:            LegacyGRPCRoute(RuntimeTarget{NodeID: "node-a", Address: "127.0.0.1:4000"}),
		SelectedExecutor: ExecutionInspectionExecutorLegacy,
		RuntimeProfile:   LegacyDirectRuntimeProfile(),
		CallPlan: LegacyExecutionCallPlan{
			RootIndex: "partsupp",
			Steps:     []LegacyExecutionCallStep{LegacyExecutionStepBuildBitmapQuery},
		},
	}

	chunk := inspection.ResultChunk(7, true)
	if chunk.Sequence != 7 || !chunk.Final {
		t.Fatalf("chunk metadata = %#v, want sequence 7 final", chunk)
	}
	if len(chunk.Rows) == 0 {
		t.Fatalf("chunk rows = 0, want inspection rows")
	}
	for _, row := range chunk.Rows {
		if len(row) != len(ExecutionInspectionResultColumns()) {
			t.Fatalf("row width = %d, want %d", len(row), len(ExecutionInspectionResultColumns()))
		}
	}
	if !resultChunkContains(chunk, "route", "target", "node=node-a address=127.0.0.1:4000") {
		t.Fatalf("chunk missing target row: %#v", chunk.Rows)
	}
	if !resultChunkContains(chunk, "runtime", "implementation", string(RuntimeImplementationLegacyDirect)) {
		t.Fatalf("chunk missing runtime implementation row: %#v", chunk.Rows)
	}
}

func TestSQLInspectionRowsIncludeSQLAndRuntimeSections(t *testing.T) {
	result := SQLInspectionResult{
		Prepared: qsbridge.PreparedPlan{Kind: qsbridge.QueryKindSelect},
		Request: qsbridge.ExecutionRequest{
			ResultColumns: []qsbridge.ResultColumn{{Name: "o_orderkey", Type: qsbridge.DataTypeInt}},
			Bound: qsbridge.BoundPlan{
				Prepared: qsbridge.PreparedPlan{
					Query: qsbridge.QueryIR{
						Kind:    qsbridge.QueryKindSelect,
						Sources: []qsbridge.TableInstance{{Table: "orders", Alias: "o"}, {Table: "lineitem", Alias: "l"}},
						Joins: []qsbridge.JoinEdge{{
							Left:  qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "o"}, Name: "o_orderkey"},
							Right: qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "l"}, Name: "l_orderkey"},
							Kind:  qsbridge.JoinKindInner,
						}},
						Memberships: []qsbridge.MembershipEdge{{
							Left:      qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "l"}, Name: "l_suppkey"},
							Right:     qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "s"}, Name: "s_suppkey"},
							Kind:      qsbridge.MembershipAnti,
							Direction: qsbridge.JoinChildToParent,
							Encoding: qsbridge.RelationshipEncodingProfile{
								Kind: qsbridge.RelationshipEncodingVector,
								Capabilities: qsbridge.RelationshipCapabilities{
									qsbridge.RelationshipCapabilityAntiJoinDifference,
								},
							},
							Predicates: []qsbridge.Predicate{{Expr: qsbridge.Binary(qsbridge.BinaryOpLike, qsbridge.Field(qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "s"}, Name: "s_comment"}), qsbridge.Literal(qsbridge.ValueString, "%Customer%Complaints%"))}},
							Legal:      true,
						}},
						Predicates: []qsbridge.Predicate{{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(qsbridge.FieldRef{Name: "l_returnflag"}), qsbridge.Literal(qsbridge.ValueString, "R"))}},
						GroupBy:    []qsbridge.Expr{qsbridge.Field(qsbridge.FieldRef{Name: "l_shipmode"})},
						Aggregates: []qsbridge.Aggregate{
							{
								Function: "count",
							},
							{
								Function: "sum",
								Input: qsbridge.SearchedCase(
									[]qsbridge.SearchedCaseWhen{{
										Condition: qsbridge.Binary(qsbridge.BinaryOpIn, qsbridge.Field(qsbridge.FieldRef{Name: "o_orderpriority"}), qsbridge.List(qsbridge.Literal(qsbridge.ValueString, "1-URGENT"))),
										Result:    qsbridge.Literal(qsbridge.ValueInt, 1),
									}},
									qsbridge.Literal(qsbridge.ValueInt, 0),
								),
							},
							{
								Function: "sum",
								Input:    qsbridge.Binary(qsbridge.BinaryOpMultiply, qsbridge.Field(qsbridge.FieldRef{Name: "l_extendedprice"}), qsbridge.Field(qsbridge.FieldRef{Name: "l_discount"})),
							},
						},
						OrderBy: []qsbridge.SortSpec{{Expr: qsbridge.Field(qsbridge.FieldRef{Name: "l_shipmode"})}},
						Result:  qsbridge.ResultShape{Limit: 10},
					},
				},
			},
		},
		Intermediate: qsbridge.QuantaIntermediateQuery{
			Fragments: []qsbridge.QuantaQueryFragment{{Index: "orders"}},
			Filter: qsbridge.QuantaFilterExpression{
				Operation: qsbridge.QuantaFilterUnion,
				Children: []qsbridge.QuantaFilterExpression{
					{
						Operation: qsbridge.QuantaFilterIntersect,
						Children: []qsbridge.QuantaFilterExpression{
							{
								Operation: qsbridge.QuantaFilterLeaf,
								Fragment: qsbridge.QuantaQueryFragment{
									Index:     "orders",
									Field:     "o_orderkey",
									Operation: qsbridge.QuantaOperationIntersect,
									BSIOp:     qsbridge.QuantaBSIOpEQ,
								},
							},
							{
								Operation: qsbridge.QuantaFilterLeaf,
								Fragment: qsbridge.QuantaQueryFragment{
									Index:     "orders",
									Field:     "o_custkey",
									Operation: qsbridge.QuantaOperationIntersect,
									BSIOp:     qsbridge.QuantaBSIOpEQ,
								},
							},
						},
					},
				},
			},
			ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "orders", Field: "o_orderkey"}},
		},
		Runtime: ExecutionInspection{
			Route:            DirectQIABRoute(),
			SelectedExecutor: ExecutionInspectionExecutorDirect,
			CallPlan:         LegacyExecutionCallPlan{RootIndex: "orders"},
		},
		FilterExecutionEnabled: true,
	}

	rows := result.Rows()
	if !inspectionRowsContain(rows, "sql", "kind", string(qsbridge.QueryKindSelect)) {
		t.Fatalf("rows missing SQL kind: %#v", rows)
	}
	if !inspectionRowsContain(rows, "sql", "result_columns", "1") {
		t.Fatalf("rows missing result column count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "intermediate", "fragments", "1") {
		t.Fatalf("rows missing fragment count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "query_shape", "sources", "2") {
		t.Fatalf("rows missing query source count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "query_shape", "joins", "1") {
		t.Fatalf("rows missing query join count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "query_shape", "memberships", "1") {
		t.Fatalf("rows missing query membership count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "query_shape", "group_by", "1") {
		t.Fatalf("rows missing query group-by count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "query_shape", "aggregates", "3") {
		t.Fatalf("rows missing query aggregate count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "query_shape", "order_by", "1") {
		t.Fatalf("rows missing query order-by count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "query_shape", "limit", "10") {
		t.Fatalf("rows missing query limit: %#v", rows)
	}
	if !inspectionRowsContain(rows, "query_shape", "aggregate_functions", "count=1,sum=2") {
		t.Fatalf("rows missing aggregate function summary: %#v", rows)
	}
	if !inspectionRowsContain(rows, "query_shape", "conditional_aggregates", "1") {
		t.Fatalf("rows missing conditional aggregate count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "query_shape", "arithmetic_aggregates", "1") {
		t.Fatalf("rows missing arithmetic aggregate count: %#v", rows)
	}
	if !inspectionRowsContainDetail(rows, "membership", "001.kind", "left=l.l_suppkey right=s.s_suppkey") {
		t.Fatalf("rows missing membership edge detail: %#v", rows)
	}
	if !inspectionRowsContain(rows, "membership", "001.kind", "anti") {
		t.Fatalf("rows missing membership kind: %#v", rows)
	}
	if !inspectionRowsContain(rows, "membership", "001.encoding", "relation_vector") {
		t.Fatalf("rows missing membership encoding: %#v", rows)
	}
	if !inspectionRowsContain(rows, "membership", "001.capabilities", "anti_join_difference") {
		t.Fatalf("rows missing membership capabilities: %#v", rows)
	}
	if !inspectionRowsContain(rows, "membership", "001.predicates", "1") {
		t.Fatalf("rows missing membership predicate count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "membership", "001.supported", "true") {
		t.Fatalf("rows missing membership support: %#v", rows)
	}
	if !inspectionRowsContain(rows, "intermediate", "filter_nodes", "4") {
		t.Fatalf("rows missing filter node count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "intermediate", "filter_leaves", "2") {
		t.Fatalf("rows missing filter leaf count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "intermediate", "filter_depth", "3") {
		t.Fatalf("rows missing filter depth: %#v", rows)
	}
	if !inspectionRowsContain(rows, "filter", "root", string(qsbridge.QuantaFilterUnion)) {
		t.Fatalf("rows missing filter root: %#v", rows)
	}
	if !inspectionRowsContain(rows, "filter", "root.1", string(qsbridge.QuantaFilterIntersect)) {
		t.Fatalf("rows missing filter branch: %#v", rows)
	}
	if !inspectionRowsContainDetail(rows, "filter", "root.1.1", "orders.o_orderkey INTERSECT EQ") {
		t.Fatalf("rows missing filter leaf detail: %#v", rows)
	}
	if !inspectionRowsContain(rows, "filter", "planned", "true") {
		t.Fatalf("rows missing filter planned row: %#v", rows)
	}
	if !inspectionRowsContain(rows, "filter", "execution_capability", "true") {
		t.Fatalf("rows missing filter execution capability row: %#v", rows)
	}
	if !inspectionRowsContain(rows, "route", "path", string(ExecutionPathDirectQIAB)) {
		t.Fatalf("rows missing runtime route: %#v", rows)
	}
}

func TestSQLInspectionRowsDoNotDuplicateRuntimeDiagnostics(t *testing.T) {
	diagnostic := qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedJoin, qsbridge.PhaseExecute, "join blocked")
	result := SQLInspectionResult{
		Prepared:    qsbridge.PreparedPlan{Kind: qsbridge.QueryKindSelect},
		Diagnostics: qsbridge.DiagnosticSet{diagnostic},
		Runtime: ExecutionInspection{
			Route:       DirectQIABRoute(),
			Diagnostics: qsbridge.DiagnosticSet{diagnostic},
		},
	}

	rows := result.Rows()

	count := 0
	for _, row := range rows {
		if row.Section == "diagnostic" && row.Value == string(qsbridge.DiagnosticUnsupportedJoin) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("diagnostic row count = %d, want 1: %#v", count, rows)
	}
}

func TestExecutionInspectionRowsIncludeRelationshipAdapterContract(t *testing.T) {
	plan := PlanRelationshipJoins([]qsbridge.JoinEdge{{
		Left:  qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "c"}, Name: "cust_id"},
		Right: qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "o"}, Name: "cust_id"},
		Kind:  qsbridge.JoinKindInner,
		Encoding: qsbridge.RelationshipEncodingProfile{
			Kind: qsbridge.RelationshipEncodingVector,
			Capabilities: qsbridge.RelationshipCapabilities{
				qsbridge.RelationshipCapabilityParentLookup,
				qsbridge.RelationshipCapabilityJoinReduction,
			},
		},
	}})
	inspection := ExecutionInspection{JoinPlan: plan}

	rows := inspection.Rows()
	if !inspectionRowsContain(rows, "relationship_adapter", "kind", "relationship_vector") {
		t.Fatalf("rows missing relationship adapter kind: %#v", rows)
	}
	if !inspectionRowsContain(rows, "relationship_adapter", "edge_count", "1") {
		t.Fatalf("rows missing relationship adapter edge count: %#v", rows)
	}
	if !inspectionRowsContain(rows, "relationship_adapter", "first_edge", "c.cust_id -> o.cust_id") {
		t.Fatalf("rows missing relationship adapter first edge: %#v", rows)
	}
	if !inspectionRowsContain(rows, "relationship_adapter", "intent", "reduce") {
		t.Fatalf("rows missing relationship adapter intent: %#v", rows)
	}
	if !inspectionRowsContain(rows, "relationship_vector_coverage", "read_001.scope", string(qsbridge.RelationshipVectorProjectionScopePredicateWindow)) {
		t.Fatalf("rows missing relationship vector coverage scope: %#v", rows)
	}
	if !inspectionRowsContain(rows, "relationship_vector_coverage", "read_001.expected_status", string(qsbridge.RelationshipVectorProjectionCoverageComplete)) {
		t.Fatalf("rows missing relationship vector expected coverage: %#v", rows)
	}
	if !inspectionRowsContain(rows, "relationship_vector_coverage", "read_001.verify", "true") {
		t.Fatalf("rows missing relationship vector coverage verification: %#v", rows)
	}
	if !inspectionRowsContain(rows, "relationship_vector_coverage", "read_001.recovery_policy", string(qsbridge.RelationshipVectorProjectionRecoveryBroadenAndIntersect)) {
		t.Fatalf("rows missing relationship vector recovery policy: %#v", rows)
	}
}

func TestExecutionInspectionRowsExposeRelationshipAdapterIntentFamilies(t *testing.T) {
	tests := []struct {
		name         string
		joinKind     qsbridge.JoinKind
		capabilities qsbridge.RelationshipCapabilities
		want         string
	}{
		{
			name:         "inner reduce",
			joinKind:     qsbridge.JoinKindInner,
			capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityJoinReduction},
			want:         "reduce",
		},
		{
			name:         "outer null extension",
			joinKind:     qsbridge.JoinKindLeftOuter,
			capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityNullExtension},
			want:         "null_extend",
		},
		{
			name:         "semi",
			joinKind:     qsbridge.JoinKindInner,
			capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilitySemiJoin},
			want:         "semi",
		},
		{
			name:         "anti",
			joinKind:     qsbridge.JoinKindInner,
			capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityAntiJoinDifference},
			want:         "anti",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := PlanRelationshipJoins([]qsbridge.JoinEdge{{
				Left:  qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "l"}, Name: "id"},
				Right: qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "r"}, Name: "id"},
				Kind:  tt.joinKind,
				Encoding: qsbridge.RelationshipEncodingProfile{
					Kind:         qsbridge.RelationshipEncodingVector,
					Capabilities: tt.capabilities,
				},
			}})
			rows := (ExecutionInspection{JoinPlan: plan}).Rows()
			if !inspectionRowsContain(rows, "relationship_adapter", "intent", tt.want) {
				t.Fatalf("rows missing relationship adapter intent %q: %#v", tt.want, rows)
			}
		})
	}
}

func inspectionRowsContain(rows []ExecutionInspectionRow, section string, name string, value string) bool {
	for _, row := range rows {
		if row.Section == section && row.Name == name && row.Value == value {
			return true
		}
	}
	return false
}

func inspectionRowsContainDetail(rows []ExecutionInspectionRow, section string, name string, detail string) bool {
	for _, row := range rows {
		if row.Section == section && row.Name == name && row.Detail == detail {
			return true
		}
	}
	return false
}

func resultChunkContains(chunk qsbridge.ResultChunk, section string, name string, value string) bool {
	for _, row := range chunk.Rows {
		if len(row) != 4 {
			continue
		}
		if row[0].Value == section && row[1].Value == name && row[2].Value == value {
			return true
		}
	}
	return false
}
