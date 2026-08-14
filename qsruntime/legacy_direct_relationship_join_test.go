package qsruntime

import (
	"context"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestLegacyDirectRelationshipFragmentTargetsTableRoleRequiresMatchingRole(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	fragment := qsbridge.QuantaQueryFragment{
		Index: "nation",
		Role:  "n1",
		Field: "n_name",
	}
	if !legacyDirectRelationshipFragmentTargetsTableRole(request, fragment, "nation", "n1") {
		t.Fatalf("fragment should target matching role n1")
	}
	if legacyDirectRelationshipFragmentTargetsTableRole(request, fragment, "nation", "n2") {
		t.Fatalf("fragment with role n1 should not target repeated alias role n2")
	}
}

func TestLegacyDirectRelationshipRownumsAllowsNilBitmap(t *testing.T) {
	if rownums := legacyDirectRelationshipRownums(nil); len(rownums) != 0 {
		t.Fatalf("rownums = %#v, want empty for nil bitmap", rownums)
	}
}

func TestLegacyDirectRelationshipGraphReductionSummaryProbes(t *testing.T) {
	summary := legacyDirectRelationshipGraphReductionSummary{}
	edge := legacyDirectRelationshipEdge{
		parentRole:  "o",
		parentTable: "orders",
		childRole:   "l",
		childTable:  "lineitem",
	}
	summary.record(2, 1, 1, edge, 3, 5, 2, 7*time.Millisecond, legacyDirectRelationshipReduceTiming{
		projectionElapsed:                  time.Millisecond,
		parentKeyElapsed:                   2 * time.Millisecond,
		reverseArtifactElapsed:             3 * time.Millisecond,
		reverseArtifactClientRPCElapsed:    4 * time.Millisecond,
		reverseArtifactClientRPCMaxElapsed: 5 * time.Millisecond,
		valueVectorElapsed:                 6 * time.Millisecond,
		batchEqualElapsed:                  8 * time.Millisecond,
		intersectElapsed:                   9 * time.Millisecond,
		pairElapsed:                        10 * time.Millisecond,
		projectionRows:                     11,
		reverseArtifactSourceValues:        12,
		reverseArtifactCandidateRows:       13,
		reverseArtifactNarrowedRows:        14,
		matchedRows:                        15,
		valueVectorColumnIDElapsed:         17 * time.Millisecond,
		valueVectorReadElapsed:             18 * time.Millisecond,
		valueVectorPairElapsed:             19 * time.Millisecond,
		valueVectorChildRows:               20,
		valueVectorValues:                  21,
		valueVectorExists:                  22,
		valueVectorParentMisses:            23,
	}, 16*time.Millisecond, 2)

	probes := summary.probes()
	assertExecutionProbe(t, probes, "relationship_join", "graph_reduction_edges_evaluated", "1")
	assertExecutionProbe(t, probes, "relationship_join", "graph_reduction_parent_rows_seen", "3")
	assertExecutionProbe(t, probes, "relationship_join", "graph_reduction_child_rows_seen", "5")
	assertExecutionProbe(t, probes, "relationship_join", "graph_reduction_joined_rows_seen", "2")
	assertExecutionProbe(t, probes, "relationship_join", "graph_reduction_projection_rows", "11")
	assertExecutionProbe(t, probes, "relationship_join", "graph_reduction_value_vector_child_rows", "20")
	assertExecutionProbe(t, probes, "relationship_join", "graph_reduction_value_vector_parent_misses", "23")
	assertExecutionProbe(t, probes, "relationship_join", "phase_graph_reduction_edge_reduce_total_elapsed", "7ms")
	assertExecutionProbe(t, probes, "relationship_join", "phase_graph_reduction_reverse_artifact_rpc_total_elapsed", "4ms")
	assertExecutionProbe(t, probes, "relationship_join", "phase_graph_reduction_value_vector_read_total_elapsed", "18ms")
	assertExecutionProbe(t, probes, "relationship_join", "phase_graph_reduction_max_child_retain_elapsed", "16ms")
	assertExecutionProbe(t, probes, "relationship_join", "graph_reduction_max_edge_reduce", "iter=1 edge=1 input=2 o:orders[3] -> l:lineitem[5]")
	assertExecutionProbeName(t, probes, "relationship_join", "graph_reduction_edge_summary_1")
}

func TestLegacyDirectRelationshipGraphScratchpadReusesAlignedParentRows(t *testing.T) {
	edge := legacyDirectRelationshipEdge{
		parentRole:  "o",
		parentTable: "orders",
		parentField: "o_orderkey",
		childRole:   "l",
		childTable:  "lineitem",
		childField:  "l_orderkey",
	}
	scratchpad := newLegacyDirectRelationshipGraphScratchpad(map[string][]qsbridge.QuantaRownum{
		"o": {11, 12},
		"l": {101, 102},
	}, []legacyDirectRelationshipEdge{edge})
	scratchpad.storeAlignedParentRows(edge, []qsbridge.QuantaRownum{101, 102}, []legacyDirectRelationshipPair{
		{child: 101, parent: 11},
		{child: 102, parent: 12},
	})

	parentRows, ok := scratchpad.alignedParentRows(edge, []qsbridge.QuantaRownum{101, 102})
	if !ok {
		t.Fatal("alignedParentRows hit = false, want true")
	}
	if !reflect.DeepEqual(parentRows, []qsbridge.QuantaRownum{11, 12}) {
		t.Fatalf("parentRows = %#v, want [11 12]", parentRows)
	}
	reorderedParentRows, ok := scratchpad.alignedParentRows(edge, []qsbridge.QuantaRownum{102, 101, 102})
	if !ok {
		t.Fatal("alignedParentRows hit = false for reordered child rows, want true")
	}
	if !reflect.DeepEqual(reorderedParentRows, []qsbridge.QuantaRownum{12, 11, 12}) {
		t.Fatalf("reordered parentRows = %#v, want [12 11 12]", reorderedParentRows)
	}
	if _, ok := scratchpad.alignedParentRows(edge, []qsbridge.QuantaRownum{103}); ok {
		t.Fatal("alignedParentRows hit = true for unknown child row, want false")
	}
}

func TestLegacyDirectRelationshipGraphAlignedRownumsUsesReductionScratchpad(t *testing.T) {
	calls := 0
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		RelationshipProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI:   testRelationshipVectorBSI(map[uint64]int64{}),
			Calls: &calls,
		},
	}
	edge := legacyDirectRelationshipEdge{
		parentRole:  "o",
		parentTable: "orders",
		parentField: "o_orderkey",
		childRole:   "l",
		childTable:  "lineitem",
		childField:  "l_orderkey",
	}
	scratchpad := newLegacyDirectRelationshipGraphScratchpad(map[string][]qsbridge.QuantaRownum{
		"o": {11, 12},
		"l": {101, 102},
	}, []legacyDirectRelationshipEdge{edge})
	scratchpad.storeAlignedParentRows(edge, []qsbridge.QuantaRownum{101, 102}, []legacyDirectRelationshipPair{
		{child: 101, parent: 11},
		{child: 102, parent: 12},
	})

	aligned, probes, diagnostics, err := executor.legacyDirectRelationshipGraphAlignedRownums(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}),
		"lineitem",
		[]qsbridge.QuantaRownum{102, 101, 102},
		[]legacyDirectRelationshipEdge{edge},
		scratchpad,
	)
	if err != nil {
		t.Fatalf("aligned rownums error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if calls != 0 {
		t.Fatalf("projection calls = %d, want 0", calls)
	}
	if !reflect.DeepEqual(aligned["o"], []qsbridge.QuantaRownum{12, 11, 12}) {
		t.Fatalf("aligned parent rows = %#v, want [12 11 12]", aligned["o"])
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_alignment_edge_1_source", "reduction_scratchpad")
	assertExecutionProbe(t, probes, "relationship_join", "graph_alignment_edge_1_child_rows", "3")
	assertExecutionProbe(t, probes, "relationship_join", "graph_alignment_edge_1_parent_rows", "3")
	assertExecutionProbeName(t, probes, "relationship_join", "phase_graph_alignment_edge_1_elapsed")
}

func TestLegacyDirectRelationshipGraphAlignedRownumsStopsAtRequiredRoles(t *testing.T) {
	calls := 0
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		RelationshipProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI:   testRelationshipVectorBSI(map[uint64]int64{}),
			Calls: &calls,
		},
	}
	orderLine := legacyDirectRelationshipEdge{
		parentRole:  "o",
		parentTable: "orders",
		parentField: "o_orderkey",
		childRole:   "l",
		childTable:  "lineitem",
		childField:  "l_orderkey",
	}
	customerOrder := legacyDirectRelationshipEdge{
		parentRole:  "c",
		parentTable: "customer",
		parentField: "c_custkey",
		childRole:   "o",
		childTable:  "orders",
		childField:  "o_custkey",
	}
	scratchpad := newLegacyDirectRelationshipGraphScratchpad(map[string][]qsbridge.QuantaRownum{
		"c": {31},
		"o": {11, 12},
		"l": {101, 102},
	}, []legacyDirectRelationshipEdge{orderLine, customerOrder})
	scratchpad.storeAlignedParentRows(orderLine, []qsbridge.QuantaRownum{101, 102}, []legacyDirectRelationshipPair{
		{child: 101, parent: 11},
		{child: 102, parent: 12},
	})

	aligned, probes, diagnostics, err := executor.legacyDirectRelationshipGraphAlignedRownumsForRoles(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}),
		"lineitem",
		[]qsbridge.QuantaRownum{101, 102},
		[]legacyDirectRelationshipEdge{orderLine, customerOrder},
		scratchpad,
		map[string]struct{}{"l": {}, "o": {}},
	)
	if err != nil {
		t.Fatalf("aligned rownums error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if calls != 0 {
		t.Fatalf("projection calls = %d, want 0", calls)
	}
	if _, ok := aligned["c"]; ok {
		t.Fatalf("aligned customer role present, want skipped; aligned=%#v", aligned)
	}
	if !reflect.DeepEqual(aligned["o"], []qsbridge.QuantaRownum{11, 12}) {
		t.Fatalf("aligned order rows = %#v, want [11 12]", aligned["o"])
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_alignment_required_roles", "l,o")
	assertExecutionProbe(t, probes, "relationship_join", "graph_alignment_edge_1_source", "reduction_scratchpad")
}

func TestLegacyDirectRelationshipExpandRequiredAlignmentRolesIncludesPathConnectors(t *testing.T) {
	expanded := legacyDirectRelationshipExpandRequiredAlignmentRoles(
		map[string]struct{}{"n": {}},
		"l",
		[]legacyDirectRelationshipEdge{
			{parentRole: "o", childRole: "l"},
			{parentRole: "c", childRole: "o"},
			{parentRole: "n", childRole: "c"},
		},
	)
	if got := legacyDirectRelationshipRoleSetDebug(expanded); got != "c,l,n,o" {
		t.Fatalf("expanded roles = %q, want c,l,n,o", got)
	}
}

func TestLegacyDirectRelationshipAggregateResultMaterializesJoinedRows(t *testing.T) {
	orders := qsbridge.TableInstance{Table: "orders_qa", Alias: "o"}
	orderID := qsbridge.FieldRef{Table: orders, Name: "order_id", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index: "orders_qa",
			Field: "order_id",
			Type:  qsbridge.DataTypeInt,
		}},
	})
	request.SQLAggregates = []qsbridge.Aggregate{
		{Function: "sum", Input: qsbridge.Field(orderID), Alias: "order_total", Type: qsbridge.DataTypeFloat},
		{Function: "avg", Input: qsbridge.Field(orderID), Alias: "order_avg", Type: qsbridge.DataTypeFloat},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field: request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueInt, Value: int64(1001)},
						{Kind: qsbridge.ValueInt, Value: int64(1002)},
						{Kind: qsbridge.ValueInt, Value: int64(2001)},
					},
				}},
			}, nil, nil
		}),
	}
	result, err := executor.legacyDirectRelationshipAggregateResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "orders_qa", parentTable: "customers_qa"},
		[]qsbridge.QuantaRownum{11, 12, 21},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 1}, {child: 21, parent: 2}},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("aggregate result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	assertExecutionProbeName(t, result.Probes, "relationship_join", "phase_child_materialization_elapsed")
	assertExecutionProbeName(t, result.Probes, "relationship_join", "phase_parent_materialization_elapsed")
	assertExecutionProbeName(t, result.Probes, "relationship_join", "phase_assemble_rows_elapsed")
	assertExecutionProbe(t, result.Probes, "relationship_join", "child_materialization_rows", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "child_materialization_unique_rows", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "child_materialization_fields", "1")
	assertExecutionProbe(t, result.Probes, "relationship_join", "parent_materialization_rows", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "parent_materialization_unique_rows", "2")
	assertExecutionProbe(t, result.Probes, "relationship_join", "parent_materialization_fields", "0")
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 2 {
		t.Fatalf("rows = %#v, want one row with two aggregate values", chunk.Rows)
	}
	if got := chunk.Rows[0][0].Value; got != float64(4004) {
		t.Fatalf("sum = %#v, want 4004", got)
	}
	if got := chunk.Rows[0][1].Value; got != float64(4004)/3 {
		t.Fatalf("avg = %#v, want %v", got, float64(4004)/3)
	}
}

func TestLegacyDirectRelationshipMaterializedValuesUsesProjectionKernel(t *testing.T) {
	field := qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt, Visible: true}
	called := false
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materialization: qsruntimeMaterializationKernelFunc(func(_ context.Context, request qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			called = true
			if request.RequestCount() != 1 {
				t.Fatalf("request count = %d, want one", request.RequestCount())
			}
			materialization := request.Requests[0]
			if materialization.Index != "orders" || len(materialization.Rownums) != 2 {
				t.Fatalf("materialization request = %#v, want orders two-row request", materialization)
			}
			return qsbridge.ProjectionMaterializationKernelResult{
				ID: request.ID,
				Results: []qsbridge.ProjectionMaterializationResult{{
					ID:      materialization.DependencyID,
					Request: materialization,
					RowSet: qsbridge.QuantaProjectedRowSet{
						Index:   "orders",
						Rownums: []qsbridge.QuantaRownum{7, 8},
						ProjectionVectors: []qsbridge.QuantaProjectionVector{{
							Field: field,
							Values: []qsbridge.ResultCell{
								{Kind: qsbridge.ValueInt, Value: int64(1007)},
								{Kind: qsbridge.ValueInt, Value: int64(1008)},
							},
						}},
					},
				}},
			}, nil
		}),
	}

	values, diagnostics, err := executor.legacyDirectRelationshipMaterializedValues(
		context.Background(),
		executor.projectionMaterializationKernel(),
		"orders",
		[]qsbridge.QuantaRownum{7, 8},
		[]qsbridge.QuantaProjectionField{field},
		qsbridge.QuantaMaterializationRequest{},
	)
	if err != nil {
		t.Fatalf("materialized values error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !called {
		t.Fatalf("projection materialization kernel was not called")
	}
	fieldValues := values[legacyDirectRelationshipProjectionFieldKey(field)]
	if got := fieldValues[8].Value; got != int64(1008) {
		t.Fatalf("row 8 value = %#v, want 1008", got)
	}
}

func TestLegacyDirectRelationshipCountAggregateAppliesResidualPredicates(t *testing.T) {
	partName := qsbridge.QuantaProjectionField{
		Index:   "part",
		Role:    "p",
		Field:   "p_name",
		Type:    qsbridge.DataTypeString,
		Visible: false,
	}
	partField := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
		Name:  "p_name",
		Type:  qsbridge.DataTypeString,
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{partName},
	})
	request.Materialization = qsbridge.QuantaMaterializationRequest{
		ProjectionFields: []qsbridge.QuantaProjectionField{partName},
	}
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpLike, qsbridge.Field(partField), qsbridge.Literal(qsbridge.ValueString, "%green%")),
		Placement: qsbridge.PredicateResidualScan,
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "line_count", Type: qsbridge.DataTypeInt}}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			if len(request.ProjectionFields) == 0 {
				return rowSet, nil, nil
			}
			rowSet.ProjectionVectors = []qsbridge.QuantaProjectionVector{{
				Field: request.ProjectionFields[0],
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueString, Value: "green part"},
					{Kind: qsbridge.ValueString, Value: "red part"},
				},
			}}
			return rowSet, nil, nil
		}),
	}
	result, err := executor.legacyDirectRelationshipAggregateResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "lineitem", childRole: "l", parentTable: "part", parentRole: "p"},
		[]qsbridge.QuantaRownum{10, 11},
		[]legacyDirectRelationshipPair{{child: 10, parent: 1}, {child: 11, parent: 2}},
		ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{Index: "lineitem", Rownums: []qsbridge.QuantaRownum{10, 11}}, Count: 2},
	)
	if err != nil {
		t.Fatalf("aggregate result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 || chunk.Rows[0][0].Value != int64(1) {
		t.Fatalf("rows = %#v, want count 1 after residual filter", chunk.Rows)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "aggregate_residual_rows_before", "2")
	assertExecutionProbe(t, result.Probes, "relationship_join", "aggregate_residual_rows_after", "1")
}

func TestLegacyDirectRelationshipFragmentsForTableUsesRoleWhenPresent(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{
		{Index: "nation", Role: "nation_1", Field: "n_name", Literal: qsbridge.LiteralExpr{Kind: qsbridge.ValueString, Value: "FRANCE"}, HasLiteral: true},
		{Index: "nation", Role: "nation_2", Field: "n_name", Literal: qsbridge.LiteralExpr{Kind: qsbridge.ValueString, Value: "GERMANY"}, HasLiteral: true},
	}})

	left := legacyDirectRelationshipFragmentsForTable(request, "nation", "nation_1")
	right := legacyDirectRelationshipFragmentsForTable(request, "nation", "nation_2")

	if len(left) != 1 || left[0].Role != "nation_1" {
		t.Fatalf("left fragments = %#v, want only nation_1", left)
	}
	if len(right) != 1 || right[0].Role != "nation_2" {
		t.Fatalf("right fragments = %#v, want only nation_2", right)
	}
}

func TestLegacyDirectRelationshipEdgeUsesTableInstanceRoles(t *testing.T) {
	edge := legacyDirectRelationshipEdge{
		childRole:   legacyDirectRelationshipTableRoleKey(qsbridge.TableInstance{ID: "lineitem_1", Table: "lineitem", Alias: "l"}),
		childTable:  "lineitem",
		parentRole:  legacyDirectRelationshipTableRoleKey(qsbridge.TableInstance{ID: "orders_1", Table: "orders", Alias: "o"}),
		parentTable: "orders",
	}

	if edge.childKey() != "l" || edge.parentKey() != "o" {
		t.Fatalf("role keys = %q/%q, want l/o", edge.childKey(), edge.parentKey())
	}
}

func TestLegacyDirectRelationshipUniqueSourceRoleKeyRequiresOneRole(t *testing.T) {
	single := ExecutionRequest{Sources: []qsbridge.TableInstance{{ID: "region_1", Table: "region", Alias: "r"}}}
	if got := legacyDirectRelationshipUniqueSourceRoleKey(single, "region"); got != "r" {
		t.Fatalf("single role = %q, want r", got)
	}

	repeated := ExecutionRequest{Sources: []qsbridge.TableInstance{
		{ID: "nation_1", Table: "nation", Alias: "n1"},
		{ID: "nation_2", Table: "nation", Alias: "n2"},
	}}
	if got := legacyDirectRelationshipUniqueSourceRoleKey(repeated, "nation"); got != "" {
		t.Fatalf("repeated role = %q, want empty ambiguous fallback", got)
	}
}

func TestLegacyDirectRelationshipRowsFromParentMapPreservesChildCandidateDomain(t *testing.T) {
	parentRows, joined, pairs := legacyDirectRelationshipRowsFromParentMap(
		[]qsbridge.QuantaRownum{10, 11, 12, 13},
		map[qsbridge.QuantaRownum]qsbridge.QuantaRownum{
			10: 2,
			11: 2,
			12: 4,
		},
	)
	if len(parentRows) != 2 || parentRows[0] != 2 || parentRows[1] != 4 {
		t.Fatalf("parent rows = %#v, want unique parents [2 4]", parentRows)
	}
	if len(joined) != 3 || joined[0] != 10 || joined[1] != 11 || joined[2] != 12 {
		t.Fatalf("joined rows = %#v, want child-domain rows [10 11 12]", joined)
	}
	if len(pairs) != 3 {
		t.Fatalf("pairs = %#v, want 3 pairs", pairs)
	}
	if pairs[0].child != 10 || pairs[0].parent != 2 || pairs[2].child != 12 || pairs[2].parent != 4 {
		t.Fatalf("pairs = %#v, want child-parent mapping preserved", pairs)
	}
}

func TestLegacyDirectRelationshipCandidateRownumsDefersEmptyResidualOnlyJoinCandidateSet(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Joins = []qsbridge.JoinEdge{{Kind: qsbridge.JoinKindInner}}
	request.Predicates = []qsbridge.Predicate{{Placement: qsbridge.PredicateResidualScan}}
	request = request.WithCandidateSet(qsbridge.QuantaCandidateSet{Index: "part"})

	if rows, ok := legacyDirectRelationshipCandidateRownumsForTable(request, "part"); ok || len(rows) != 0 {
		t.Fatalf("candidate rows = %#v, ok=%t; want deferred empty candidate set", rows, ok)
	}

	request.Query.Fragments = []qsbridge.QuantaQueryFragment{{Index: "part", Field: "p_brand"}}
	if rows, ok := legacyDirectRelationshipCandidateRownumsForTable(request, "part"); !ok || len(rows) != 0 {
		t.Fatalf("candidate rows = %#v, ok=%t; want explicit empty candidate set when pushdown remains", rows, ok)
	}
}

func TestLegacyDirectRelationshipProjectionResultSkipsUnusedJoinKeys(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	pBrand := qsbridge.FieldRef{Table: part, Name: "p_brand", Type: qsbridge.DataTypeString}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.ProjectionOrder = []qsbridge.FieldRef{pBrand, psSuppkey}
	var materializations []qsbridge.QuantaMaterializationRequest
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materializations = append(materializations, request)
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
				for i := range request.Rownums {
					switch field.Field {
					case "p_brand":
						values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "Brand#11"})
					case "ps_suppkey":
						values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(100 + i)})
					default:
						t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
					Field:  field,
					Values: values,
				})
			}
			return rowSet, nil, nil
		}),
	}
	result, err := executor.legacyDirectRelationshipProjectionResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "partsupp", parentTable: "part"},
		[]qsbridge.QuantaRownum{11, 12},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 2}},
		ExecutionResult{Count: 2},
	)
	if err != nil {
		t.Fatalf("projection result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if len(materializations) != 2 {
		t.Fatalf("materializations = %#v, want child and parent requests", materializations)
	}
	assertLegacyDirectRelationshipMaterializationFields(t, materializations, "partsupp", []string{"ps_suppkey"})
	assertLegacyDirectRelationshipMaterializationFields(t, materializations, "part", []string{"p_brand"})
	assertExecutionProbe(t, result.Probes, "relationship_join", "projection_limit_pushed", "false")
	assertExecutionProbe(t, result.Probes, "relationship_join", "child_materialization_fields", "1")
	assertExecutionProbe(t, result.Probes, "relationship_join", "parent_materialization_fields", "1")
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 || len(chunk.Rows[0]) != 2 {
		t.Fatalf("rows = %#v, want two projected rows", chunk.Rows)
	}
}

func TestLegacyDirectRelationshipProjectionResultPushesUnorderedLimitBeforeMaterialization(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	pBrand := qsbridge.FieldRef{Table: part, Name: "p_brand", Type: qsbridge.DataTypeString}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.ProjectionOrder = []qsbridge.FieldRef{pBrand, psSuppkey}
	request.Result = qsbridge.ResultShape{Offset: 1, Limit: 1}
	var materializations []qsbridge.QuantaMaterializationRequest
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materializations = append(materializations, request)
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
				for i := range request.Rownums {
					switch field.Field {
					case "p_brand":
						values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "Brand#11"})
					case "ps_suppkey":
						values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(100 + i)})
					default:
						t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
					Field:  field,
					Values: values,
				})
			}
			return rowSet, nil, nil
		}),
	}
	result, err := executor.legacyDirectRelationshipProjectionResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "partsupp", parentTable: "part"},
		[]qsbridge.QuantaRownum{11, 12, 13},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 2}, {child: 13, parent: 3}},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("projection result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "projection_limit_pushed", "true")
	assertExecutionProbe(t, result.Probes, "relationship_join", "child_materialization_rows", "1")
	assertExecutionProbe(t, result.Probes, "relationship_join", "parent_materialization_rows", "1")
	assertLegacyDirectRelationshipMaterializationRownums(t, materializations, "partsupp", []qsbridge.QuantaRownum{12})
	assertLegacyDirectRelationshipMaterializationRownums(t, materializations, "part", []qsbridge.QuantaRownum{2})
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 1 {
		t.Fatalf("rows = %#v, want one projected row after pushed limit", chunk.Rows)
	}
}

func TestLegacyDirectRelationshipProjectionResultOrdersBeforeLimit(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	pPartkey := qsbridge.FieldRef{Table: part, Name: "p_partkey", Type: qsbridge.DataTypeInt}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.ProjectionOrder = []qsbridge.FieldRef{pPartkey, psSuppkey}
	request.OrderBy = []qsbridge.SortSpec{
		{Expr: qsbridge.Field(pPartkey), Direction: qsbridge.SortAscending},
		{Expr: qsbridge.Field(psSuppkey), Direction: qsbridge.SortAscending},
	}
	request.Result = qsbridge.ResultShape{Offset: 0, Limit: 2}
	var materializations []qsbridge.QuantaMaterializationRequest
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materializations = append(materializations, request)
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "p_partkey":
						values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum)})
					case "ps_suppkey":
						suppkeys := map[qsbridge.QuantaRownum]int64{11: 80, 12: 8, 13: 5, 14: 30}
						values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: suppkeys[rownum]})
					default:
						t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
					Field:  field,
					Values: values,
				})
			}
			return rowSet, nil, nil
		}),
	}
	result, err := executor.legacyDirectRelationshipProjectionResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "partsupp", parentTable: "part"},
		[]qsbridge.QuantaRownum{11, 12, 13, 14},
		[]legacyDirectRelationshipPair{{child: 11, parent: 4}, {child: 12, parent: 7}, {child: 13, parent: 4}, {child: 14, parent: 4}},
		ExecutionResult{Count: 4},
	)
	if err != nil {
		t.Fatalf("projection result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "projection_limit_pushed", "false")
	assertLegacyDirectRelationshipMaterializationRownums(t, materializations, "partsupp", []qsbridge.QuantaRownum{11, 12, 13, 14})
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two ordered rows", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != int64(4) || chunk.Rows[0][1].Value != int64(5) {
		t.Fatalf("first row = %#v, want part 4 suppkey 5", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != int64(4) || chunk.Rows[1][1].Value != int64(30) {
		t.Fatalf("second row = %#v, want part 4 suppkey 30", chunk.Rows[1])
	}
}

func TestLegacyDirectRelationshipProjectionResultFiltersResidualsBeforeLimit(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	pPartkey := qsbridge.FieldRef{Table: part, Name: "p_partkey", Type: qsbridge.DataTypeInt}
	pType := qsbridge.FieldRef{Table: part, Name: "p_type", Type: qsbridge.DataTypeString}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.ProjectionOrder = []qsbridge.FieldRef{pPartkey, pType, psSuppkey}
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpNotLike, qsbridge.Field(pType), qsbridge.Literal(qsbridge.ValueString, "MEDIUM POLISHED%")),
		Placement: qsbridge.PredicateResidualScan,
	}}
	request.Result = qsbridge.ResultShape{Offset: 0, Limit: 1}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "p_partkey":
						values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum)})
					case "p_type":
						types := map[qsbridge.QuantaRownum]string{1: "MEDIUM POLISHED BRASS", 2: "SMALL PLATED BRASS"}
						values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: types[rownum]})
					case "ps_suppkey":
						values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(100 + rownum)})
					default:
						t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
					Field:  field,
					Values: values,
				})
			}
			return rowSet, nil, nil
		}),
	}
	result, err := executor.legacyDirectRelationshipProjectionResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "partsupp", parentTable: "part"},
		[]qsbridge.QuantaRownum{11, 12},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 2}},
		ExecutionResult{Count: 2},
	)
	if err != nil {
		t.Fatalf("projection result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "projection_limit_pushed", "false")
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 1 {
		t.Fatalf("rows = %#v, want one surviving row", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != int64(2) || chunk.Rows[0][1].Value != "SMALL PLATED BRASS" {
		t.Fatalf("row = %#v, want residual survivor for part 2", chunk.Rows[0])
	}
}

func TestLegacyDirectRelationshipPostReductionFieldsIncludeJoinOnResiduals(t *testing.T) {
	supplier := qsbridge.TableInstance{Table: "supplier", Alias: "s"}
	customer := qsbridge.TableInstance{Table: "customer", Alias: "c"}
	nation := qsbridge.TableInstance{Table: "nation", Alias: "n"}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(qsbridge.FieldRef{Table: nation, Name: "n_name", Type: qsbridge.DataTypeString})}
	request.Joins = []qsbridge.JoinEdge{{
		On: []qsbridge.Predicate{{
			Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(qsbridge.FieldRef{Table: supplier, Name: "s_nationkey", Type: qsbridge.DataTypeInt}), qsbridge.Field(qsbridge.FieldRef{Table: customer, Name: "c_nationkey", Type: qsbridge.DataTypeInt})),
			Placement: qsbridge.PredicateResidualJoin,
			Scope:     qsbridge.PredicateScopeOn,
		}},
	}}
	fields := []qsbridge.QuantaProjectionField{
		{Index: "nation", Field: "n_name", Type: qsbridge.DataTypeString},
		{Index: "supplier", Field: "s_nationkey", Type: qsbridge.DataTypeInt},
		{Index: "customer", Field: "c_nationkey", Type: qsbridge.DataTypeInt},
		{Index: "lineitem", Field: "l_orderkey", Type: qsbridge.DataTypeInt},
	}

	filtered := legacyDirectRelationshipPostReductionFields(request, fields)
	got := make(map[string]bool, len(filtered))
	for _, field := range filtered {
		got[field.Index+"."+field.Field] = true
	}
	for _, want := range []string{"nation.n_name", "supplier.s_nationkey", "customer.c_nationkey"} {
		if !got[want] {
			t.Fatalf("post-reduction fields = %#v, missing %s", filtered, want)
		}
	}
	if got["lineitem.l_orderkey"] {
		t.Fatalf("post-reduction fields = %#v, did not expect unrelated join key", filtered)
	}
}

func TestLegacyDirectRelationshipVectorProjectionWindowKeepsShardWindow(t *testing.T) {
	fromTime, toTime := legacyDirectRelationshipVectorProjectionWindow(qsbridge.QuantaMaterializationRequest{
		Index:           "lineitem",
		FromEpochMillis: 820454400000,
		ToEpochMillis:   828316800000,
		Rownums:         []qsbridge.QuantaRownum{1, 2, 3},
	})

	if fromTime != 820454400000000000 {
		t.Fatalf("from nanos = %d, want 820454400000000000", fromTime)
	}
	if toTime != 828316800000000000 {
		t.Fatalf("to nanos = %d, want 828316800000000000", toTime)
	}
}

func TestLegacyDirectRelationshipParentMapSkipsProjectionForEmptyChildRows(t *testing.T) {
	executor := LegacyDirectRelationshipVectorJoinExecutor{}
	parentByChild, diagnostics, err := executor.legacyDirectRelationshipParentMap(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}), legacyDirectRelationshipEdge{
		childTable: "lineitem",
		childField: "l_partkey",
	}, nil)

	if err != nil {
		t.Fatalf("parent map error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want no blockers", diagnostics)
	}
	if len(parentByChild) != 0 {
		t.Fatalf("parent map len = %d, want 0", len(parentByChild))
	}
}

func TestLegacyDirectRelationshipReduceSkipsProjectionForEmptyInputs(t *testing.T) {
	executor := LegacyDirectRelationshipVectorJoinExecutor{}
	joined, pairs, diagnostics, err := executor.legacyDirectRelationshipReduce(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}), legacyDirectRelationshipEdge{
		childTable: "lineitem",
		childField: "l_partkey",
	}, []qsbridge.QuantaRownum{1}, nil)

	if err != nil {
		t.Fatalf("reduce error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want no blockers", diagnostics)
	}
	if len(joined) != 0 {
		t.Fatalf("joined len = %d, want 0", len(joined))
	}
	if len(pairs) != 0 {
		t.Fatalf("pairs len = %d, want 0", len(pairs))
	}

	joined, pairs, diagnostics, err = executor.legacyDirectRelationshipReduce(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}), legacyDirectRelationshipEdge{
		childTable: "lineitem",
		childField: "l_partkey",
	}, nil, []qsbridge.QuantaRownum{1})

	if err != nil {
		t.Fatalf("reduce with empty parent error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("empty parent diagnostics = %#v, want no blockers", diagnostics)
	}
	if len(joined) != 0 || len(pairs) != 0 {
		t.Fatalf("empty parent joined/pairs = %d/%d, want 0/0", len(joined), len(pairs))
	}
}

func TestLegacyDirectRelationshipReduceProjectedFKBSITranslatesParentKeysToRows(t *testing.T) {
	fkBSI := roaring64.NewDefaultBSI()
	fkBSI.SetValue(101, int64(1007))
	fkBSI.SetValue(102, int64(9999))
	fkBSI.SetValue(103, int64(2009))
	parentKeyRows := map[int64]qsbridge.QuantaRownum{
		1007: 7,
		2009: 9,
	}

	joined, pairs, diagnostics := legacyDirectRelationshipReduceProjectedFKBSI(
		fkBSI,
		[]qsbridge.QuantaRownum{101, 102, 103},
		parentKeyRows,
	)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want no blockers", diagnostics)
	}
	wantJoined := []qsbridge.QuantaRownum{101, 103}
	if len(joined) != len(wantJoined) {
		t.Fatalf("joined = %#v, want %#v", joined, wantJoined)
	}
	for i := range wantJoined {
		if joined[i] != wantJoined[i] {
			t.Fatalf("joined = %#v, want %#v", joined, wantJoined)
		}
	}
	if len(pairs) != 2 {
		t.Fatalf("pairs len = %d, want 2", len(pairs))
	}
	if pairs[0].child != 101 || pairs[0].parent != 7 {
		t.Fatalf("pair 0 = %#v, want child 101 parent rownum 7", pairs[0])
	}
	if pairs[1].child != 103 || pairs[1].parent != 9 {
		t.Fatalf("pair 1 = %#v, want child 103 parent rownum 9", pairs[1])
	}
}

func TestLegacyDirectRelationshipReduceProjectedFKBSIPreservesDuplicateChildFKValues(t *testing.T) {
	fkBSI := roaring64.NewDefaultBSI()
	fkBSI.SetValue(101, int64(1007))
	fkBSI.SetValue(102, int64(1007))
	fkBSI.SetValue(103, int64(2009))
	parentKeyRows := map[int64]qsbridge.QuantaRownum{
		1007: 7,
		2009: 9,
	}

	joined, pairs, diagnostics := legacyDirectRelationshipReduceProjectedFKBSI(
		fkBSI,
		[]qsbridge.QuantaRownum{101, 102, 103},
		parentKeyRows,
	)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want no blockers", diagnostics)
	}
	wantJoined := []qsbridge.QuantaRownum{101, 102, 103}
	if len(joined) != len(wantJoined) {
		t.Fatalf("joined = %#v, want %#v", joined, wantJoined)
	}
	for i := range wantJoined {
		if joined[i] != wantJoined[i] {
			t.Fatalf("joined = %#v, want %#v", joined, wantJoined)
		}
	}
	wantPairs := []legacyDirectRelationshipPair{
		{child: 101, parent: 7},
		{child: 102, parent: 7},
		{child: 103, parent: 9},
	}
	if len(pairs) != len(wantPairs) {
		t.Fatalf("pairs = %#v, want %#v", pairs, wantPairs)
	}
	for i := range wantPairs {
		if pairs[i] != wantPairs[i] {
			t.Fatalf("pairs = %#v, want %#v", pairs, wantPairs)
		}
	}
}

func TestLegacyDirectRelationshipReduceProjectedFKBSIUsesBatchEqualForLargeChildSets(t *testing.T) {
	fkBSI := roaring64.NewDefaultBSI()
	childRows := make([]qsbridge.QuantaRownum, 0, 1200)
	for row := 1200; row >= 1; row-- {
		child := qsbridge.QuantaRownum(row)
		childRows = append(childRows, child)
		switch {
		case row%10 == 0:
			fkBSI.SetValue(uint64(child), int64(1007))
		case row%25 == 0:
			fkBSI.SetValue(uint64(child), int64(2009))
		default:
			fkBSI.SetValue(uint64(child), int64(9999))
		}
	}
	parentKeyRows := map[int64]qsbridge.QuantaRownum{
		1007: 7,
		2009: 9,
	}

	joined, pairs, timing, diagnostics := legacyDirectRelationshipReduceProjectedFKBSIWithTiming(fkBSI, childRows, parentKeyRows)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want no blockers", diagnostics)
	}
	if !timing.batchEqualUsed {
		t.Fatalf("batchEqualUsed = false, want batch-equal path")
	}
	if len(joined) == 0 || len(joined) != len(pairs) {
		t.Fatalf("joined/pairs = %d/%d, want matching non-zero counts", len(joined), len(pairs))
	}
	for i := 1; i < len(joined); i++ {
		if joined[i-1] < joined[i] {
			t.Fatalf("joined row order = %#v, want original child row order preserved", joined[:10])
		}
	}
	for _, pair := range pairs {
		if pair.child%10 == 0 {
			if pair.parent != 7 {
				t.Fatalf("pair for child %d = %#v, want parent 7", pair.child, pair)
			}
			continue
		}
		if pair.child%25 == 0 {
			if pair.parent != 9 {
				t.Fatalf("pair for child %d = %#v, want parent 9", pair.child, pair)
			}
			continue
		}
		t.Fatalf("unexpected pair = %#v", pair)
	}
}

func TestLegacyDirectRelationshipReduceProjectedFKBSIUsesValueVectorForManyParentKeys(t *testing.T) {
	fkBSI := roaring64.NewDefaultBSI()
	parentKeyRows := make(map[int64]qsbridge.QuantaRownum, 64)
	for key := int64(1000); key < 1064; key++ {
		parentKeyRows[key] = qsbridge.QuantaRownum(key - 900)
	}

	childRows := make([]qsbridge.QuantaRownum, 0, 1500)
	wantJoined := []qsbridge.QuantaRownum{}
	wantPairs := []legacyDirectRelationshipPair{}
	for row := 1500; row >= 1; row-- {
		child := qsbridge.QuantaRownum(row)
		childRows = append(childRows, child)
		parentKey := int64(9999)
		if row%10 == 0 {
			parentKey = 1000 + int64(row%64)
		} else if row%25 == 0 {
			parentKey = 1000 + int64((row+7)%64)
		}
		fkBSI.SetValue(uint64(child), parentKey)
		if parent, ok := parentKeyRows[parentKey]; ok {
			wantJoined = append(wantJoined, child)
			wantPairs = append(wantPairs, legacyDirectRelationshipPair{child: child, parent: parent})
		}
	}

	joined, pairs, timing, diagnostics := legacyDirectRelationshipReduceProjectedFKBSIWithTiming(fkBSI, childRows, parentKeyRows)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want no blockers", diagnostics)
	}
	if !timing.valueVectorUsed {
		t.Fatalf("valueVectorUsed = false, want value-vector path")
	}
	if timing.valueVectorMode != "int64" {
		t.Fatalf("valueVectorMode = %q, want int64", timing.valueVectorMode)
	}
	if timing.valueVectorChildRows != len(childRows) || timing.valueVectorValues != len(childRows) || timing.valueVectorExists != len(childRows) {
		t.Fatalf("value vector rows/values/exists = %d/%d/%d, want %d/%d/%d", timing.valueVectorChildRows, timing.valueVectorValues, timing.valueVectorExists, len(childRows), len(childRows), len(childRows))
	}
	if timing.valueVectorParentMisses != len(childRows)-len(wantJoined) {
		t.Fatalf("valueVectorParentMisses = %d, want %d", timing.valueVectorParentMisses, len(childRows)-len(wantJoined))
	}
	if timing.batchEqualUsed || timing.singleKeyEqualUsed {
		t.Fatalf("batch/single paths = %t/%t, want false/false", timing.batchEqualUsed, timing.singleKeyEqualUsed)
	}
	if len(joined) != len(wantJoined) || len(pairs) != len(wantPairs) {
		t.Fatalf("joined/pairs = %d/%d, want %d/%d", len(joined), len(pairs), len(wantJoined), len(wantPairs))
	}
	for i := range wantJoined {
		if joined[i] != wantJoined[i] {
			t.Fatalf("joined[%d] = %d, want %d", i, joined[i], wantJoined[i])
		}
		if pairs[i] != wantPairs[i] {
			t.Fatalf("pairs[%d] = %#v, want %#v", i, pairs[i], wantPairs[i])
		}
	}
}

func TestLegacyDirectRelationshipReduceUsesReverseArtifactToNarrowChildRows(t *testing.T) {
	fkBSI := roaring64.NewDefaultBSI()
	fkBSI.SetValue(10, 7)
	fkBSI.SetValue(25, 9)
	fkBSI.SetValue(100, 99)
	fkBSI.SetValue(1200, 1200)
	for row := uint64(1); row <= 1500; row++ {
		if _, ok := fkBSI.GetValue(row); ok {
			continue
		}
		fkBSI.SetValue(row, 5000+int64(row))
	}
	childRows := make([]qsbridge.QuantaRownum, 0, 1500)
	for row := 1500; row >= 1; row-- {
		childRows = append(childRows, qsbridge.QuantaRownum(row))
	}
	projectionCalls := 0
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		RelationshipProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI:   fkBSI,
			Calls: &projectionCalls,
		},
		ReverseArtifactCandidateReader: fakeLegacyDirectRelationshipVectorReverseArtifactCandidateReader{
			OK: true,
			Result: LegacyDirectRelationshipVectorReverseArtifactCandidateResult{
				Candidates: qsbridge.QuantaCandidateSet{
					Index:   "lineitem",
					Rownums: []qsbridge.QuantaRownum{10, 25},
				},
				ParentValueByChild: map[qsbridge.QuantaRownum]int64{
					10: 7,
					25: 9,
				},
				Mode:         "reverse_artifact_server",
				CacheHit:     true,
				SourceValues: 2,
				TargetRows:   2,
			},
		},
	}
	edge := legacyDirectRelationshipEdge{
		parentRole:   "o",
		parentTable:  "orders",
		parentField:  "o_orderkey",
		childRole:    "l",
		childTable:   "lineitem",
		childField:   "l_orderkey",
		capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityChildExpansion},
	}

	joined, pairs, timing, diagnostics, err := executor.legacyDirectRelationshipReduceWithProjectionRows(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}),
		edge,
		[]qsbridge.QuantaRownum{7, 9},
		childRows,
		childRows,
	)

	if err != nil {
		t.Fatalf("reduce error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want no blockers", diagnostics)
	}
	if !timing.reverseArtifactUsed {
		t.Fatal("reverseArtifactUsed = false, want true")
	}
	if timing.reverseArtifactMode != "reverse_artifact_server" || !timing.reverseArtifactCacheHit {
		t.Fatalf("reverse artifact mode/cache = %q/%t, want server/true", timing.reverseArtifactMode, timing.reverseArtifactCacheHit)
	}
	if timing.reverseArtifactCandidateRows != 2 || timing.reverseArtifactNarrowedRows != 2 {
		t.Fatalf("reverse artifact rows = %d/%d, want 2/2", timing.reverseArtifactCandidateRows, timing.reverseArtifactNarrowedRows)
	}
	if timing.projectionRows != 0 {
		t.Fatalf("projectionRows = %d, want no FK projection when artifact supplies child-parent values", timing.projectionRows)
	}
	if timing.valueVectorElapsed != 0 {
		t.Fatalf("valueVectorElapsed = %v, want no full value-vector read after artifact narrowing", timing.valueVectorElapsed)
	}
	if projectionCalls != 0 {
		t.Fatalf("projection calls = %d, want no FK projection when artifact supplies child-parent values", projectionCalls)
	}
	if timing.fkProjectionScope != "reverse_artifact_parent_map" {
		t.Fatalf("fkProjectionScope = %q, want reverse_artifact_parent_map", timing.fkProjectionScope)
	}
	if timing.reverseArtifactProjectMode != "skipped_parent_map" || timing.reverseArtifactProjectElapsed != 0 {
		t.Fatalf("reverse artifact projection intersection = %q/%v, want skipped_parent_map/0", timing.reverseArtifactProjectMode, timing.reverseArtifactProjectElapsed)
	}
	if !timing.childRetainCovered || timing.childRetainMode != "reverse_artifact_parent_map" {
		t.Fatalf("child retain coverage = %t/%q, want reverse_artifact_parent_map", timing.childRetainCovered, timing.childRetainMode)
	}
	if timing.reverseArtifactLocalMode != "parent_value_single_pass" {
		t.Fatalf("reverseArtifactLocalMode = %q, want parent_value_single_pass", timing.reverseArtifactLocalMode)
	}
	if timing.reverseArtifactParentElapsed != 0 || timing.pairElapsed != 0 {
		t.Fatalf("reverse artifact local phases parent/pair = %v/%v, want fused into narrow phase", timing.reverseArtifactParentElapsed, timing.pairElapsed)
	}
	wantJoined := []qsbridge.QuantaRownum{25, 10}
	if len(joined) != len(wantJoined) {
		t.Fatalf("joined = %#v, want %#v", joined, wantJoined)
	}
	for i := range wantJoined {
		if joined[i] != wantJoined[i] {
			t.Fatalf("joined = %#v, want %#v", joined, wantJoined)
		}
	}
	wantPairs := []legacyDirectRelationshipPair{{child: 25, parent: 9}, {child: 10, parent: 7}}
	if len(pairs) != len(wantPairs) {
		t.Fatalf("pairs = %#v, want %#v", pairs, wantPairs)
	}
	for i := range wantPairs {
		if pairs[i] != wantPairs[i] {
			t.Fatalf("pairs = %#v, want %#v", pairs, wantPairs)
		}
	}
}

func TestLegacyDirectRelationshipReduceUsesSortedReverseArtifactCandidates(t *testing.T) {
	childRows := make([]qsbridge.QuantaRownum, 0, 1500)
	for row := 1; row <= 1500; row++ {
		childRows = append(childRows, qsbridge.QuantaRownum(row))
	}
	var artifactRead LegacyDirectRelationshipVectorReadRequest
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ReverseArtifactCandidateReader: fakeLegacyDirectRelationshipVectorReverseArtifactCandidateReader{
			OK:       true,
			LastRead: &artifactRead,
			Result: LegacyDirectRelationshipVectorReverseArtifactCandidateResult{
				Candidates: qsbridge.QuantaCandidateSet{
					Index:   "lineitem",
					Rownums: []qsbridge.QuantaRownum{10, 25},
				},
				ParentValueByChild: map[qsbridge.QuantaRownum]int64{
					10: 7,
					25: 9,
				},
				Mode:         "reverse_artifact_server",
				CacheHit:     true,
				SourceValues: 2,
				TargetRows:   2,
			},
		},
	}
	edge := legacyDirectRelationshipEdge{
		parentRole:   "o",
		parentTable:  "orders",
		parentField:  "o_orderkey",
		childRole:    "l",
		childTable:   "lineitem",
		childField:   "l_orderkey",
		capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityChildExpansion},
	}

	joined, pairs, timing, diagnostics, err := executor.legacyDirectRelationshipReduceWithProjectionRows(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}),
		edge,
		[]qsbridge.QuantaRownum{7, 9},
		childRows,
		childRows,
	)

	if err != nil {
		t.Fatalf("reduce error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want no blockers", diagnostics)
	}
	if !reflect.DeepEqual(artifactRead.TargetCandidateRows, childRows) {
		t.Fatalf("artifact target candidate rows = %d rows, want childRows", len(artifactRead.TargetCandidateRows))
	}
	if timing.reverseArtifactLocalMode != "sorted_candidate_single_pass" {
		t.Fatalf("reverseArtifactLocalMode = %q, want sorted_candidate_single_pass", timing.reverseArtifactLocalMode)
	}
	wantJoined := []qsbridge.QuantaRownum{10, 25}
	if !reflect.DeepEqual(joined, wantJoined) {
		t.Fatalf("joined = %#v, want %#v", joined, wantJoined)
	}
	wantPairs := []legacyDirectRelationshipPair{{child: 10, parent: 7}, {child: 25, parent: 9}}
	if !reflect.DeepEqual(pairs, wantPairs) {
		t.Fatalf("pairs = %#v, want %#v", pairs, wantPairs)
	}
}

func TestLegacyDirectRelationshipReduceCanOmitFullDomainReverseArtifactTargetCandidates(t *testing.T) {
	childRows := []qsbridge.QuantaRownum{1, 2, 3, 4}
	var artifactRead LegacyDirectRelationshipVectorReadRequest
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ReverseArtifactCandidateReader: fakeLegacyDirectRelationshipVectorReverseArtifactCandidateReader{
			OK:       true,
			LastRead: &artifactRead,
			Result: LegacyDirectRelationshipVectorReverseArtifactCandidateResult{
				Candidates: qsbridge.QuantaCandidateSet{
					Index:   "lineitem",
					Rownums: []qsbridge.QuantaRownum{4, 2, 4},
				},
				ParentValueByChild: map[qsbridge.QuantaRownum]int64{
					2: 7,
					4: 9,
				},
				RawParentValueByChild: map[uint64]int64{
					2: 7,
					4: 9,
				},
				Mode:         "reverse_artifact_server",
				CacheHit:     true,
				SourceValues: 2,
				TargetRows:   2,
			},
		},
	}
	edge := legacyDirectRelationshipEdge{
		parentRole:   "o",
		parentTable:  "orders",
		parentField:  "o_orderkey",
		childRole:    "l",
		childTable:   "lineitem",
		childField:   "l_orderkey",
		capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityChildExpansion},
	}

	joined, pairs, timing, diagnostics, err := executor.legacyDirectRelationshipReduceWithProjectionRowsOptions(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}),
		edge,
		[]qsbridge.QuantaRownum{7, 9},
		childRows,
		childRows,
		legacyDirectRelationshipReduceOptions{omitFullDomainTargetCandidates: true},
	)

	if err != nil {
		t.Fatalf("reduce error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want no blockers", diagnostics)
	}
	if len(artifactRead.TargetCandidateRows) != 0 {
		t.Fatalf("artifact target candidate rows = %#v, want omitted", artifactRead.TargetCandidateRows)
	}
	if !artifactRead.PreserveArtifactOrder {
		t.Fatal("preserve artifact order = false, want true for omitted full-domain candidates")
	}
	if artifactRead.MaxEstimatedTargetRows != len(childRows) {
		t.Fatalf("max estimated target rows = %d, want %d", artifactRead.MaxEstimatedTargetRows, len(childRows))
	}
	if timing.reverseArtifactTargetCandidateMode != "omitted_full_domain" {
		t.Fatalf("reverse artifact target candidate mode = %q, want omitted_full_domain", timing.reverseArtifactTargetCandidateMode)
	}
	if timing.reverseArtifactLocalMode != "omitted_target_candidate_rows" {
		t.Fatalf("reverse artifact local mode = %q, want omitted_target_candidate_rows", timing.reverseArtifactLocalMode)
	}
	if !sameRownumSet(joined, []qsbridge.QuantaRownum{4, 2}) {
		t.Fatalf("joined = %#v, want rows 4 and 2", joined)
	}
	wantPairs := []legacyDirectRelationshipPair{{child: 4, parent: 9}, {child: 2, parent: 7}}
	if !sameLegacyDirectRelationshipPairSet(pairs, wantPairs) {
		t.Fatalf("pairs = %#v, want %#v", pairs, wantPairs)
	}
}

func TestLegacyDirectRelationshipRetainUnchangedFullDomainRolesDisablesChangedRoles(t *testing.T) {
	fullDomainRowsByRole := map[string]bool{
		"l": true,
		"o": true,
	}
	before := map[string][]qsbridge.QuantaRownum{
		"l": []qsbridge.QuantaRownum{1, 2, 3},
		"o": []qsbridge.QuantaRownum{10, 20},
	}
	after := map[string][]qsbridge.QuantaRownum{
		"l": []qsbridge.QuantaRownum{1, 3},
		"o": []qsbridge.QuantaRownum{10, 20},
	}

	legacyDirectRelationshipRetainUnchangedFullDomainRoles(fullDomainRowsByRole, before, after)

	if fullDomainRowsByRole["l"] {
		t.Fatal("lineitem role kept full-domain marker after rows changed")
	}
	if !fullDomainRowsByRole["o"] {
		t.Fatal("orders role lost full-domain marker despite unchanged rows")
	}
}

func TestLegacyDirectRelationshipReduceProjectedFKBSIUsesSingleKeyEqualForLargeChildSets(t *testing.T) {
	fkBSI := roaring64.NewDefaultBSI()
	childRows := make([]qsbridge.QuantaRownum, 0, 1200)
	for row := 1200; row >= 1; row-- {
		child := qsbridge.QuantaRownum(row)
		childRows = append(childRows, child)
		if row%10 == 0 {
			fkBSI.SetValue(uint64(child), int64(1007))
			continue
		}
		fkBSI.SetValue(uint64(child), int64(9999))
	}
	parentKeyRows := map[int64]qsbridge.QuantaRownum{
		1007: 7,
	}

	joined, pairs, timing, diagnostics := legacyDirectRelationshipReduceProjectedFKBSIWithTiming(fkBSI, childRows, parentKeyRows)

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want no blockers", diagnostics)
	}
	if !timing.singleKeyEqualUsed {
		t.Fatalf("singleKeyEqualUsed = false, want single-key equality path")
	}
	if timing.batchEqualUsed {
		t.Fatalf("batchEqualUsed = true, want single-key equality path")
	}
	if len(joined) != 120 || len(pairs) != len(joined) {
		t.Fatalf("joined/pairs = %d/%d, want 120 matching rows", len(joined), len(pairs))
	}
	for i := 1; i < len(joined); i++ {
		if joined[i-1] < joined[i] {
			t.Fatalf("joined row order = %#v, want original child row order preserved", joined[:10])
		}
	}
	for _, pair := range pairs {
		if pair.child%10 != 0 || pair.parent != 7 {
			t.Fatalf("unexpected pair = %#v, want child divisible by 10 and parent 7", pair)
		}
	}
}

func TestLegacyDirectRelationshipCountFastPathUsesVectorExistence(t *testing.T) {
	calls := 0
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"lineitem"}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "order_line_count", Type: qsbridge.DataTypeInt}}
	request.Predicates = []qsbridge.Predicate{{
		Expr: qsbridge.Binary(
			qsbridge.BinaryOpEqual,
			qsbridge.Field(qsbridge.FieldRef{Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"}, Name: "l_orderkey", Type: qsbridge.DataTypeInt}),
			qsbridge.Field(qsbridge.FieldRef{Table: qsbridge.TableInstance{Table: "orders", Alias: "o"}, Name: "o_orderkey", Type: qsbridge.DataTypeInt}),
		),
		Placement: qsbridge.PredicatePushdown,
		Scope:     qsbridge.PredicateScopeOn,
	}}
	cache := core.NewTableCacheStruct()
	cache.TableCache["lineitem"] = &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem"},
		AttributeNameMap: map[string]*core.Attribute{
			"l_orderkey": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_orderkey", SourceName: "l_orderkey", Type: "Integer", MappingStrategy: "ParentRelation", ForeignKey: "orders.o_orderkey"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
		TableCache:      cache,
		RelationshipProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				101: 1,
				102: 1,
				103: 2,
			}),
			Calls: &calls,
		},
	}
	vector := RelationshipVectorJoinRequest{RootIndex: "lineitem", Edges: []qsbridge.RelationshipJoinPlanEdge{{
		Left:          qsbridge.FieldRef{Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"}, Name: "l_orderkey", Type: qsbridge.DataTypeInt},
		LeftRole:      "l",
		Right:         qsbridge.FieldRef{Table: qsbridge.TableInstance{Table: "orders", Alias: "o"}, Name: "o_orderkey", Type: qsbridge.DataTypeInt},
		RightRole:     "o",
		SQLKind:       qsbridge.JoinKindInner,
		ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
	}}}

	result, err := executor.ExecuteRelationshipVectorJoin(context.Background(), request, vector)
	if err != nil {
		t.Fatalf("relationship join: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if calls != 1 {
		t.Fatalf("projection calls = %d, want 1", calls)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "count_fast_path", "relationship_vector_existence")
	assertExecutionProbe(t, result.Probes, "relationship_join", "count_fast_path_rows", "3")
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 || chunk.Rows[0][0].Value != int64(3) {
		t.Fatalf("rows = %#v, want count 3", chunk.Rows)
	}
}

func TestLegacyDirectRelationshipParentKeyRowsUsesParentRownumDomain(t *testing.T) {
	executor := LegacyDirectRelationshipVectorJoinExecutor{}

	parentKeyRows, materialized, diagnostics, err := executor.legacyDirectRelationshipParentKeyRows(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}),
		legacyDirectRelationshipEdge{parentTable: "customers_qa", parentField: "cust_id"},
		[]qsbridge.QuantaRownum{1, 4},
	)

	if err != nil {
		t.Fatalf("parent key rows error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want no blockers", diagnostics)
	}
	if materialized {
		t.Fatalf("materialized = true, want false")
	}
	if parentKeyRows[1] != 1 || parentKeyRows[4] != 4 {
		t.Fatalf("parent key rows = %#v, want rownum-domain mapping", parentKeyRows)
	}
	if _, ok := parentKeyRows[10]; ok {
		t.Fatalf("parent key rows = %#v, should not use business key 10", parentKeyRows)
	}
}

func TestLegacyDirectRelationshipReduceTimingIncludesParentKeyAndMatchMetrics(t *testing.T) {
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
		RelationshipProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				101: 1,
				102: 1,
				103: 2,
				104: 4,
			}),
		},
	}
	edge := legacyDirectRelationshipEdge{
		parentTable: "orders",
		parentField: "o_orderkey",
		childTable:  "lineitem",
		childField:  "l_orderkey",
		sqlKind:     qsbridge.JoinKindInner,
	}

	joined, pairs, timing, diagnostics, err := executor.legacyDirectRelationshipReduceWithTiming(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}),
		edge,
		[]qsbridge.QuantaRownum{1, 2},
		[]qsbridge.QuantaRownum{101, 102, 103, 104},
	)

	if err != nil {
		t.Fatalf("reduce error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if got := len(joined); got != 3 {
		t.Fatalf("joined rows = %d, want 3", got)
	}
	if got := len(pairs); got != 3 {
		t.Fatalf("pairs = %d, want 3", got)
	}
	if timing.parentKeyRows != 2 {
		t.Fatalf("parent key rows = %d, want 2", timing.parentKeyRows)
	}
	if timing.parentKeyMaterialization {
		t.Fatalf("parent key materialization = true, want false for rownum-domain parent keys")
	}
	if timing.matchedRows != 3 {
		t.Fatalf("matched rows = %d, want 3", timing.matchedRows)
	}
	if timing.projectionRows != 4 || timing.fkProjectionRows != 4 {
		t.Fatalf("projection rows = %d fk rows = %d, want 4/4", timing.projectionRows, timing.fkProjectionRows)
	}
}

func TestLegacyDirectRelationshipSingleEdgePrefiltersParentResidualBeforeReduction(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	partName := qsbridge.FieldRef{Table: part, Name: "p_name", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "part", Role: "p", Field: "p_partkey", Type: qsbridge.DataTypeInt, Visible: true},
			{Index: "part", Role: "p", Field: "p_name", Type: qsbridge.DataTypeString, Visible: true},
			{Index: "lineitem", Role: "l", Field: "l_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
			{Index: "lineitem", Role: "l", Field: "l_suppkey", Type: qsbridge.DataTypeInt, Visible: true},
		},
	})
	request.Sources = []qsbridge.TableInstance{part, lineitem}
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpLike, qsbridge.Field(partName), qsbridge.Literal(qsbridge.ValueString, "%green%")),
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}
	cache := core.NewTableCacheStruct()
	cache.TableCache["part"] = &core.Table{
		BasicTable: &shared.BasicTable{Name: "part", PrimaryKey: "p_partkey"},
		AttributeNameMap: map[string]*core.Attribute{
			"p_partkey": {BasicAttribute: &shared.BasicAttribute{FieldName: "p_partkey", SourceName: "p_partkey", Type: "Integer", MappingStrategy: "IntBSI"}},
			"p_name":    {BasicAttribute: &shared.BasicAttribute{FieldName: "p_name", SourceName: "p_name", Type: "String", MappingStrategy: "StringLexBSI"}},
		},
	}
	cache.TableCache["lineitem"] = &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem", PrimaryKey: "l_orderkey"},
		AttributeNameMap: map[string]*core.Attribute{
			"l_orderkey": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_orderkey", SourceName: "l_orderkey", Type: "Integer", MappingStrategy: "IntBSI"}},
			"l_suppkey":  {BasicAttribute: &shared.BasicAttribute{FieldName: "l_suppkey", SourceName: "l_suppkey", Type: "Integer", MappingStrategy: "IntBSI"}},
			"l_partkey":  {BasicAttribute: &shared.BasicAttribute{FieldName: "l_partkey", SourceName: "l_partkey", Type: "Integer", MappingStrategy: "ParentRelation", ForeignKey: "part.p_partkey"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: cache,
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			index, _ := request.RootIndex()
			return DirectSessionHandleFunc{QueryFunc: func(context.Context, ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
				switch index {
				case "part":
					return BitmapQueryResult{Success: true, Count: 3, Rownums: []qsbridge.QuantaRownum{1, 2, 3}}, nil, nil
				case "lineitem":
					return BitmapQueryResult{Success: true, Count: 4, Rownums: []qsbridge.QuantaRownum{101, 102, 103, 104}}, nil, nil
				default:
					t.Fatalf("unexpected bitmap query index %q", index)
					return BitmapQueryResult{}, nil, nil
				}
			}}, nil, nil
		}),
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
		RelationshipProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				101: 1,
				102: 2,
				103: 3,
				104: 3,
			}),
		},
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				vector := qsbridge.QuantaProjectionVector{Field: field, Values: make([]qsbridge.ResultCell, 0, len(request.Rownums))}
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "p_partkey":
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum)})
					case "p_name":
						name := "green part"
						if rownum == 2 {
							name = "red part"
						}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: name})
					case "l_orderkey":
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum)})
					case "l_suppkey":
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum + 1000)})
					default:
						t.Fatalf("unexpected materialization field %q", field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}
	vector := RelationshipVectorJoinRequest{RootIndex: "lineitem", Edges: []qsbridge.RelationshipJoinPlanEdge{{
		Left:          qsbridge.FieldRef{Table: lineitem, Name: "l_partkey", Type: qsbridge.DataTypeInt},
		LeftRole:      "l",
		Right:         qsbridge.FieldRef{Table: part, Name: "p_partkey", Type: qsbridge.DataTypeInt},
		RightRole:     "p",
		SQLKind:       qsbridge.JoinKindInner,
		ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
	}}}

	result, err := executor.ExecuteRelationshipVectorJoin(context.Background(), request, vector)
	if err != nil {
		t.Fatalf("relationship join: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "residual_prefilter_1_rows_before", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "residual_prefilter_1_rows_after", "2")
	assertExecutionProbe(t, result.Probes, "relationship_join", "parent_rows", "2")
	assertExecutionProbe(t, result.Probes, "relationship_join", "joined_rows", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "child_materialization_rows", "3")
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 3 {
		t.Fatalf("rows = %#v, want 3 joined rows after parent residual prefilter", chunk.Rows)
	}
}

func TestLegacyDirectRelationshipReduceUsesQueryScratchpadDomainMapping(t *testing.T) {
	calls := 0
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
		RelationshipProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				101: 1,
				102: 1,
				103: 2,
				104: 4,
			}),
			Calls: &calls,
		},
	}
	edge := legacyDirectRelationshipEdge{
		parentRole:  "o",
		parentTable: "orders",
		parentField: "o_orderkey",
		childRole:   "l",
		childTable:  "lineitem",
		childField:  "l_orderkey",
		sqlKind:     qsbridge.JoinKindInner,
	}
	ctx := WithQueryScratchpad(context.Background())
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	parentRows := []qsbridge.QuantaRownum{1, 2}
	childRows := []qsbridge.QuantaRownum{101, 102, 103, 104}

	joined, pairs, firstTiming, diagnostics, err := executor.legacyDirectRelationshipReduceWithTiming(ctx, request, edge, parentRows, childRows)
	if err != nil {
		t.Fatalf("first reduce error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("first diagnostics = %#v, want none", diagnostics)
	}
	if firstTiming.domainMappingCacheHit {
		t.Fatal("first reduce should miss the domain mapping cache")
	}
	if calls != 1 {
		t.Fatalf("projection calls after first reduce = %d, want 1", calls)
	}
	if len(joined) != 3 || len(pairs) != 3 {
		t.Fatalf("first joined/pairs = %d/%d, want 3/3", len(joined), len(pairs))
	}

	joined, pairs, secondTiming, diagnostics, err := executor.legacyDirectRelationshipReduceWithTiming(ctx, request, edge, parentRows, childRows)
	if err != nil {
		t.Fatalf("second reduce error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("second diagnostics = %#v, want none", diagnostics)
	}
	if !secondTiming.domainMappingCacheHit || secondTiming.domainMappingCacheMode != "exact" {
		t.Fatalf("second domain cache = %t/%q, want exact hit", secondTiming.domainMappingCacheHit, secondTiming.domainMappingCacheMode)
	}
	if calls != 1 {
		t.Fatalf("projection calls after second reduce = %d, want still 1", calls)
	}
	if len(joined) != 3 || len(pairs) != 3 {
		t.Fatalf("second joined/pairs = %d/%d, want 3/3", len(joined), len(pairs))
	}
}

func TestLegacyDirectRelationshipParentMapUsesDomainMappingChildSubset(t *testing.T) {
	calls := 0
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		RelationshipProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				101: 1,
				102: 1,
				103: 2,
				104: 4,
			}),
			Calls: &calls,
		},
	}
	edge := legacyDirectRelationshipEdge{
		parentRole:  "o",
		parentTable: "orders",
		parentField: "o_orderkey",
		childRole:   "l",
		childTable:  "lineitem",
		childField:  "l_orderkey",
		sqlKind:     qsbridge.JoinKindInner,
	}
	scratchpad := NewQueryScratchpad()
	scratchpad.RelationshipVectorProjections = nil
	ctx := context.WithValue(context.Background(), queryScratchpadContextKey{}, scratchpad)
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	parentRows := []qsbridge.QuantaRownum{1, 2}
	childRows := []qsbridge.QuantaRownum{101, 102, 103, 104}

	joined, _, _, diagnostics, err := executor.legacyDirectRelationshipReduceWithTiming(ctx, request, edge, parentRows, childRows)
	if err != nil {
		t.Fatalf("reduce error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("reduce diagnostics = %#v, want none", diagnostics)
	}
	if calls != 1 {
		t.Fatalf("projection calls after reduce = %d, want 1", calls)
	}
	if len(joined) != 3 {
		t.Fatalf("joined rows = %d, want 3", len(joined))
	}

	parentByChild, diagnostics, err := executor.legacyDirectRelationshipParentMapWithProjectionRows(
		ctx,
		request,
		edge,
		[]qsbridge.QuantaRownum{101, 103},
		childRows,
	)
	if err != nil {
		t.Fatalf("parent map error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("parent map diagnostics = %#v, want none", diagnostics)
	}
	if calls != 1 {
		t.Fatalf("projection calls after parent map = %d, want still 1", calls)
	}
	if len(parentByChild) != 2 || parentByChild[101] != 1 || parentByChild[103] != 2 {
		t.Fatalf("parent map = %#v, want child-subset domain mapping", parentByChild)
	}
}

func TestLegacyDirectRelationshipVectorProjectionWindowUsesChildTimePredicate(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem", TimeQuantumField: "l_shipdate"},
		AttributeNameMap: map[string]*core.Attribute{
			"l_shipdate": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "time"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"lineitem": table}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index: "lineitem",
		Field: "l_shipdate",
		BSIOp: qsbridge.QuantaBSIOpRange,
		Begin: big.NewInt(820454400000),
		End:   big.NewInt(828316800000),
	}}})

	fromTime, toTime := executor.legacyDirectRelationshipVectorProjectionWindow(request, "lineitem")

	if fromTime != 820454400000000000 {
		t.Fatalf("from nanos = %d, want 820454400000000000", fromTime)
	}
	if toTime != 828316800000000000 {
		t.Fatalf("to nanos = %d, want 828316800000000000", toTime)
	}
}

func TestLegacyDirectRelationshipVectorProjectionWindowUsesAliasedChildTimePredicate(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem", TimeQuantumField: "l_shipdate"},
		AttributeNameMap: map[string]*core.Attribute{
			"l_shipdate": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "time"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"lineitem": table}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index: "l",
		Field: "l.l_shipdate",
		BSIOp: qsbridge.QuantaBSIOpRange,
		Begin: big.NewInt(820454400000),
		End:   big.NewInt(828316800000),
	}}})
	request.Sources = []qsbridge.TableInstance{{Table: "lineitem", Alias: "l"}}

	fromTime, toTime := executor.legacyDirectRelationshipVectorProjectionWindow(request, "lineitem")

	if fromTime != 820454400000000000 {
		t.Fatalf("from nanos = %d, want 820454400000000000", fromTime)
	}
	if toTime != 828316800000000000 {
		t.Fatalf("to nanos = %d, want 828316800000000000", toTime)
	}
}

func TestLegacyDirectRelationshipVectorProjectionWindowIgnoresNonShardTimePredicate(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem", TimeQuantumField: "l_shipdate"},
		AttributeNameMap: map[string]*core.Attribute{
			"l_receiptdate": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_receiptdate", Type: "time"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"lineitem": table}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index: "lineitem",
		Field: "l_receiptdate",
		BSIOp: qsbridge.QuantaBSIOpRange,
		Begin: big.NewInt(820454400000),
		End:   big.NewInt(828316800000),
	}}})

	fromTime, toTime := executor.legacyDirectRelationshipVectorProjectionWindow(request, "lineitem")

	if fromTime != legacyDirectRelationshipFullTimeRangeBeginMillis*int64(1000000) {
		t.Fatalf("from nanos = %d, want synthetic full range", fromTime)
	}
	if toTime != legacyDirectRelationshipFullTimeRangeEndMillis*int64(1000000) {
		t.Fatalf("to nanos = %d, want synthetic full range", toTime)
	}
}

func TestLegacyDirectRelationshipVectorProjectionWindowUsesSyntheticTimeRange(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "orders",
			TimeQuantumField: "o_orderdate",
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"orders": table}},
	}

	fromTime, toTime := executor.legacyDirectRelationshipVectorProjectionWindow(NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}), "orders")

	if fromTime != legacyDirectRelationshipFullTimeRangeBeginMillis*int64(1000000) {
		t.Fatalf("from nanos = %d, want synthetic full range", fromTime)
	}
	if toTime != legacyDirectRelationshipFullTimeRangeEndMillis*int64(1000000) {
		t.Fatalf("to nanos = %d, want synthetic full range", toTime)
	}
}

func TestLegacyDirectRelationshipVectorProjectionWindowForEdgeBroadensWhenFoundsetPresent(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "lineitem",
			TimeQuantumField: "l_shipdate",
		},
		AttributeNameMap: map[string]*core.Attribute{
			"l_shipdate": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "time"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"lineitem": table}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index: "lineitem",
		Field: "l_shipdate",
		BSIOp: qsbridge.QuantaBSIOpRange,
		Begin: big.NewInt(820454400000),
		End:   big.NewInt(828316800000),
	}}})
	edge := legacyDirectRelationshipEdge{
		childTable:      "lineitem",
		childField:      "l_orderkey",
		projectionScope: qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset,
	}

	fromTime, toTime := executor.legacyDirectRelationshipVectorProjectionWindowForEdge(request, edge, []qsbridge.QuantaRownum{1})

	if fromTime != legacyDirectRelationshipFullTimeRangeBeginMillis*int64(1000000) {
		t.Fatalf("from nanos = %d, want synthetic full range", fromTime)
	}
	if toTime != legacyDirectRelationshipFullTimeRangeEndMillis*int64(1000000) {
		t.Fatalf("to nanos = %d, want synthetic full range", toTime)
	}
}

func TestLegacyDirectRelationshipVectorProjectionWindowConvertsMicrosecondFragments(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "orders_qa", TimeQuantumField: "order_date"},
		AttributeNameMap: map[string]*core.Attribute{
			"order_date": {BasicAttribute: &shared.BasicAttribute{FieldName: "order_date", Type: "DateTime", MappingStrategy: "SysMicroBSI"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"orders_qa": table}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index: "orders_qa",
		Field: "order_date",
		BSIOp: qsbridge.QuantaBSIOpRange,
		Begin: big.NewInt(1685577600000000),
		End:   big.NewInt(1685664000000000),
	}}})

	fromTime, toTime := executor.legacyDirectRelationshipVectorProjectionWindow(request, "orders_qa")

	if fromTime != 1685577600000000000 {
		t.Fatalf("from nanos = %d, want microsecond fragment converted to nanos", fromTime)
	}
	if toTime != 1685664000000000000 {
		t.Fatalf("to nanos = %d, want microsecond fragment converted to nanos", toTime)
	}
}
func TestLegacyDirectRelationshipVectorProjectionWindowForEdgeKeepsPredicateWindowWithoutFoundset(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "lineitem",
			TimeQuantumField: "l_shipdate",
		},
		AttributeNameMap: map[string]*core.Attribute{
			"l_shipdate": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "time"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"lineitem": table}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index: "lineitem",
		Field: "l_shipdate",
		BSIOp: qsbridge.QuantaBSIOpRange,
		Begin: big.NewInt(820454400000),
		End:   big.NewInt(828316800000),
	}}})
	edge := legacyDirectRelationshipEdge{
		childRole:       "lineitem",
		childTable:      "lineitem",
		childField:      "l_orderkey",
		projectionScope: qsbridge.RelationshipVectorProjectionScopeBroadFromFoundset,
	}

	fromTime, toTime := executor.legacyDirectRelationshipVectorProjectionWindowForEdge(request, edge, nil)

	if fromTime != 820454400000000000 {
		t.Fatalf("from nanos = %d, want 820454400000000000", fromTime)
	}
	if toTime != 828316800000000000 {
		t.Fatalf("to nanos = %d, want 828316800000000000", toTime)
	}
}

func TestLegacyDirectRelationshipVectorProjectionWindowForEdgeKeepsPredicateScope(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "lineitem",
			TimeQuantumField: "l_shipdate",
		},
		AttributeNameMap: map[string]*core.Attribute{
			"l_shipdate": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "time"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"lineitem": table}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index: "lineitem",
		Field: "l_shipdate",
		BSIOp: qsbridge.QuantaBSIOpRange,
		Begin: big.NewInt(820454400000),
		End:   big.NewInt(828316800000),
	}}})
	edge := legacyDirectRelationshipEdge{
		childRole:       "lineitem",
		childTable:      "lineitem",
		childField:      "l_orderkey",
		projectionScope: qsbridge.RelationshipVectorProjectionScopePredicateWindow,
	}

	fromTime, toTime := executor.legacyDirectRelationshipVectorProjectionWindowForEdge(request, edge, []qsbridge.QuantaRownum{1})

	if fromTime != 820454400000000000 {
		t.Fatalf("from nanos = %d, want 820454400000000000", fromTime)
	}
	if toTime != 828316800000000000 {
		t.Fatalf("to nanos = %d, want 828316800000000000", toTime)
	}
}

func TestLegacyDirectRelationshipVectorProjectionWindowForEdgeBroadensFoundsetWhenSiblingTableHasShardTimePredicate(t *testing.T) {
	lineitem := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "lineitem",
			TimeQuantumField: "l_shipdate",
		},
		AttributeNameMap: map[string]*core.Attribute{
			"l_shipdate": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "time"}},
		},
	}
	orders := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "orders",
			TimeQuantumField: "o_orderdate",
		},
		AttributeNameMap: map[string]*core.Attribute{
			"o_orderdate": {BasicAttribute: &shared.BasicAttribute{FieldName: "o_orderdate", Type: "time"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{
			"lineitem": lineitem,
			"orders":   orders,
		}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index: "o",
		Field: "o.o_orderdate",
		BSIOp: qsbridge.QuantaBSIOpRange,
		Begin: big.NewInt(757382400000),
		End:   big.NewInt(788918400000),
	}}})
	request.Sources = []qsbridge.TableInstance{
		{Table: "orders", Alias: "o"},
		{Table: "lineitem", Alias: "l"},
	}
	edge := legacyDirectRelationshipEdge{
		childRole:       "l",
		childTable:      "lineitem",
		childField:      "l_orderkey",
		projectionScope: qsbridge.RelationshipVectorProjectionScopePredicateWindow,
	}

	fromTime, toTime := executor.legacyDirectRelationshipVectorProjectionWindowForEdge(request, edge, []qsbridge.QuantaRownum{788918400000000005})

	if fromTime != legacyDirectRelationshipFullTimeRangeBeginMillis*int64(1000000) {
		t.Fatalf("from nanos = %d, want synthetic full range", fromTime)
	}
	if toTime != legacyDirectRelationshipFullTimeRangeEndMillis*int64(1000000) {
		t.Fatalf("to nanos = %d, want synthetic full range", toTime)
	}
}

func TestLegacyDirectRelationshipTimeMaterializationUsesChildTimePredicate(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem", TimeQuantumField: "l_shipdate"},
		AttributeNameMap: map[string]*core.Attribute{
			"l_shipdate": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "time"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"lineitem": table}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index: "l",
		Field: "l.l_shipdate",
		BSIOp: qsbridge.QuantaBSIOpRange,
		Begin: big.NewInt(820454400000),
		End:   big.NewInt(828316800000),
	}}})
	request.Sources = []qsbridge.TableInstance{{Table: "lineitem", Alias: "l"}}

	materialization := executor.legacyDirectRelationshipTimeMaterialization(request, "lineitem")

	if materialization.Index != "lineitem" {
		t.Fatalf("materialization index = %q, want lineitem", materialization.Index)
	}
	if materialization.FromEpochMillis != 820454400000 || materialization.ToEpochMillis != 828316800000 {
		t.Fatalf("materialization window = %d..%d, want 820454400000..828316800000", materialization.FromEpochMillis, materialization.ToEpochMillis)
	}
	if len(materialization.ProjectionFields) != 1 {
		t.Fatalf("projection fields = %#v, want one time field", materialization.ProjectionFields)
	}
	field := materialization.ProjectionFields[0]
	if field.Index != "l" || field.Field != "l.l_shipdate" || field.PhysicalName != "l_shipdate" || field.Type != qsbridge.DataTypeTime {
		t.Fatalf("projection field = %#v, want aliased l_shipdate time field", field)
	}
}

func TestLegacyDirectRelationshipTimeMaterializationUsesSyntheticTimeRange(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "orders",
			TimeQuantumField: "o_orderdate",
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"orders": table}},
	}

	materialization := executor.legacyDirectRelationshipTimeMaterialization(NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}), "orders")

	if materialization.Index != "orders" {
		t.Fatalf("materialization index = %q, want orders", materialization.Index)
	}
	if materialization.FromEpochMillis != legacyDirectRelationshipFullTimeRangeBeginMillis {
		t.Fatalf("from millis = %d, want synthetic full range", materialization.FromEpochMillis)
	}
	if materialization.ToEpochMillis != legacyDirectRelationshipFullTimeRangeEndMillis {
		t.Fatalf("to millis = %d, want synthetic full range", materialization.ToEpochMillis)
	}
}

func TestLegacyDirectRelationshipAllRownumRequestPrefersCatalogIdentityOverEndpointField(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "customers_qa",
			PrimaryKey:       "cust_id",
			TimeQuantumField: "createdAtTimestamp",
		},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "cust_id", Type: "string", MappingStrategy: "StringLexBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "createdAtTimestamp", Type: "DateTime", MappingStrategy: "SysMicroBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "rownum", Type: "int", MappingStrategy: "IntDirect"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"customers_qa": table}},
	}

	request := executor.legacyDirectRelationshipAllRownumRequest("customers_qa", "phoneType")

	assertAllRownumSeed(t, request, "customers_qa", "cust_id", false)
}

func TestLegacyDirectRelationshipAllRownumRequestUsesTimeQuantumField(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "orders",
			TimeQuantumType:  "YMD",
			TimeQuantumField: "o_orderdate",
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"orders": table}},
	}

	request := executor.legacyDirectRelationshipAllRownumRequest("orders", "")

	seed := assertAllRownumSeed(t, request, "orders", "o_orderdate", true)
	begin, end := legacyDirectRelationshipFullTimeRangeEncoded(table, "o_orderdate")
	if seed.Begin == nil || seed.End == nil || seed.Begin.Int64() != begin || seed.End.Int64() != end {
		t.Fatalf("seed range = %#v..%#v, want %d..%d", seed.Begin, seed.End, begin, end)
	}
	if len(request.Query.ProjectionFields) != 1 || request.Query.ProjectionFields[0].Field != "o_orderdate" || request.Query.ProjectionFields[0].Type != qsbridge.DataTypeTime {
		t.Fatalf("projection fields = %#v, want time-window metadata", request.Query.ProjectionFields)
	}
}

func TestLegacyDirectRelationshipAllRownumRequestUsesPhysicalTimeShardSeedOverPrimaryKeyFallback(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "orders",
			PrimaryKey:       "o_orderkey",
			TimeQuantumType:  "YMD",
			TimeQuantumField: "o_orderdate",
		},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "o_orderkey", MappingStrategy: "IntBSI"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"orders": table}},
	}

	request := executor.legacyDirectRelationshipAllRownumRequest("orders", "o_orderkey")

	assertAllRownumSeed(t, request, "orders", "o_orderdate", true)
}

func TestLegacyDirectRelationshipAllRownumRequestUsesPhysicalTimeShardSeedOverParentRelationFallback(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "orders",
			TimeQuantumType:  "YMD",
			TimeQuantumField: "o_orderdate",
		},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "o_custkey", MappingStrategy: "ParentRelation"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"orders": table}},
	}

	request := executor.legacyDirectRelationshipAllRownumRequest("orders", "o_custkey")

	assertAllRownumSeed(t, request, "orders", "o_orderdate", true)
}

func TestLegacyDirectRelationshipAllRownumRequestUsesEndpointFallbackWhenCatalogHasNoIdentitySeed(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "orders_qa"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "order_date", Type: "DateTime", MappingStrategy: "SysMicroBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "cust_id", MappingStrategy: "ParentRelation"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"orders_qa": table}},
	}

	request := executor.legacyDirectRelationshipAllRownumRequest("orders_qa", "cust_id")

	assertAllRownumSeed(t, request, "orders_qa", "cust_id", false)
}

func TestLegacyDirectRelationshipAllRownumRequestDerivesNonShardTimeSeedField(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name: "lineitem",
		},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_commitdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"lineitem": table}},
	}

	request := executor.legacyDirectRelationshipAllRownumRequest("lineitem", "")

	seed := assertAllRownumSeed(t, request, "lineitem", "l_shipdate", false)
	if seed.Begin != nil || seed.End != nil {
		t.Fatalf("seed = %#v, did not expect non-physical derived time seed bounds", seed)
	}
}

func TestLegacyDirectRelationshipAllRownumRequestUsesTimeFieldGranularity(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{Name: "orders_qa"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "order_date", Type: "DateTime", MappingStrategy: "SysMicroBSI"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"orders_qa": table}},
	}

	request := executor.legacyDirectRelationshipAllRownumRequest("orders_qa", "")

	seed := assertAllRownumSeed(t, request, "orders_qa", "order_date", false)
	if seed.Begin != nil || seed.End != nil {
		t.Fatalf("seed = %#v, did not expect non-physical time seed bounds", seed)
	}
}

func TestLegacyDirectRelationshipAllRownumRequestFallsBackToExistence(t *testing.T) {
	executor := LegacyDirectRelationshipVectorJoinExecutor{}

	request := executor.legacyDirectRelationshipAllRownumRequest("nation", "n_regionkey")

	assertAllRownumSeed(t, request, "nation", "n_regionkey", false)
}

func assertAllRownumSeed(t *testing.T, request ExecutionRequest, index string, field string, shardWindow bool) qsbridge.QuantaSeed {
	t.Helper()
	if len(request.Query.Fragments) != 0 {
		t.Fatalf("fragments = %#v, want all-rownum table seed instead", request.Query.Fragments)
	}
	if len(request.Query.Seeds) != 1 {
		t.Fatalf("seeds = %#v, want one table existence seed", request.Query.Seeds)
	}
	seed := request.Query.Seeds[0]
	if seed.Index != index || seed.Field != field || seed.Kind != qsbridge.QuantaSeedTableExistence {
		t.Fatalf("seed = %#v, want %s.%s table existence seed", seed, index, field)
	}
	if seed.ShardWindow != shardWindow {
		t.Fatalf("seed shard window = %t, want %t for %#v", seed.ShardWindow, shardWindow, seed)
	}
	return seed
}

func TestLegacyDirectRelationshipInitialRownumsForRoleUsesRelationshipVectorExistence(t *testing.T) {
	edge := legacyDirectRelationshipEdge{
		parentRole:  "o",
		parentTable: "orders",
		parentField: "o_orderkey",
		childRole:   "l",
		childTable:  "lineitem",
		childField:  "l_orderkey",
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
	}
	fromTime, toTime := executor.legacyDirectRelationshipBroadVectorProjectionWindow(edge.childTable)
	cacheKey := executor.legacyDirectRelationshipProjectionCacheKey(edge.childTable, edge.childField, fromTime, toTime, nil)
	executor.ProjectionCache.Put(cacheKey, testRelationshipVectorBSI(map[uint64]int64{
		11: 1001,
		12: 1002,
	}))

	rows, seedKind, seedProbes, diagnostics, err := executor.legacyDirectRelationshipInitialRownumsForRole(
		context.Background(),
		ExecutionRequest{},
		legacyDirectRelationshipRoleFallback{table: "lineitem", role: "l", field: "l_orderkey"},
		[]legacyDirectRelationshipEdge{edge},
	)
	if err != nil {
		t.Fatalf("initial rownums: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if seedKind != "relationship_vector_existence" {
		t.Fatalf("seed kind = %q, want relationship_vector_existence", seedKind)
	}
	if len(rows) != 2 || rows[0] != 11 || rows[1] != 12 {
		t.Fatalf("rows = %#v, want vector existence rownums [11 12]", rows)
	}
	assertExecutionProbe(t, seedProbes, "relationship_join", "relationship_vector_projection_cache_hit", "true")
}

func TestLegacyDirectRelationshipCachedFullFKBSIReusesCoveredFoundSet(t *testing.T) {
	edge := legacyDirectRelationshipEdge{
		childTable: "lineitem",
		childField: "l_orderkey",
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
	}
	fromTime, toTime := int64(10), int64(20)
	full := testRelationshipVectorBSI(map[uint64]int64{
		11: 1001,
		12: 1002,
	})
	cacheKey := executor.legacyDirectRelationshipProjectionCacheKey(edge.childTable, edge.childField, fromTime, toTime, nil)
	executor.ProjectionCache.Put(cacheKey, full)

	got, ok := executor.legacyDirectRelationshipCachedFullFKBSI(context.Background(), edge, fromTime, toTime, roaring64.BitmapOf(11, 12))
	if !ok {
		t.Fatalf("full FK BSI cache hit = false, want true for equal foundset")
	}
	if got != full {
		t.Fatalf("cached BSI pointer changed")
	}
	got, ok = executor.legacyDirectRelationshipCachedFullFKBSI(context.Background(), edge, fromTime, toTime, roaring64.BitmapOf(11))
	if !ok {
		t.Fatalf("full FK BSI cache hit = false, want true for covered narrowed foundset")
	}
	if got != full {
		t.Fatalf("covered foundset cached BSI pointer changed")
	}
	if _, ok := executor.legacyDirectRelationshipCachedFullFKBSI(context.Background(), edge, fromTime, toTime, roaring64.BitmapOf(11, 13)); ok {
		t.Fatalf("full FK BSI cache hit = true, want false for uncovered foundset")
	}
}

func TestLegacyDirectRelationshipParentMapRetriesFullFKBSIWhenProjectionMissesChild(t *testing.T) {
	edge := legacyDirectRelationshipEdge{
		parentRole:  "n1",
		parentTable: "nation",
		parentField: "n_nationkey",
		childRole:   "s",
		childTable:  "supplier",
		childField:  "s_nationkey",
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	fromTime, toTime := executor.legacyDirectRelationshipVectorProjectionWindowForEdge(request, edge, []qsbridge.QuantaRownum{70, 71})
	narrowKey := executor.legacyDirectRelationshipProjectionCacheKey(edge.childTable, edge.childField, fromTime, toTime, legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{70, 71}))
	executor.ProjectionCache.Put(narrowKey, testRelationshipVectorBSI(map[uint64]int64{
		71: 5,
	}))
	fullFrom, fullTo := executor.legacyDirectRelationshipBroadVectorProjectionWindow(edge.childTable)
	fullKey := executor.legacyDirectRelationshipProjectionCacheKey(edge.childTable, edge.childField, fullFrom, fullTo, nil)
	executor.ProjectionCache.Put(fullKey, testRelationshipVectorBSI(map[uint64]int64{
		70: 4,
		71: 5,
	}))

	parentByChild, diagnostics, err := executor.legacyDirectRelationshipParentMapWithProjectionRows(
		context.Background(),
		request,
		edge,
		[]qsbridge.QuantaRownum{70, 71},
		[]qsbridge.QuantaRownum{70, 71},
	)

	if err != nil {
		t.Fatalf("parent map: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if parentByChild[70] != 4 || parentByChild[71] != 5 {
		t.Fatalf("parent map = %#v, want recovered full FK values for children 70 and 71", parentByChild)
	}
}

func TestLegacyDirectRelationshipAggregateResultCountsDistinctJoinedRows(t *testing.T) {
	orders := qsbridge.TableInstance{Table: "orders_qa", Alias: "o"}
	shipVia := qsbridge.FieldRef{Table: orders, Name: "ship_via", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index: "orders_qa",
			Field: "ship_via",
			Type:  qsbridge.DataTypeString,
		}},
	})
	request.SQLAggregates = []qsbridge.Aggregate{
		{Function: "count", Mode: qsbridge.AggregateDistinct, Input: qsbridge.Field(shipVia), Alias: "carrier_count", Type: qsbridge.DataTypeInt},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field: request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueString, Value: "UPS"},
						{Kind: qsbridge.ValueString, Value: "UPS"},
						{Kind: qsbridge.ValueString, Value: "FEDEX"},
					},
				}},
			}, nil, nil
		}),
	}
	result, err := executor.legacyDirectRelationshipAggregateResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "orders_qa", parentTable: "customers_qa"},
		[]qsbridge.QuantaRownum{11, 12, 21},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 1}, {child: 21, parent: 2}},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("aggregate result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 {
		t.Fatalf("rows = %#v, want one count distinct value", chunk.Rows)
	}
	if got := chunk.Rows[0][0].Value; got != int64(2) {
		t.Fatalf("count distinct = %#v, want 2", got)
	}
}

func TestLegacyDirectRelationshipAggregateResultGroupsJoinedRows(t *testing.T) {
	customers := qsbridge.TableInstance{Table: "customers_qa", Alias: "c"}
	firstName := qsbridge.FieldRef{Table: customers, Name: "first_name", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index:   "customers_qa",
			Field:   "first_name",
			Type:    qsbridge.DataTypeString,
			Visible: true,
		}},
	})
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(firstName)}
	request.SQLAggregates = []qsbridge.Aggregate{
		{Function: "count", Alias: "order_count", Type: qsbridge.DataTypeInt},
	}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(firstName), Type: qsbridge.DataTypeString},
		{Expr: qsbridge.AggregateRef("order_count", 0), Alias: "order_count", Type: qsbridge.DataTypeInt},
	}
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.Field(firstName), Direction: qsbridge.SortAscending}}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			if request.Index != "customers_qa" {
				return qsbridge.QuantaProjectedRowSet{Index: request.Index, Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...)}, nil, nil
			}
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field: request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueString, Value: "Abe"},
						{Kind: qsbridge.ValueString, Value: "Abby"},
					},
				}},
			}, nil, nil
		}),
	}
	result, err := executor.legacyDirectRelationshipAggregateResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "orders_qa", parentTable: "customers_qa"},
		[]qsbridge.QuantaRownum{11, 12, 21},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 1}, {child: 21, parent: 2}},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("aggregate result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two grouped rows", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "Abby" || chunk.Rows[0][1].Value != int64(1) {
		t.Fatalf("first row = %#v, want Abby/1", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != "Abe" || chunk.Rows[1][1].Value != int64(2) {
		t.Fatalf("second row = %#v, want Abe/2", chunk.Rows[1])
	}
}

func TestLegacyDirectRelationshipAggregateResultGroupsJoinedRowsByMultipleFields(t *testing.T) {
	customers := qsbridge.TableInstance{Table: "customers_qa", Alias: "c"}
	city := qsbridge.FieldRef{Table: customers, Name: "city", Type: qsbridge.DataTypeString}
	firstName := qsbridge.FieldRef{Table: customers, Name: "first_name", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{
				Index:   "customers_qa",
				Field:   "city",
				Type:    qsbridge.DataTypeString,
				Visible: true,
			},
			{
				Index:   "customers_qa",
				Field:   "first_name",
				Type:    qsbridge.DataTypeString,
				Visible: true,
			},
		},
	})
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(city), qsbridge.Field(firstName)}
	request.SQLAggregates = []qsbridge.Aggregate{
		{Function: "count", Alias: "order_count", Type: qsbridge.DataTypeInt},
	}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(city), Type: qsbridge.DataTypeString},
		{Expr: qsbridge.Field(firstName), Type: qsbridge.DataTypeString},
		{Expr: qsbridge.AggregateRef("order_count", 0), Alias: "order_count", Type: qsbridge.DataTypeInt},
	}
	request.OrderBy = []qsbridge.SortSpec{
		{Expr: qsbridge.Field(city), Direction: qsbridge.SortAscending},
		{Expr: qsbridge.Field(firstName), Direction: qsbridge.SortAscending},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			if request.Index != "customers_qa" {
				return qsbridge.QuantaProjectedRowSet{Index: request.Index, Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...)}, nil, nil
			}
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{
					{
						Field: request.ProjectionFields[0],
						Values: []qsbridge.ResultCell{
							{Kind: qsbridge.ValueString, Value: "Seattle"},
							{Kind: qsbridge.ValueString, Value: "Tacoma"},
						},
					},
					{
						Field: request.ProjectionFields[1],
						Values: []qsbridge.ResultCell{
							{Kind: qsbridge.ValueString, Value: "Abe"},
							{Kind: qsbridge.ValueString, Value: "Abby"},
						},
					},
				},
			}, nil, nil
		}),
	}
	result, err := executor.legacyDirectRelationshipAggregateResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "orders_qa", parentTable: "customers_qa"},
		[]qsbridge.QuantaRownum{11, 12, 21},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 1}, {child: 21, parent: 2}},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("aggregate result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two grouped rows", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "Seattle" || chunk.Rows[0][1].Value != "Abe" || chunk.Rows[0][2].Value != int64(2) {
		t.Fatalf("first row = %#v, want Seattle/Abe/2", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != "Tacoma" || chunk.Rows[1][1].Value != "Abby" || chunk.Rows[1][2].Value != int64(1) {
		t.Fatalf("second row = %#v, want Tacoma/Abby/1", chunk.Rows[1])
	}
}

func TestLegacyDirectRelationshipAggregateResultCountsDistinctGroupedValues(t *testing.T) {
	customers := qsbridge.TableInstance{Table: "customers_qa", Alias: "c"}
	orders := qsbridge.TableInstance{Table: "orders_qa", Alias: "o"}
	firstName := qsbridge.FieldRef{Table: customers, Name: "first_name", Type: qsbridge.DataTypeString}
	shipVia := qsbridge.FieldRef{Table: orders, Name: "ship_via", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{
				Index:   "customers_qa",
				Field:   "first_name",
				Type:    qsbridge.DataTypeString,
				Visible: true,
			},
			{
				Index: "orders_qa",
				Field: "ship_via",
				Type:  qsbridge.DataTypeString,
			},
		},
	})
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(firstName)}
	request.SQLAggregates = []qsbridge.Aggregate{
		{Function: "count", Mode: qsbridge.AggregateDistinct, Input: qsbridge.Field(shipVia), Alias: "carrier_count", Type: qsbridge.DataTypeInt},
	}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(firstName), Type: qsbridge.DataTypeString},
		{Expr: qsbridge.AggregateRef("carrier_count", 0), Alias: "carrier_count", Type: qsbridge.DataTypeInt},
	}
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.Field(firstName), Direction: qsbridge.SortAscending}}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			switch request.Index {
			case "customers_qa":
				return qsbridge.QuantaProjectedRowSet{
					Index:   request.Index,
					Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
					ProjectionVectors: []qsbridge.QuantaProjectionVector{{
						Field: request.ProjectionFields[0],
						Values: []qsbridge.ResultCell{
							{Kind: qsbridge.ValueString, Value: "Abe"},
							{Kind: qsbridge.ValueString, Value: "Abby"},
						},
					}},
				}, nil, nil
			case "orders_qa":
				return qsbridge.QuantaProjectedRowSet{
					Index:   request.Index,
					Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
					ProjectionVectors: []qsbridge.QuantaProjectionVector{{
						Field: request.ProjectionFields[0],
						Values: []qsbridge.ResultCell{
							{Kind: qsbridge.ValueString, Value: "UPS"},
							{Kind: qsbridge.ValueString, Value: "UPS"},
							{Kind: qsbridge.ValueString, Value: "FEDEX"},
						},
					}},
				}, nil, nil
			default:
				return qsbridge.QuantaProjectedRowSet{Index: request.Index, Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...)}, nil, nil
			}
		}),
	}
	result, err := executor.legacyDirectRelationshipAggregateResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "orders_qa", parentTable: "customers_qa"},
		[]qsbridge.QuantaRownum{11, 12, 21},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 1}, {child: 21, parent: 2}},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("aggregate result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two grouped rows", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "Abby" || chunk.Rows[0][1].Value != int64(1) {
		t.Fatalf("first row = %#v, want Abby/1", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != "Abe" || chunk.Rows[1][1].Value != int64(1) {
		t.Fatalf("second row = %#v, want Abe/1", chunk.Rows[1])
	}
}

func TestLegacyDirectRelationshipAggregateResultOrdersGroupedRowsByMultipleKeys(t *testing.T) {
	customers := qsbridge.TableInstance{Table: "customers_qa", Alias: "c"}
	firstName := qsbridge.FieldRef{Table: customers, Name: "first_name", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index:   "customers_qa",
			Field:   "first_name",
			Type:    qsbridge.DataTypeString,
			Visible: true,
		}},
	})
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(firstName)}
	request.SQLAggregates = []qsbridge.Aggregate{
		{Function: "count", Alias: "order_count", Type: qsbridge.DataTypeInt},
	}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(firstName), Type: qsbridge.DataTypeString},
		{Expr: qsbridge.AggregateRef("order_count", 0), Alias: "order_count", Type: qsbridge.DataTypeInt},
	}
	request.OrderBy = []qsbridge.SortSpec{
		{Expr: qsbridge.AggregateRef("order_count", 0), Direction: qsbridge.SortDescending},
		{Expr: qsbridge.Field(firstName), Direction: qsbridge.SortAscending},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			if request.Index != "customers_qa" {
				return qsbridge.QuantaProjectedRowSet{Index: request.Index, Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...)}, nil, nil
			}
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field: request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueString, Value: "Abe"},
						{Kind: qsbridge.ValueString, Value: "Abby"},
					},
				}},
			}, nil, nil
		}),
	}
	result, err := executor.legacyDirectRelationshipAggregateResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "orders_qa", parentTable: "customers_qa"},
		[]qsbridge.QuantaRownum{11, 12, 21},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 1}, {child: 21, parent: 2}},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("aggregate result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two grouped rows", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "Abe" || chunk.Rows[0][1].Value != int64(2) {
		t.Fatalf("first row = %#v, want Abe/2", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != "Abby" || chunk.Rows[1][1].Value != int64(1) {
		t.Fatalf("second row = %#v, want Abby/1", chunk.Rows[1])
	}
}

func TestLegacyDirectRelationshipAggregateResultOrdersGroupedNumericAggregate(t *testing.T) {
	customers := qsbridge.TableInstance{Table: "customers_qa", Alias: "c"}
	orders := qsbridge.TableInstance{Table: "orders_qa", Alias: "o"}
	firstName := qsbridge.FieldRef{Table: customers, Name: "first_name", Type: qsbridge.DataTypeString}
	orderID := qsbridge.FieldRef{Table: orders, Name: "order_id", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{
				Index:   "customers_qa",
				Field:   "first_name",
				Type:    qsbridge.DataTypeString,
				Visible: true,
			},
			{
				Index: "orders_qa",
				Field: "order_id",
				Type:  qsbridge.DataTypeInt,
			},
		},
	})
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(firstName)}
	request.SQLAggregates = []qsbridge.Aggregate{
		{Function: "sum", Input: qsbridge.Field(orderID), Alias: "order_total", Type: qsbridge.DataTypeFloat},
	}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(firstName), Type: qsbridge.DataTypeString},
		{Expr: qsbridge.AggregateRef("order_total", 0), Alias: "order_total", Type: qsbridge.DataTypeFloat},
	}
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.AggregateRef("order_total", 0), Direction: qsbridge.SortDescending}}
	request.Result.Limit = 1
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			switch request.Index {
			case "customers_qa":
				return qsbridge.QuantaProjectedRowSet{
					Index:   request.Index,
					Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
					ProjectionVectors: []qsbridge.QuantaProjectionVector{{
						Field: request.ProjectionFields[0],
						Values: []qsbridge.ResultCell{
							{Kind: qsbridge.ValueString, Value: "Abe"},
							{Kind: qsbridge.ValueString, Value: "Abby"},
						},
					}},
				}, nil, nil
			case "orders_qa":
				return qsbridge.QuantaProjectedRowSet{
					Index:   request.Index,
					Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
					ProjectionVectors: []qsbridge.QuantaProjectionVector{{
						Field: request.ProjectionFields[0],
						Values: []qsbridge.ResultCell{
							{Kind: qsbridge.ValueInt, Value: int64(1001)},
							{Kind: qsbridge.ValueInt, Value: int64(1002)},
							{Kind: qsbridge.ValueInt, Value: int64(2001)},
						},
					}},
				}, nil, nil
			default:
				return qsbridge.QuantaProjectedRowSet{Index: request.Index, Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...)}, nil, nil
			}
		}),
	}
	result, err := executor.legacyDirectRelationshipAggregateResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "orders_qa", parentTable: "customers_qa"},
		[]qsbridge.QuantaRownum{11, 12, 21},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 1}, {child: 21, parent: 2}},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("aggregate result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 1 {
		t.Fatalf("rows = %#v, want one limited grouped row", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "Abe" || chunk.Rows[0][1].Value != float64(2003) {
		t.Fatalf("row = %#v, want Abe/2003", chunk.Rows[0])
	}
}

func TestLegacyDirectRelationshipAggregateResultFiltersGroupedHaving(t *testing.T) {
	customers := qsbridge.TableInstance{Table: "customers_qa", Alias: "c"}
	orders := qsbridge.TableInstance{Table: "orders_qa", Alias: "o"}
	firstName := qsbridge.FieldRef{Table: customers, Name: "first_name", Type: qsbridge.DataTypeString}
	orderID := qsbridge.FieldRef{Table: orders, Name: "order_id", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{
				Index:   "customers_qa",
				Field:   "first_name",
				Type:    qsbridge.DataTypeString,
				Visible: true,
			},
			{
				Index: "orders_qa",
				Field: "order_id",
				Type:  qsbridge.DataTypeInt,
			},
		},
	})
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(firstName)}
	request.SQLAggregates = []qsbridge.Aggregate{
		{Function: "sum", Input: qsbridge.Field(orderID), Alias: "order_total", Type: qsbridge.DataTypeFloat},
	}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(firstName), Type: qsbridge.DataTypeString},
		{Expr: qsbridge.AggregateRef("order_total", 0), Alias: "order_total", Type: qsbridge.DataTypeFloat},
	}
	request.Having = []qsbridge.Predicate{{
		Expr: qsbridge.Binary(qsbridge.BinaryOpGreater, qsbridge.AggregateRef("order_total", 0), qsbridge.Literal(qsbridge.ValueInt, int64(2001))),
	}}
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.Field(firstName), Direction: qsbridge.SortAscending}}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			switch request.Index {
			case "customers_qa":
				return qsbridge.QuantaProjectedRowSet{
					Index:   request.Index,
					Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
					ProjectionVectors: []qsbridge.QuantaProjectionVector{{
						Field: request.ProjectionFields[0],
						Values: []qsbridge.ResultCell{
							{Kind: qsbridge.ValueString, Value: "Abe"},
							{Kind: qsbridge.ValueString, Value: "Abby"},
						},
					}},
				}, nil, nil
			case "orders_qa":
				return qsbridge.QuantaProjectedRowSet{
					Index:   request.Index,
					Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
					ProjectionVectors: []qsbridge.QuantaProjectionVector{{
						Field: request.ProjectionFields[0],
						Values: []qsbridge.ResultCell{
							{Kind: qsbridge.ValueInt, Value: int64(1001)},
							{Kind: qsbridge.ValueInt, Value: int64(1002)},
							{Kind: qsbridge.ValueInt, Value: int64(2001)},
						},
					}},
				}, nil, nil
			default:
				return qsbridge.QuantaProjectedRowSet{Index: request.Index, Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...)}, nil, nil
			}
		}),
	}
	result, err := executor.legacyDirectRelationshipAggregateResult(
		context.Background(),
		request,
		legacyDirectRelationshipEdge{childTable: "orders_qa", parentTable: "customers_qa"},
		[]qsbridge.QuantaRownum{11, 12, 21},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 1}, {child: 21, parent: 2}},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("aggregate result: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 1 {
		t.Fatalf("rows = %#v, want one HAVING-filtered grouped row", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "Abe" || chunk.Rows[0][1].Value != float64(2003) {
		t.Fatalf("row = %#v, want Abe/2003", chunk.Rows[0])
	}
}

func TestLegacyDirectRelationshipShapeDiagnosticsAllowsMembershipComposition(t *testing.T) {
	request := ExecutionRequest{
		Memberships: []qsbridge.MembershipEdge{{
			Kind: qsbridge.MembershipAnti,
		}},
	}
	diagnostics := legacyDirectRelationshipShapeDiagnostics(request, RelationshipVectorJoinRequest{
		RootIndex: "orders_qa",
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want membership composition accepted", diagnostics)
	}
}

func TestLegacyDirectRelationshipTupleMembershipUsesBSIFastPath(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	rownums := make([]qsbridge.QuantaRownum, 0, directBitmapMembershipMaxDynamicBatchEQValues+2)
	orderKeys := make(map[uint64]int64, directBitmapMembershipMaxDynamicBatchEQValues+2)
	suppKeys := make(map[uint64]int64, directBitmapMembershipMaxDynamicBatchEQValues+2)
	for i := 1; i <= directBitmapMembershipMaxDynamicBatchEQValues+2; i++ {
		rownum := qsbridge.QuantaRownum(i)
		rownums = append(rownums, rownum)
		orderKeys[uint64(rownum)] = int64(1000 + i)
		suppKeys[uint64(rownum)] = 1
	}
	orderKeys[1] = 10
	orderKeys[2] = 10
	suppKeys[1] = 1
	suppKeys[2] = 2
	reader := fakeMembershipProjectionBSIReader{
		Values: map[string]map[uint64]int64{
			"l_orderkey": orderKeys,
			"l_suppkey":  suppKeys,
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ProjectionBSIReader: reader,
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("relationship graph membership should use raw BSI vectors instead of materializing %s", request.Index)
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					values := directBitmapTestBatchEQValues(request.Query.Fragments, "l_orderkey")
					filtered := make([]qsbridge.QuantaRownum, 0, len(rownums))
					for _, rownum := range rownums {
						if len(values) > 0 {
							if _, ok := values[reader.Values["l_orderkey"][uint64(rownum)]]; !ok {
								continue
							}
						}
						filtered = append(filtered, rownum)
					}
					return BitmapQueryResult{Success: true, Count: uint64(len(filtered)), Rownums: filtered}, nil, nil
				},
			}, nil, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Memberships = []qsbridge.MembershipEdge{{
		Left:  l1OrderKey,
		Right: l2OrderKey,
		Kind:  qsbridge.MembershipSemi,
		Legal: true,
		Predicates: []qsbridge.Predicate{
			{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l2OrderKey), qsbridge.Field(l1OrderKey))},
			{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
		},
	}}
	tupleRows := RelationshipTupleRowSet{Rows: make([]RelationshipTupleRow, 0, len(rownums))}
	for _, rownum := range rownums {
		tupleRows.Rows = append(tupleRows.Rows, RelationshipTupleRow{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l1": rownum}})
	}
	alignedRows := map[string][]qsbridge.QuantaRownum{
		"l1": append([]qsbridge.QuantaRownum(nil), rownums...),
	}

	filtered, filteredAligned, probes, diagnostics, err := executor.legacyDirectRelationshipApplyTupleMemberships(context.Background(), request, tupleRows, alignedRows, nil)
	if err != nil {
		t.Fatalf("apply tuple memberships: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	assertRelationshipTupleRows(t, filtered, []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{
		{"l1": 1},
		{"l1": 2},
	})
	if got := filteredAligned["l1"]; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("filtered aligned rows = %#v, want [1 2]", filteredAligned)
	}
	assertExecutionProbeName(t, probes, "direct_bitmap_membership", "correlated_sibling_bsi_fast_path_applied")
	assertExecutionProbeName(t, probes, "direct_bitmap_membership", "correlated_sibling_bsi_right_vector_reuse")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_bsi_key_mode", "int64")
}

func TestLegacyDirectRelationshipTupleMembershipObservesGraphCandidateDerivation(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	membership := qsbridge.MembershipEdge{
		Left:  l1OrderKey,
		Right: l2OrderKey,
		Kind:  qsbridge.MembershipSemi,
		Legal: true,
		Predicates: []qsbridge.Predicate{
			{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
		},
	}
	tupleRows := RelationshipTupleRowSet{Rows: []RelationshipTupleRow{
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"o": 101, "l1": 1001}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"o": 101, "l1": 1002}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"o": 202, "l1": 2001}},
	}}
	alignedRows := map[string][]qsbridge.QuantaRownum{
		"o":  {101, 101, 202},
		"l1": {1001, 1002, 2001},
	}
	edges := []legacyDirectRelationshipEdge{{
		childRole:   "l1",
		childTable:  "lineitem",
		childField:  "l_orderkey",
		parentRole:  "o",
		parentTable: "orders",
		parentField: "o_orderkey",
		sqlKind:     qsbridge.JoinKindInner,
	}}

	observation := legacyDirectRelationshipObserveTupleMembershipCandidateDerivation(membership, "l1", tupleRows, alignedRows, edges)
	probes := legacyDirectRelationshipTupleMembershipCandidateDerivationProbes("graph_membership_1_", observation)

	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_available", "true")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_mode", "parent_role_vector_expansion")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_parent_role", "o")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_left_role", "l1")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_right_role", "l2")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_edge", "orders.o_orderkey->lineitem.l_orderkey")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_parent_rows", "3")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_unique_parent_rows", "2")

	missingParent := map[string][]qsbridge.QuantaRownum{
		"l1": {1001, 1002, 2001},
	}
	observation = legacyDirectRelationshipObserveTupleMembershipCandidateDerivation(membership, "l1", tupleRows, missingParent, edges)
	probes = legacyDirectRelationshipTupleMembershipCandidateDerivationProbes("graph_membership_1_", observation)
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_available", "false")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_reason", "parent_role_not_aligned")
}

func TestLegacyDirectRelationshipTupleMembershipUsesGraphDerivedRightCandidateSeed(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	queryCalls := 0
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
		RelationshipProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				10: 9001,
				11: 9001,
				12: 9001,
				20: 2,
			}),
		},
		RelationshipSourceKeyReader: fakeLegacyDirectRelationshipVectorSourceKeyReader{
			Values: []int64{9001},
		},
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			valuesByField := map[string]map[qsbridge.QuantaRownum]qsbridge.ResultCell{
				"l_orderkey": {
					10: {Kind: qsbridge.ValueInt, Value: int64(9001)},
					11: {Kind: qsbridge.ValueInt, Value: int64(9001)},
					12: {Kind: qsbridge.ValueInt, Value: int64(9001)},
					20: {Kind: qsbridge.ValueInt, Value: int64(2)},
				},
				"l_suppkey": {
					10: {Kind: qsbridge.ValueInt, Value: int64(7)},
					11: {Kind: qsbridge.ValueInt, Value: int64(7)},
					12: {Kind: qsbridge.ValueInt, Value: int64(8)},
					20: {Kind: qsbridge.ValueInt, Value: int64(9)},
				},
			}
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				name := field.PhysicalName
				if name == "" {
					name = field.Field
				}
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					vector.Values = append(vector.Values, valuesByField[name][rownum])
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					queryCalls++
					if len(request.Query.Fragments) != 1 {
						t.Fatalf("candidate fragments = %#v, want one BATCH_EQ relationship-vector fragment", request.Query.Fragments)
					}
					fragment := request.Query.Fragments[0]
					if fragment.Index != "lineitem" || fragment.Field != "l_orderkey" || fragment.BSIOp != qsbridge.QuantaBSIOpBatchEQ {
						t.Fatalf("candidate fragment = %#v, want lineitem.l_orderkey BATCH_EQ", fragment)
					}
					return BitmapQueryResult{
						Success: true,
						Count:   3,
						Rownums: []qsbridge.QuantaRownum{10, 11, 12},
					}, nil, nil
				},
			}, nil, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Memberships = []qsbridge.MembershipEdge{{
		Left:  l1OrderKey,
		Right: l2OrderKey,
		Kind:  qsbridge.MembershipSemi,
		Legal: true,
		Predicates: []qsbridge.Predicate{
			{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
		},
	}}
	tupleRows := RelationshipTupleRowSet{Rows: []RelationshipTupleRow{
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"o": 1, "l1": 10}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"o": 1, "l1": 11}},
	}}
	alignedRows := map[string][]qsbridge.QuantaRownum{
		"o":  {1, 1},
		"l1": {10, 11},
	}
	edges := []legacyDirectRelationshipEdge{{
		childRole:   "l1",
		childTable:  "lineitem",
		childField:  "l_orderkey",
		parentRole:  "o",
		parentTable: "orders",
		parentField: "o_orderkey",
		sqlKind:     qsbridge.JoinKindInner,
	}}

	filtered, filteredAligned, probes, diagnostics, err := executor.legacyDirectRelationshipApplyTupleMemberships(context.Background(), request, tupleRows, alignedRows, edges)
	if err != nil {
		t.Fatalf("apply tuple memberships: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if queryCalls != 1 {
		t.Fatalf("relationship-vector candidate query calls = %d, want 1", queryCalls)
	}
	assertRelationshipTupleRows(t, filtered, []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{
		{"o": 1, "l1": 10},
		{"o": 1, "l1": 11},
	})
	if got := filteredAligned["l1"]; len(got) != 2 || got[0] != 10 || got[1] != 11 {
		t.Fatalf("filtered aligned rows = %#v, want l1 [10 11]", filteredAligned)
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_applied", "true")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_rows", "3")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_source_key_projection_used", "true")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_source_key_projection_reason", "projected_source_key")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "membership_right_candidate_seed_reuse", "true")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "membership_right_candidate_seed_mode", "graph_parent_vector_expansion")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_right_narrow_reason", "right_candidate_seed")
}

func TestLegacyDirectRelationshipTupleMembershipDerivedSeedAllowsResidualRightOnlyPredicate(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2ReceiptDate := qsbridge.FieldRef{Table: l2, Name: "l_receiptdate", PhysicalName: "l_receiptdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}
	l2CommitDate := qsbridge.FieldRef{Table: l2, Name: "l_commitdate", PhysicalName: "l_commitdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}

	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
		RelationshipProjectionReader: fakeLegacyDirectRelationshipVectorProjectionReader{
			BSI: testRelationshipVectorBSI(map[uint64]int64{
				10: 9001,
				11: 9001,
				20: 2,
			}),
		},
		RelationshipSourceKeyReader: fakeLegacyDirectRelationshipVectorSourceKeyReader{
			Values: []int64{9001},
		},
	}
	membership := qsbridge.MembershipEdge{
		Left:  l1OrderKey,
		Right: l2OrderKey,
		Kind:  qsbridge.MembershipAnti,
		Legal: true,
		Predicates: []qsbridge.Predicate{
			{
				Placement: qsbridge.PredicateResidualScan,
				Expr:      qsbridge.Binary(qsbridge.BinaryOpGreater, qsbridge.Field(l2ReceiptDate), qsbridge.Field(l2CommitDate)),
			},
			{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
		},
	}
	tupleRows := RelationshipTupleRowSet{Rows: []RelationshipTupleRow{
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"o": 1, "l1": 10}},
	}}
	observation := legacyDirectRelationshipTupleMembershipCandidateDerivationObservation{
		available:        true,
		mode:             "parent_role_vector_expansion",
		parentRole:       "o",
		leftRole:         "l1",
		rightRole:        "l2",
		edgeDetail:       "orders.o_orderkey->lineitem.l_orderkey",
		parentRows:       1,
		uniqueParentRows: 1,
		edge: legacyDirectRelationshipEdge{
			childRole:   "l1",
			childTable:  "lineitem",
			childField:  "l_orderkey",
			parentRole:  "o",
			parentTable: "orders",
			parentField: "o_orderkey",
			sqlKind:     qsbridge.JoinKindInner,
		},
	}

	seed, probes, ok := executor.legacyDirectRelationshipTupleMembershipDerivedRightCandidateSeed(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}),
		membership,
		tupleRows,
		"graph_membership_1_",
		observation,
	)
	if !ok {
		t.Fatalf("derived seed not applied; probes=%#v", probes)
	}
	if got := seed.Rownums; len(got) != 2 || got[0] != 10 || got[1] != 11 {
		t.Fatalf("seed rownums = %#v, want [10 11]", got)
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_applied", "true")
	assertExecutionProbe(t, probes, "relationship_join", "graph_membership_1_candidate_derivation_source_key_projection_used", "true")
}

func TestLegacyDirectRelationshipTupleMembershipBSIFastPathPolicyRequiresLargeReusableDomain(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	membership := qsbridge.MembershipEdge{
		Left:  l1OrderKey,
		Right: l2OrderKey,
		Kind:  qsbridge.MembershipSemi,
		Legal: true,
		Predicates: []qsbridge.Predicate{
			{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l2OrderKey), qsbridge.Field(l1OrderKey))},
			{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
		},
	}

	small := []qsbridge.QuantaRownum{1, 2, 3}
	if legacyDirectRelationshipTupleMembershipShouldUseBSIFastPath(small, membership) {
		t.Fatalf("small graph membership candidate set should stay on materialized path")
	}
	large := make([]qsbridge.QuantaRownum, 0, directBitmapMembershipMaxDynamicBatchEQValues+1)
	for i := 1; i <= directBitmapMembershipMaxDynamicBatchEQValues+1; i++ {
		large = append(large, qsbridge.QuantaRownum(i))
	}
	if !legacyDirectRelationshipTupleMembershipShouldUseBSIFastPath(large, membership) {
		t.Fatalf("large reusable graph membership candidate set should use BSI fast path")
	}

	l2OtherOrderKey := qsbridge.FieldRef{Table: l2, Name: "other_orderkey", PhysicalName: "other_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	mismatch := membership
	mismatch.Right = l2OtherOrderKey
	mismatch.Predicates = []qsbridge.Predicate{
		{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l2OtherOrderKey), qsbridge.Field(l1OrderKey))},
		{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
	}
	if legacyDirectRelationshipTupleMembershipShouldUseBSIFastPath(large, mismatch) {
		t.Fatalf("graph membership BSI fast path should require reusable matching BSI fields")
	}
}

func TestLegacyDirectRelationshipApplyMembershipFiltersJoinedPairs(t *testing.T) {
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	supplier := qsbridge.TableInstance{Table: "supplier", Alias: "s"}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	sSuppkey := qsbridge.FieldRef{Table: supplier, Name: "s_suppkey", Type: qsbridge.DataTypeInt}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			values := make([]qsbridge.ResultCell, 0, len(request.Rownums))
			for _, rownum := range request.Rownums {
				switch rownum {
				case 11:
					values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(100)})
				case 12:
					values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(200)})
				case 13:
					values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(300)})
				default:
					values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueNull})
				}
			}
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field:  request.ProjectionFields[0],
					Values: values,
				}},
			}, nil, nil
		}),
	}
	pairs := []legacyDirectRelationshipPair{
		{child: 11, parent: 1},
		{child: 12, parent: 2},
		{child: 13, parent: 3},
	}
	rightValues := map[string]struct{}{
		directBitmapGroupKey(qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(200)}): {},
	}
	filtered, diagnostics, err := executor.legacyDirectRelationshipApplyMembership(
		context.Background(),
		executor.projectionMaterializationKernel(),
		legacyDirectRelationshipEdge{childTable: "partsupp", parentTable: "part"},
		pairs,
		qsbridge.MembershipEdge{Left: psSuppkey, Right: sSuppkey, Kind: qsbridge.MembershipAnti},
		rightValues,
	)
	if err != nil {
		t.Fatalf("apply membership: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(filtered) != 2 || filtered[0].child != 11 || filtered[1].child != 13 {
		t.Fatalf("filtered pairs = %#v, want child rows 11 and 13", filtered)
	}
}

func TestLegacyDirectRelationshipApplyMembershipShortCircuitsEmptyAntiRHS(t *testing.T) {
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	supplier := qsbridge.TableInstance{Table: "supplier", Alias: "s"}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	sSuppkey := qsbridge.FieldRef{Table: supplier, Name: "s_suppkey", Type: qsbridge.DataTypeInt}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("materializer should not be called for empty anti-membership RHS")
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
	}
	pairs := []legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 2}}
	filtered, diagnostics, err := executor.legacyDirectRelationshipApplyMembership(
		context.Background(),
		executor.projectionMaterializationKernel(),
		legacyDirectRelationshipEdge{childTable: "partsupp", parentTable: "part"},
		pairs,
		qsbridge.MembershipEdge{Left: psSuppkey, Right: sSuppkey, Kind: qsbridge.MembershipAnti},
		map[string]struct{}{},
	)
	if err != nil {
		t.Fatalf("apply membership: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(filtered) != len(pairs) || filtered[0] != pairs[0] || filtered[1] != pairs[1] {
		t.Fatalf("filtered pairs = %#v, want original pairs", filtered)
	}
}

func TestLegacyDirectRelationshipApplyMembershipShortCircuitsEmptySemiRHS(t *testing.T) {
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	supplier := qsbridge.TableInstance{Table: "supplier", Alias: "s"}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	sSuppkey := qsbridge.FieldRef{Table: supplier, Name: "s_suppkey", Type: qsbridge.DataTypeInt}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("materializer should not be called for empty semi-membership RHS")
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
	}
	filtered, diagnostics, err := executor.legacyDirectRelationshipApplyMembership(
		context.Background(),
		executor.projectionMaterializationKernel(),
		legacyDirectRelationshipEdge{childTable: "partsupp", parentTable: "part"},
		[]legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 2}},
		qsbridge.MembershipEdge{Left: psSuppkey, Right: sSuppkey, Kind: qsbridge.MembershipSemi},
		map[string]struct{}{},
	)
	if err != nil {
		t.Fatalf("apply membership: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(filtered) != 0 {
		t.Fatalf("filtered pairs = %#v, want none", filtered)
	}
}

func TestLegacyDirectRelationshipFieldsForEdgeDropsMembershipOnlyFields(t *testing.T) {
	fields := []qsbridge.QuantaProjectionField{
		{Index: "part", Field: "p_brand", Type: qsbridge.DataTypeString},
		{Index: "partsupp", Field: "ps_suppkey", Type: qsbridge.DataTypeInt},
		{Index: "supplier", Field: "s_suppkey", Type: qsbridge.DataTypeInt},
	}
	filtered := legacyDirectRelationshipFieldsForEdge(fields, legacyDirectRelationshipEdge{
		childTable:  "partsupp",
		parentTable: "part",
	})
	if len(filtered) != 2 {
		t.Fatalf("filtered fields = %#v, want only relationship edge fields", filtered)
	}
	for _, field := range filtered {
		if field.Index == "supplier" {
			t.Fatalf("filtered fields = %#v, want supplier field dropped", filtered)
		}
	}
}

func TestLegacyDirectRelationshipPostReductionFieldsDropsUnusedJoinKeys(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	pPartkey := qsbridge.FieldRef{Table: part, Name: "p_partkey", Type: qsbridge.DataTypeInt}
	pBrand := qsbridge.FieldRef{Table: part, Name: "p_brand", Type: qsbridge.DataTypeString}
	pType := qsbridge.FieldRef{Table: part, Name: "p_type", Type: qsbridge.DataTypeString}
	pSize := qsbridge.FieldRef{Table: part, Name: "p_size", Type: qsbridge.DataTypeInt}
	psPartkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_partkey", Type: qsbridge.DataTypeInt}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	request := ExecutionRequest{
		GroupBy: []qsbridge.Expr{
			qsbridge.Field(pBrand),
			qsbridge.Field(pType),
			qsbridge.Field(pSize),
		},
		SQLAggregates: []qsbridge.Aggregate{{
			Function: "count",
			Mode:     qsbridge.AggregateDistinct,
			Input:    qsbridge.Field(psSuppkey),
			Alias:    "supplier_cnt",
			Type:     qsbridge.DataTypeInt,
		}},
	}
	fields := []qsbridge.QuantaProjectionField{
		{Index: "part", Field: "p_partkey", Type: qsbridge.DataTypeInt},
		{Index: "part", Field: "p_brand", Type: qsbridge.DataTypeString},
		{Index: "part", Field: "p_type", Type: qsbridge.DataTypeString},
		{Index: "part", Field: "p_size", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Field: "ps_partkey", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Field: "ps_suppkey", Type: qsbridge.DataTypeInt},
	}
	pruned := legacyDirectRelationshipPostReductionFields(request, fields)
	if len(pruned) != 4 {
		t.Fatalf("pruned fields = %#v, want four post-reduction fields", pruned)
	}
	assertLegacyDirectRelationshipProjectionField(t, pruned, "part", "p_brand")
	assertLegacyDirectRelationshipProjectionField(t, pruned, "part", "p_type")
	assertLegacyDirectRelationshipProjectionField(t, pruned, "part", "p_size")
	assertLegacyDirectRelationshipProjectionField(t, pruned, "partsupp", "ps_suppkey")
	assertNoLegacyDirectRelationshipProjectionField(t, pruned, pPartkey.Table.Table, pPartkey.Name)
	assertNoLegacyDirectRelationshipProjectionField(t, pruned, psPartkey.Table.Table, psPartkey.Name)
}

func TestLegacyDirectRelationshipPostReductionFieldsDropsPushedDownPredicateOnlyFields(t *testing.T) {
	customer := qsbridge.TableInstance{Table: "customer", Alias: "c"}
	order := qsbridge.TableInstance{Table: "orders", Alias: "o"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	cMktSegment := qsbridge.FieldRef{Table: customer, Name: "c_mktsegment", Type: qsbridge.DataTypeString}
	lOrderKey := qsbridge.FieldRef{Table: lineitem, Name: "l_orderkey", Type: qsbridge.DataTypeInt}
	lExtendedPrice := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	oOrderDate := qsbridge.FieldRef{Table: order, Name: "o_orderdate", Type: qsbridge.DataTypeTime}
	oShipPriority := qsbridge.FieldRef{Table: order, Name: "o_shippriority", Type: qsbridge.DataTypeInt}
	request := ExecutionRequest{
		Result: qsbridge.ResultShape{
			Columns: []qsbridge.FieldRef{lOrderKey, oOrderDate, oShipPriority},
			Hidden:  []qsbridge.FieldRef{lExtendedPrice},
		},
		Predicates: []qsbridge.Predicate{{
			Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(cMktSegment), qsbridge.Literal(qsbridge.ValueString, "BUILDING")),
			Placement: qsbridge.PredicatePushdown,
			Scope:     qsbridge.PredicateScopeWhere,
		}},
		GroupBy: []qsbridge.Expr{
			qsbridge.Field(lOrderKey),
			qsbridge.Field(oOrderDate),
			qsbridge.Field(oShipPriority),
		},
		SQLAggregates: []qsbridge.Aggregate{{
			Function: "sum",
			Input:    qsbridge.Field(lExtendedPrice),
			Alias:    "revenue",
			Type:     qsbridge.DataTypeFloat,
		}},
	}
	fields := []qsbridge.QuantaProjectionField{
		{Index: "customer", Role: "c", Field: "c_mktsegment", Type: qsbridge.DataTypeString},
		{Index: "lineitem", Role: "l", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
		{Index: "lineitem", Role: "l", Field: "l_orderkey", Type: qsbridge.DataTypeInt},
		{Index: "orders", Role: "o", Field: "o_orderdate", Type: qsbridge.DataTypeTime},
		{Index: "orders", Role: "o", Field: "o_shippriority", Type: qsbridge.DataTypeInt},
	}

	required := legacyDirectRelationshipPostReductionMaterializationFieldKeys(request)
	if len(required) == 0 {
		t.Fatalf("required keys unexpectedly empty")
	}
	pruned := legacyDirectRelationshipPostReductionMaterializationFields(request, fields)
	if len(pruned) != 4 {
		t.Fatalf("pruned fields = %#v, want only four grouped aggregate fields", pruned)
	}
	assertNoLegacyDirectRelationshipProjectionField(t, pruned, "customer", "c_mktsegment")
	assertLegacyDirectRelationshipProjectionField(t, pruned, "lineitem", "l_extendedprice")
	assertLegacyDirectRelationshipProjectionField(t, pruned, "lineitem", "l_orderkey")
	assertLegacyDirectRelationshipProjectionField(t, pruned, "orders", "o_orderdate")
	assertLegacyDirectRelationshipProjectionField(t, pruned, "orders", "o_shippriority")
}

func TestLegacyDirectRelationshipGraphSinkTableFindsConvergedSink(t *testing.T) {
	sink, diagnostics := legacyDirectRelationshipGraphSinkTable([]legacyDirectRelationshipEdge{
		{parentTable: "region", childTable: "nation"},
		{parentTable: "nation", childTable: "customer"},
		{parentTable: "customer", childTable: "orders"},
		{parentTable: "orders", childTable: "lineitem"},
		{parentTable: "supplier", childTable: "lineitem"},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if sink != "lineitem" {
		t.Fatalf("sink = %q, want lineitem", sink)
	}
}

func TestLegacyDirectRelationshipSiblingRootGraphShapeRecognizesQ9Siblings(t *testing.T) {
	shape, ok, diagnostics := legacyDirectRelationshipSiblingRootGraphShape([]legacyDirectRelationshipEdge{
		{parentTable: "part", childTable: "lineitem"},
		{parentTable: "part", childTable: "partsupp"},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !ok {
		t.Fatalf("sibling root shape not recognized")
	}
	if shape.rootRole != "part" || shape.rootTable != "part" {
		t.Fatalf("root = %s/%s, want part/part", shape.rootRole, shape.rootTable)
	}
	if got := legacyDirectRelationshipGraphSiblingRootDebug(shape); got != "part:part->lineitem,partsupp" {
		t.Fatalf("debug = %q, want part:part->lineitem,partsupp", got)
	}
}

func TestLegacyDirectRelationshipSiblingRootGraphShapeIgnoresConvergedSink(t *testing.T) {
	_, ok, diagnostics := legacyDirectRelationshipSiblingRootGraphShape([]legacyDirectRelationshipEdge{
		{parentTable: "region", childTable: "nation"},
		{parentTable: "nation", childTable: "customer"},
		{parentTable: "customer", childTable: "orders"},
		{parentTable: "orders", childTable: "lineitem"},
		{parentTable: "supplier", childTable: "lineitem"},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if ok {
		t.Fatalf("converged sink graph should not be classified as sibling root")
	}
}

func TestLegacyDirectRelationshipChildRowsByParentFiltersAndPreservesChildOrder(t *testing.T) {
	byParent := legacyDirectRelationshipChildRowsByParent(
		[]qsbridge.QuantaRownum{1, 2},
		[]qsbridge.QuantaRownum{11, 12, 21},
		[]legacyDirectRelationshipPair{
			{child: 9, parent: 1},
			{child: 11, parent: 1},
			{child: 12, parent: 1},
			{child: 12, parent: 1},
			{child: 21, parent: 2},
			{child: 31, parent: 3},
		},
	)
	if got := byParent[1]; len(got) != 2 || got[0] != 11 || got[1] != 12 {
		t.Fatalf("parent 1 children = %#v, want 11,12", got)
	}
	if got := byParent[2]; len(got) != 1 || got[0] != 21 {
		t.Fatalf("parent 2 children = %#v, want 21", got)
	}
	if _, ok := byParent[3]; ok {
		t.Fatalf("parent 3 should have been filtered: %#v", byParent)
	}
}

func TestLegacyDirectRelationshipTupleRowsFromReducedGraphTraversesReverseRelationshipEdges(t *testing.T) {
	rowSet, diagnostics := legacyDirectRelationshipTupleRowsFromReducedGraph(
		"p",
		[]qsbridge.QuantaRownum{1},
		[]legacyDirectRelationshipSiblingRootReducedEdge{
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
				childRows: []qsbridge.QuantaRownum{11, 12},
				pairs:     []legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 1}},
			},
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
				childRows: []qsbridge.QuantaRownum{101},
				pairs:     []legacyDirectRelationshipPair{{child: 101, parent: 1}},
			},
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "o", parentTable: "orders", childRole: "l", childTable: "lineitem"},
				childRows: []qsbridge.QuantaRownum{11, 12},
				pairs:     []legacyDirectRelationshipPair{{child: 11, parent: 201}, {child: 12, parent: 202}},
			},
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "s", parentTable: "supplier", childRole: "l", childTable: "lineitem"},
				childRows: []qsbridge.QuantaRownum{11, 12},
				pairs:     []legacyDirectRelationshipPair{{child: 11, parent: 301}, {child: 12, parent: 301}},
			},
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "n", parentTable: "nation", childRole: "s", childTable: "supplier"},
				childRows: []qsbridge.QuantaRownum{301},
				pairs:     []legacyDirectRelationshipPair{{child: 301, parent: 401}},
			},
		},
	)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	assertRelationshipTupleRows(t, rowSet, []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{
		{"p": 1, "l": 11, "ps": 101, "o": 201, "s": 301, "n": 401},
		{"p": 1, "l": 12, "ps": 101, "o": 202, "s": 301, "n": 401},
	})
}

func TestLegacyDirectRelationshipTupleRowsFromReducedGraphBuildsFormalQ9Shape(t *testing.T) {
	rowSet, diagnostics := legacyDirectRelationshipTupleRowsFromReducedGraph(
		"p",
		[]qsbridge.QuantaRownum{1},
		[]legacyDirectRelationshipSiblingRootReducedEdge{
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
				childRows: []qsbridge.QuantaRownum{11, 12},
				pairs:     []legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 1}},
			},
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
				childRows: []qsbridge.QuantaRownum{101, 102},
				pairs:     []legacyDirectRelationshipPair{{child: 101, parent: 1}, {child: 102, parent: 1}},
			},
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "l", parentTable: "lineitem", childRole: "o", childTable: "orders"},
				childRows: []qsbridge.QuantaRownum{201, 202},
				pairs:     []legacyDirectRelationshipPair{{child: 201, parent: 11}, {child: 202, parent: 12}},
			},
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "l", parentTable: "lineitem", childRole: "s", childTable: "supplier"},
				childRows: []qsbridge.QuantaRownum{301},
				pairs:     []legacyDirectRelationshipPair{{child: 301, parent: 11}, {child: 301, parent: 12}},
			},
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "s", parentTable: "supplier", childRole: "n", childTable: "nation"},
				childRows: []qsbridge.QuantaRownum{401},
				pairs:     []legacyDirectRelationshipPair{{child: 401, parent: 301}},
			},
		},
	)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	assertRelationshipTupleRows(t, rowSet, []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{
		{"p": 1, "l": 11, "ps": 101, "o": 201, "s": 301, "n": 401},
		{"p": 1, "l": 11, "ps": 102, "o": 201, "s": 301, "n": 401},
		{"p": 1, "l": 12, "ps": 101, "o": 202, "s": 301, "n": 401},
		{"p": 1, "l": 12, "ps": 102, "o": 202, "s": 301, "n": 401},
	})
}

func TestLegacyDirectRelationshipSiblingRootTupleRowsFromReducedEdgesBuildsQ9Expansion(t *testing.T) {
	shape, ok, diagnostics := legacyDirectRelationshipSiblingRootGraphShape([]legacyDirectRelationshipEdge{
		{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
		{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
	})
	if diagnostics.BlocksNative() || !ok {
		t.Fatalf("shape diagnostics = %#v ok=%t, want sibling-root shape", diagnostics, ok)
	}
	rowSet, diagnostics := legacyDirectRelationshipSiblingRootTupleRowsFromReducedEdges(
		shape,
		[]qsbridge.QuantaRownum{1, 2},
		[]legacyDirectRelationshipSiblingRootReducedEdge{
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
				childRows: []qsbridge.QuantaRownum{11, 12, 21},
				pairs: []legacyDirectRelationshipPair{
					{child: 11, parent: 1},
					{child: 12, parent: 1},
					{child: 21, parent: 2},
				},
			},
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
				childRows: []qsbridge.QuantaRownum{101, 102, 201},
				pairs: []legacyDirectRelationshipPair{
					{child: 101, parent: 1},
					{child: 102, parent: 1},
					{child: 201, parent: 2},
				},
			},
		},
	)
	if diagnostics.BlocksNative() {
		t.Fatalf("tuple diagnostics = %#v, want none", diagnostics)
	}
	assertRelationshipTupleRows(t, rowSet, []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{
		{"p": 1, "l": 11, "ps": 101},
		{"p": 1, "l": 11, "ps": 102},
		{"p": 1, "l": 12, "ps": 101},
		{"p": 1, "l": 12, "ps": 102},
		{"p": 2, "l": 21, "ps": 201},
	})
}

func TestLegacyDirectRelationshipSiblingRootTuplePreviewResultReportsTupleShape(t *testing.T) {
	shape, ok, diagnostics := legacyDirectRelationshipSiblingRootGraphShape([]legacyDirectRelationshipEdge{
		{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
		{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
	})
	if diagnostics.BlocksNative() || !ok {
		t.Fatalf("shape diagnostics = %#v ok=%t, want sibling-root shape", diagnostics, ok)
	}
	result := legacyDirectRelationshipSiblingRootTuplePreviewResult(
		shape,
		[]qsbridge.QuantaRownum{1, 2},
		[]legacyDirectRelationshipSiblingRootReducedEdge{
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
				childRows: []qsbridge.QuantaRownum{11, 12, 21},
				pairs: []legacyDirectRelationshipPair{
					{child: 11, parent: 1},
					{child: 12, parent: 1},
					{child: 21, parent: 2},
				},
			},
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
				childRows: []qsbridge.QuantaRownum{101, 102, 201},
				pairs: []legacyDirectRelationshipPair{
					{child: 101, parent: 1},
					{child: 102, parent: 1},
					{child: 201, parent: 2},
				},
			},
		},
	)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if result.Count != 5 {
		t.Fatalf("count = %d, want 5", result.Count)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_shape", "sibling_root")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_sibling_root", "p:part->l,ps")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_sibling_children", "lineitem,partsupp")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "roles", "l,p,ps")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "expanded_rows", "5")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "filtered_rows", "5")
}

func TestLegacyDirectRelationshipSiblingRootProjectedAggregateResultComputesQ9Profit(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	_ = part
	lSuppkey := qsbridge.FieldRef{Table: lineitem, Name: "l_suppkey", Type: qsbridge.DataTypeInt}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	extendedPrice := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	discount := qsbridge.FieldRef{Table: lineitem, Name: "l_discount", Type: qsbridge.DataTypeFloat}
	quantity := qsbridge.FieldRef{Table: lineitem, Name: "l_quantity", Type: qsbridge.DataTypeInt}
	supplyCost := qsbridge.FieldRef{Table: partsupp, Name: "ps_supplycost", Type: qsbridge.DataTypeFloat}
	shape, ok, diagnostics := legacyDirectRelationshipSiblingRootGraphShape([]legacyDirectRelationshipEdge{
		{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
		{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
	})
	if diagnostics.BlocksNative() || !ok {
		t.Fatalf("shape diagnostics = %#v ok=%t, want sibling-root shape", diagnostics, ok)
	}
	fields := []qsbridge.QuantaProjectionField{
		{Index: "lineitem", Role: "l", Field: "l_suppkey", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Role: "ps", Field: "ps_suppkey", Type: qsbridge.DataTypeInt},
		{Index: "lineitem", Role: "l", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
		{Index: "lineitem", Role: "l", Field: "l_discount", Type: qsbridge.DataTypeFloat},
		{Index: "lineitem", Role: "l", Field: "l_quantity", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Role: "ps", Field: "ps_supplycost", Type: qsbridge.DataTypeFloat},
	}
	values := RelationshipTupleValueStore{
		RelationshipTupleValueKeyForField(fields[0]): {
			11: {Kind: qsbridge.ValueInt, Value: int64(7)},
			12: {Kind: qsbridge.ValueInt, Value: int64(8)},
			21: {Kind: qsbridge.ValueInt, Value: int64(9)},
		},
		RelationshipTupleValueKeyForField(fields[1]): {
			101: {Kind: qsbridge.ValueInt, Value: int64(7)},
			102: {Kind: qsbridge.ValueInt, Value: int64(99)},
			201: {Kind: qsbridge.ValueInt, Value: int64(9)},
		},
		RelationshipTupleValueKeyForField(fields[2]): {
			11: {Kind: qsbridge.ValueFloat, Value: float64(1000)},
			12: {Kind: qsbridge.ValueFloat, Value: float64(300)},
			21: {Kind: qsbridge.ValueFloat, Value: float64(500)},
		},
		RelationshipTupleValueKeyForField(fields[3]): {
			11: {Kind: qsbridge.ValueFloat, Value: float64(0.10)},
			12: {Kind: qsbridge.ValueFloat, Value: float64(0.05)},
			21: {Kind: qsbridge.ValueFloat, Value: float64(0.20)},
		},
		RelationshipTupleValueKeyForField(fields[4]): {
			11: {Kind: qsbridge.ValueInt, Value: int64(5)},
			12: {Kind: qsbridge.ValueInt, Value: int64(3)},
			21: {Kind: qsbridge.ValueInt, Value: int64(10)},
		},
		RelationshipTupleValueKeyForField(fields[5]): {
			101: {Kind: qsbridge.ValueFloat, Value: float64(20)},
			102: {Kind: qsbridge.ValueFloat, Value: float64(40)},
			201: {Kind: qsbridge.ValueFloat, Value: float64(15)},
		},
	}
	residualRequest := ExecutionRequest{Joins: []qsbridge.JoinEdge{{On: []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(psSuppkey), qsbridge.Field(lSuppkey)),
		Placement: qsbridge.PredicateResidualJoin,
		Scope:     qsbridge.PredicateScopeOn,
	}}}}}
	profitExpr := qsbridge.Binary(
		qsbridge.BinaryOpSubtract,
		qsbridge.Binary(
			qsbridge.BinaryOpMultiply,
			qsbridge.Field(extendedPrice),
			qsbridge.Binary(qsbridge.BinaryOpSubtract, qsbridge.Literal(qsbridge.ValueInt, int64(1)), qsbridge.Field(discount)),
		),
		qsbridge.Binary(qsbridge.BinaryOpMultiply, qsbridge.Field(supplyCost), qsbridge.Field(quantity)),
	)
	aggregateRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{Index: "lineitem"}}})
	aggregateRequest.SQLAggregates = []qsbridge.Aggregate{{Function: "sum", Input: profitExpr, Alias: "profit", Type: qsbridge.DataTypeFloat}}
	result := legacyDirectRelationshipSiblingRootProjectedAggregateResult(
		shape,
		[]qsbridge.QuantaRownum{1, 2},
		[]legacyDirectRelationshipSiblingRootReducedEdge{
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
				childRows: []qsbridge.QuantaRownum{11, 12, 21},
				pairs: []legacyDirectRelationshipPair{
					{child: 11, parent: 1},
					{child: 12, parent: 1},
					{child: 21, parent: 2},
				},
			},
			{
				edge:      legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
				childRows: []qsbridge.QuantaRownum{101, 102, 201},
				pairs: []legacyDirectRelationshipPair{
					{child: 101, parent: 1},
					{child: 102, parent: 1},
					{child: 201, parent: 2},
				},
			},
		},
		"lineitem",
		fields,
		values,
		residualRequest,
		aggregateRequest,
	)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if got := chunk.Rows[0][0].Value; got != float64(1050) {
		t.Fatalf("profit = %#v, want 1050", got)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_shape", "sibling_root")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "roles", "l,p,ps")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "expanded_rows", "5")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "filtered_rows", "2")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "aggregate_alias", "profit")
}

func TestLegacyDirectRelationshipSiblingRootProjectedAggregateResultGroupsQ9Profit(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	pBrand := qsbridge.FieldRef{Table: part, Name: "p_brand", Type: qsbridge.DataTypeString}
	lSuppkey := qsbridge.FieldRef{Table: lineitem, Name: "l_suppkey", Type: qsbridge.DataTypeInt}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	extendedPrice := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	discount := qsbridge.FieldRef{Table: lineitem, Name: "l_discount", Type: qsbridge.DataTypeFloat}
	quantity := qsbridge.FieldRef{Table: lineitem, Name: "l_quantity", Type: qsbridge.DataTypeInt}
	supplyCost := qsbridge.FieldRef{Table: partsupp, Name: "ps_supplycost", Type: qsbridge.DataTypeFloat}
	shape, ok, diagnostics := legacyDirectRelationshipSiblingRootGraphShape([]legacyDirectRelationshipEdge{
		{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
		{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
	})
	if diagnostics.BlocksNative() || !ok {
		t.Fatalf("shape diagnostics = %#v ok=%t, want sibling-root shape", diagnostics, ok)
	}
	fields := []qsbridge.QuantaProjectionField{
		{Index: "part", Role: "p", Field: "p_brand", Type: qsbridge.DataTypeString},
		{Index: "lineitem", Role: "l", Field: "l_suppkey", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Role: "ps", Field: "ps_suppkey", Type: qsbridge.DataTypeInt},
		{Index: "lineitem", Role: "l", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
		{Index: "lineitem", Role: "l", Field: "l_discount", Type: qsbridge.DataTypeFloat},
		{Index: "lineitem", Role: "l", Field: "l_quantity", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Role: "ps", Field: "ps_supplycost", Type: qsbridge.DataTypeFloat},
	}
	values := RelationshipTupleValueStore{
		RelationshipTupleValueKeyForField(fields[0]): {1: {Kind: qsbridge.ValueString, Value: "Brand#11"}, 2: {Kind: qsbridge.ValueString, Value: "Brand#22"}},
		RelationshipTupleValueKeyForField(fields[1]): {11: {Kind: qsbridge.ValueInt, Value: int64(7)}, 12: {Kind: qsbridge.ValueInt, Value: int64(8)}, 21: {Kind: qsbridge.ValueInt, Value: int64(9)}},
		RelationshipTupleValueKeyForField(fields[2]): {101: {Kind: qsbridge.ValueInt, Value: int64(7)}, 102: {Kind: qsbridge.ValueInt, Value: int64(99)}, 201: {Kind: qsbridge.ValueInt, Value: int64(9)}},
		RelationshipTupleValueKeyForField(fields[3]): {11: {Kind: qsbridge.ValueFloat, Value: float64(1000)}, 12: {Kind: qsbridge.ValueFloat, Value: float64(300)}, 21: {Kind: qsbridge.ValueFloat, Value: float64(500)}},
		RelationshipTupleValueKeyForField(fields[4]): {11: {Kind: qsbridge.ValueFloat, Value: float64(0.10)}, 12: {Kind: qsbridge.ValueFloat, Value: float64(0.05)}, 21: {Kind: qsbridge.ValueFloat, Value: float64(0.20)}},
		RelationshipTupleValueKeyForField(fields[5]): {11: {Kind: qsbridge.ValueInt, Value: int64(5)}, 12: {Kind: qsbridge.ValueInt, Value: int64(3)}, 21: {Kind: qsbridge.ValueInt, Value: int64(10)}},
		RelationshipTupleValueKeyForField(fields[6]): {101: {Kind: qsbridge.ValueFloat, Value: float64(20)}, 102: {Kind: qsbridge.ValueFloat, Value: float64(40)}, 201: {Kind: qsbridge.ValueFloat, Value: float64(15)}},
	}
	residualRequest := ExecutionRequest{Joins: []qsbridge.JoinEdge{{On: []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(psSuppkey), qsbridge.Field(lSuppkey)),
		Placement: qsbridge.PredicateResidualJoin,
		Scope:     qsbridge.PredicateScopeOn,
	}}}}}
	profitExpr := qsbridge.Binary(
		qsbridge.BinaryOpSubtract,
		qsbridge.Binary(qsbridge.BinaryOpMultiply, qsbridge.Field(extendedPrice), qsbridge.Binary(qsbridge.BinaryOpSubtract, qsbridge.Literal(qsbridge.ValueInt, int64(1)), qsbridge.Field(discount))),
		qsbridge.Binary(qsbridge.BinaryOpMultiply, qsbridge.Field(supplyCost), qsbridge.Field(quantity)),
	)
	aggregateRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{Index: "lineitem"}}})
	aggregateRequest.GroupBy = []qsbridge.Expr{qsbridge.Field(pBrand)}
	aggregateRequest.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.Field(pBrand), Direction: qsbridge.SortAscending}}
	aggregateRequest.SQLAggregates = []qsbridge.Aggregate{{Function: "sum", Input: profitExpr, Alias: "profit", Type: qsbridge.DataTypeFloat}}
	result := legacyDirectRelationshipSiblingRootProjectedAggregateResult(
		shape,
		[]qsbridge.QuantaRownum{1, 2},
		[]legacyDirectRelationshipSiblingRootReducedEdge{
			{edge: legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"}, childRows: []qsbridge.QuantaRownum{11, 12, 21}, pairs: []legacyDirectRelationshipPair{{child: 11, parent: 1}, {child: 12, parent: 1}, {child: 21, parent: 2}}},
			{edge: legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"}, childRows: []qsbridge.QuantaRownum{101, 102, 201}, pairs: []legacyDirectRelationshipPair{{child: 101, parent: 1}, {child: 102, parent: 1}, {child: 201, parent: 2}}},
		},
		"lineitem",
		fields,
		values,
		residualRequest,
		aggregateRequest,
	)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two grouped rows", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "Brand#11" || chunk.Rows[0][1].Value != float64(800) {
		t.Fatalf("first row = %#v, want Brand#11/800", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != "Brand#22" || chunk.Rows[1][1].Value != float64(250) {
		t.Fatalf("second row = %#v, want Brand#22/250", chunk.Rows[1])
	}
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "filtered_rows", "2")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "aggregate_alias", "profit")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "groups", "2")
}

func TestLegacyDirectRelationshipQ18LargeOrderProjectionLateMaterializesSurvivors(t *testing.T) {
	customer := qsbridge.TableInstance{Table: "customer", Alias: "c"}
	orders := qsbridge.TableInstance{Table: "orders", Alias: "o"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	cName := qsbridge.FieldRef{Table: customer, Name: "c_name", Type: qsbridge.DataTypeString}
	cCustkey := qsbridge.FieldRef{Table: customer, Name: "c_custkey", Type: qsbridge.DataTypeInt}
	oOrderkey := qsbridge.FieldRef{Table: orders, Name: "o_orderkey", Type: qsbridge.DataTypeInt}
	oOrderdate := qsbridge.FieldRef{Table: orders, Name: "o_orderdate", Type: qsbridge.DataTypeTime}
	oTotalprice := qsbridge.FieldRef{Table: orders, Name: "o_totalprice", Type: qsbridge.DataTypeFloat}
	lQuantity := qsbridge.FieldRef{Table: lineitem, Name: "l_quantity", Type: qsbridge.DataTypeInt}
	fields := []qsbridge.QuantaProjectionField{
		{Index: "customer", Role: "c", Field: "c_name", Type: qsbridge.DataTypeString, Visible: true},
		{Index: "customer", Role: "c", Field: "c_custkey", Type: qsbridge.DataTypeInt, Visible: true},
		{Index: "orders", Role: "o", Field: "o_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
		{Index: "orders", Role: "o", Field: "o_orderdate", Type: qsbridge.DataTypeTime, Visible: true},
		{Index: "orders", Role: "o", Field: "o_totalprice", Type: qsbridge.DataTypeFloat, Visible: true},
		{Index: "lineitem", Role: "l", Field: "l_quantity", Type: qsbridge.DataTypeInt},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{ProjectionFields: fields})
	request.GroupBy = []qsbridge.Expr{
		qsbridge.Field(cName),
		qsbridge.Field(cCustkey),
		qsbridge.Field(oOrderkey),
		qsbridge.Field(oOrderdate),
		qsbridge.Field(oTotalprice),
	}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(cName), Type: qsbridge.DataTypeString},
		{Expr: qsbridge.Field(cCustkey), Type: qsbridge.DataTypeInt},
		{Expr: qsbridge.Field(oOrderkey), Type: qsbridge.DataTypeInt},
		{Expr: qsbridge.Field(oOrderdate), Type: qsbridge.DataTypeTime},
		{Expr: qsbridge.Field(oTotalprice), Type: qsbridge.DataTypeFloat},
		{Expr: qsbridge.AggregateRef("sum_quantity", 0), Alias: "sum_quantity", Type: qsbridge.DataTypeFloat},
	}
	request.ProjectionOrder = []qsbridge.FieldRef{cName, cCustkey, oOrderkey, oOrderdate, oTotalprice}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "sum", Input: qsbridge.Field(lQuantity), Alias: "sum_quantity", Type: qsbridge.DataTypeFloat}}
	request.Having = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpGreater, qsbridge.AggregateRef("sum_quantity", 0), qsbridge.Literal(qsbridge.ValueInt, int64(300))),
		Placement: qsbridge.PredicateResidualScan,
	}}
	request.OrderBy = []qsbridge.SortSpec{
		{Expr: qsbridge.Field(oTotalprice), Direction: qsbridge.SortDescending},
		{Expr: qsbridge.Field(oOrderdate), Direction: qsbridge.SortAscending},
	}
	request.Result = qsbridge.ResultShape{Kind: qsbridge.ResultQuery, Limit: 10}
	var materializations []qsbridge.QuantaMaterializationRequest
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materializations = append(materializations, request)
			rowSet := qsbridge.QuantaProjectedRowSet{Index: request.Index, Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...)}
			for _, field := range request.ProjectionFields {
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "l_quantity":
						values := map[qsbridge.QuantaRownum]int64{101: 200, 102: 150, 103: 100, 104: 400}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: values[rownum]})
					case "c_name":
						values := map[qsbridge.QuantaRownum]string{10: "Customer#10", 30: "Customer#30"}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: values[rownum]})
					case "o_orderdate":
						values := map[qsbridge.QuantaRownum]string{1: "1995-01-01", 3: "1995-01-03"}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: values[rownum]})
					case "o_totalprice":
						values := map[qsbridge.QuantaRownum]float64{1: 900, 3: 700}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: values[rownum]})
					default:
						t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}
	alignedRows := map[string][]qsbridge.QuantaRownum{
		"l": []qsbridge.QuantaRownum{101, 102, 103, 104},
		"o": []qsbridge.QuantaRownum{1, 1, 2, 3},
		"c": []qsbridge.QuantaRownum{10, 10, 20, 30},
	}
	result, handled, err := executor.legacyDirectRelationshipQ18LargeOrderProjectionResult(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{101, 102, 103, 104},
		[]legacyDirectRelationshipEdge{
			{childRole: "l", childTable: "lineitem", childField: "l_orderkey", parentRole: "o", parentTable: "orders", parentField: "o_orderkey"},
			{childRole: "o", childTable: "orders", childField: "o_custkey", parentRole: "c", parentTable: "customer", parentField: "c_custkey"},
		},
		fields,
		alignedRows,
		0,
		ExecutionResult{},
	)
	if err != nil {
		t.Fatalf("late q18 result: %v", err)
	}
	if !handled {
		t.Fatalf("handled = false, want Q18 late-materialization fast path")
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if len(materializations) != 3 {
		t.Fatalf("materializations = %d, want quantity plus two final role batches", len(materializations))
	}
	if got := materializations[0].Rownums; len(got) != 4 || got[0] != 101 || got[3] != 104 {
		t.Fatalf("quantity rownums = %#v, want all lineitem rows", got)
	}
	for _, request := range materializations[1:] {
		if len(request.Rownums) != 2 {
			t.Fatalf("final materialization %s rownums = %#v, want only two surviving groups", request.Index, request.Rownums)
		}
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two surviving orders", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "Customer#10" || chunk.Rows[0][2].Value != int64(1) || chunk.Rows[0][5].Value != float64(350) {
		t.Fatalf("first row = %#v, want customer 10/order 1/sum 350", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != "Customer#30" || chunk.Rows[1][2].Value != int64(3) || chunk.Rows[1][5].Value != float64(400) {
		t.Fatalf("second row = %#v, want customer 30/order 3/sum 400", chunk.Rows[1])
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_late_materialization", "q18_large_order_projection")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_post_having_groups", "2")
}

func TestLegacyDirectRelationshipQ3OrderRevenuePreAggregatesByOrder(t *testing.T) {
	orders := qsbridge.TableInstance{Table: "orders", Alias: "o"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	lOrderkey := qsbridge.FieldRef{Table: lineitem, Name: "l_orderkey", Type: qsbridge.DataTypeInt}
	lExtendedprice := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	oOrderdate := qsbridge.FieldRef{Table: orders, Name: "o_orderdate", Type: qsbridge.DataTypeTime}
	oShippriority := qsbridge.FieldRef{Table: orders, Name: "o_shippriority", Type: qsbridge.DataTypeInt}
	fields := []qsbridge.QuantaProjectionField{
		{Index: "lineitem", Role: "l", Field: "l_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
		{Index: "lineitem", Role: "l", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
		{Index: "orders", Role: "o", Field: "o_orderdate", Type: qsbridge.DataTypeTime, Visible: true},
		{Index: "orders", Role: "o", Field: "o_shippriority", Type: qsbridge.DataTypeInt, Visible: true},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{ProjectionFields: fields})
	request.GroupBy = []qsbridge.Expr{
		qsbridge.Field(lOrderkey),
		qsbridge.Field(oOrderdate),
		qsbridge.Field(oShippriority),
	}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(lOrderkey), Type: qsbridge.DataTypeInt},
		{Expr: qsbridge.AggregateRef("revenue", 0), Alias: "revenue", Type: qsbridge.DataTypeFloat},
		{Expr: qsbridge.Field(oOrderdate), Type: qsbridge.DataTypeTime},
		{Expr: qsbridge.Field(oShippriority), Type: qsbridge.DataTypeInt},
	}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "sum", Input: qsbridge.Field(lExtendedprice), Alias: "revenue", Type: qsbridge.DataTypeFloat}}
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.AggregateRef("revenue", 0), Direction: qsbridge.SortDescending}}
	request.Result = qsbridge.ResultShape{Kind: qsbridge.ResultQuery, Limit: 10}
	var materializations []qsbridge.QuantaMaterializationRequest
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materializations = append(materializations, request)
			rowSet := qsbridge.QuantaProjectedRowSet{Index: request.Index, Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...)}
			for _, field := range request.ProjectionFields {
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "l_extendedprice":
						values := map[qsbridge.QuantaRownum]float64{101: 100, 102: 250, 103: 99, 104: 400, 105: 50}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: values[rownum]})
					case "o_orderdate":
						values := map[qsbridge.QuantaRownum]string{1: "1995-01-01", 2: "1995-01-02", 3: "1995-01-03"}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: values[rownum]})
					case "o_shippriority":
						values := map[qsbridge.QuantaRownum]int64{1: 0, 2: 1, 3: 2}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: values[rownum]})
					default:
						t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}
	alignedRows := map[string][]qsbridge.QuantaRownum{
		"l": []qsbridge.QuantaRownum{101, 102, 103, 104, 105},
		"o": []qsbridge.QuantaRownum{1, 1, 2, 3, 3},
		"c": []qsbridge.QuantaRownum{10, 10, 20, 30, 30},
	}
	result, handled, err := executor.legacyDirectRelationshipQ3OrderRevenueResult(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{101, 102, 103, 104, 105},
		[]legacyDirectRelationshipEdge{
			{childRole: "l", childTable: "lineitem", childField: "l_orderkey", parentRole: "o", parentTable: "orders", parentField: "o_orderkey"},
			{childRole: "o", childTable: "orders", childField: "o_custkey", parentRole: "c", parentTable: "customer", parentField: "c_custkey"},
		},
		fields,
		alignedRows,
		0,
		0,
		ExecutionResult{},
	)
	if err != nil {
		t.Fatalf("late q3 result: %v", err)
	}
	if !handled {
		t.Fatalf("handled = false, want Q3 pre-aggregate fast path")
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if len(materializations) != 2 {
		t.Fatalf("materializations = %d, want price plus final order fields", len(materializations))
	}
	if got := materializations[0].Rownums; len(got) != 5 || got[0] != 101 || got[4] != 105 {
		t.Fatalf("price rownums = %#v, want all lineitem rows", got)
	}
	if got := materializations[1].Rownums; len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("final order rownums = %#v, want reduced order rows", got)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 3 {
		t.Fatalf("rows = %#v, want three grouped orders", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != int64(3) || chunk.Rows[0][1].Value != float64(450) || chunk.Rows[0][2].Value != "1995-01-03" || chunk.Rows[0][3].Value != int64(2) {
		t.Fatalf("first row = %#v, want order 3/revenue 450/date/priority", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != int64(1) || chunk.Rows[1][1].Value != float64(350) {
		t.Fatalf("second row = %#v, want order 1/revenue 350", chunk.Rows[1])
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_late_materialization", "q3_order_revenue_projection")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_final_materialization_mode", "direct_q3_group_rows")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_preagg_rows", "5")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_preagg_groups", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "q3_attribution_scope", "graph_reduction_to_output")
	assertExecutionProbe(t, result.Probes, "relationship_join", "q3_attribution_input_line_rows", "5")
	assertExecutionProbe(t, result.Probes, "relationship_join", "q3_attribution_final_materialization_rows", "3")
	assertExecutionProbeName(t, result.Probes, "relationship_join", "q3_attribution_preagg_prune_elapsed")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "group_strategy", "relationship_preaggregate")
}

func TestLegacyDirectRelationshipQ3OrderRevenueUsesStorageAggregate(t *testing.T) {
	orders := qsbridge.TableInstance{Table: "orders", Alias: "o"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	lOrderkey := qsbridge.FieldRef{Table: lineitem, Name: "l_orderkey", Type: qsbridge.DataTypeInt}
	lExtendedprice := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	oOrderdate := qsbridge.FieldRef{Table: orders, Name: "o_orderdate", Type: qsbridge.DataTypeTime}
	oShippriority := qsbridge.FieldRef{Table: orders, Name: "o_shippriority", Type: qsbridge.DataTypeInt}
	fields := []qsbridge.QuantaProjectionField{
		{Index: "lineitem", Role: "l", Field: "l_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
		{Index: "lineitem", Role: "l", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
		{Index: "orders", Role: "o", Field: "o_orderdate", Type: qsbridge.DataTypeTime, Visible: true},
		{Index: "orders", Role: "o", Field: "o_shippriority", Type: qsbridge.DataTypeInt, Visible: true},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: fields,
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index: "lineitem",
			Role:  "l",
			Field: "l_shipdate",
			BSIOp: qsbridge.QuantaBSIOpRange,
			Begin: big.NewInt(820454400000),
			End:   big.NewInt(828316800000),
		}},
	})
	request.GroupBy = []qsbridge.Expr{
		qsbridge.Field(lOrderkey),
		qsbridge.Field(oOrderdate),
		qsbridge.Field(oShippriority),
	}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(lOrderkey), Type: qsbridge.DataTypeInt},
		{Expr: qsbridge.AggregateRef("revenue", 0), Alias: "revenue", Type: qsbridge.DataTypeFloat},
		{Expr: qsbridge.Field(oOrderdate), Type: qsbridge.DataTypeTime},
		{Expr: qsbridge.Field(oShippriority), Type: qsbridge.DataTypeInt},
	}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "sum", Input: qsbridge.Field(lExtendedprice), Alias: "revenue", Type: qsbridge.DataTypeFloat}}
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.AggregateRef("revenue", 0), Direction: qsbridge.SortDescending}}
	request.Result = qsbridge.ResultShape{Kind: qsbridge.ResultQuery, Limit: 10}

	aggregateReader := &fakeRelationshipVectorAggregateReader{
		Result: LegacyDirectRelationshipVectorAggregateResult{
			Mode:                       "reverse_artifact_sum",
			Rows:                       5,
			Values:                     3,
			SourceValues:               3,
			TargetRows:                 5,
			ProjectionShardsVisited:    4,
			ProjectionShardsInWindow:   3,
			ProjectionShardsRetained:   2,
			ProjectionRetainedRows:     5,
			ProjectionRetainBypassRows: 1,
			Groups: []LegacyDirectRelationshipVectorAggregateGroup{
				{ParentRow: 1, RepresentativeChildRow: 101, Count: 2, Sum: big.NewInt(350)},
				{ParentRow: 2, RepresentativeChildRow: 103, Count: 1, Sum: big.NewInt(99)},
				{ParentRow: 3, RepresentativeChildRow: 104, Count: 2, Sum: big.NewInt(450)},
			},
		},
		OK: true,
	}
	var materializations []qsbridge.QuantaMaterializationRequest
	cache := core.NewTableCacheStruct()
	cache.TableCache["lineitem"] = &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem", TimeQuantumField: "l_shipdate"},
		AttributeNameMap: map[string]*core.Attribute{
			"l_shipdate": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_shipdate", Type: "DateTime", MappingStrategy: "TimestampBSI", MapperConfig: map[string]string{"granularity": "millisecond"}}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		RelationshipAggregateReader: aggregateReader,
		TableCache:                  cache,
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materializations = append(materializations, request)
			for _, field := range request.ProjectionFields {
				if field.Field == "l_extendedprice" {
					t.Fatalf("storage aggregate path should not materialize %s.%s", field.Index, field.Field)
				}
			}
			rowSet := qsbridge.QuantaProjectedRowSet{Index: request.Index, Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...)}
			for _, field := range request.ProjectionFields {
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "o_orderdate":
						values := map[qsbridge.QuantaRownum]string{1: "1995-01-01", 2: "1995-01-02", 3: "1995-01-03"}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: values[rownum]})
					case "o_shippriority":
						values := map[qsbridge.QuantaRownum]int64{1: 0, 2: 1, 3: 2}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: values[rownum]})
					default:
						t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}
	alignedRows := map[string][]qsbridge.QuantaRownum{
		"l": []qsbridge.QuantaRownum{101, 102, 103, 104, 105},
		"o": []qsbridge.QuantaRownum{1, 1, 2, 3, 3},
		"c": []qsbridge.QuantaRownum{10, 10, 20, 30, 30},
	}
	result, handled, err := executor.legacyDirectRelationshipQ3OrderRevenueResult(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{101, 102, 103, 104, 105},
		[]legacyDirectRelationshipEdge{
			{childRole: "l", childTable: "lineitem", childField: "l_orderkey", parentRole: "o", parentTable: "orders", parentField: "o_orderkey"},
			{childRole: "o", childTable: "orders", childField: "o_custkey", parentRole: "c", parentTable: "customer", parentField: "c_custkey"},
		},
		fields,
		alignedRows,
		0,
		0,
		ExecutionResult{},
	)
	if err != nil {
		t.Fatalf("late q3 result: %v", err)
	}
	if !handled {
		t.Fatalf("handled = false, want Q3 storage aggregate fast path")
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if len(aggregateReader.Requests) != 1 {
		t.Fatalf("aggregate requests = %d, want one", len(aggregateReader.Requests))
	}
	if got := aggregateReader.Requests[0].ChildRows; len(got) != 5 || got[0] != 101 || got[4] != 105 {
		t.Fatalf("aggregate child rows = %#v, want lineitem rows", got)
	}
	if aggregateReader.Requests[0].FromEpochMillis != 820454400000 || aggregateReader.Requests[0].ToEpochMillis != 828316800000 {
		t.Fatalf("aggregate window = %d..%d, want lineitem shipdate window",
			aggregateReader.Requests[0].FromEpochMillis,
			aggregateReader.Requests[0].ToEpochMillis)
	}
	if len(materializations) != 1 {
		t.Fatalf("materializations = %d, want final order fields only", len(materializations))
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 3 {
		t.Fatalf("rows = %#v, want three grouped orders", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != int64(3) || chunk.Rows[0][1].Value != float64(450) {
		t.Fatalf("first row = %#v, want order 3/revenue 450", chunk.Rows[0])
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_preagg_mode", "storage_relationship_sum")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_final_materialization_mode", "direct_q3_group_rows")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_preagg_storage_mode", "reverse_artifact_sum")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_preagg_groups", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "q3_attribution_preagg_groups", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_preagg_storage_projection_shards_visited", "4")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_preagg_storage_projection_shards_in_window", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_preagg_storage_projection_shards_retained", "2")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_preagg_storage_projection_rows_retained", "5")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_preagg_storage_projection_retain_bypass_rows", "1")
	assertExecutionProbeName(t, result.Probes, "relationship_join", "q3_attribution_preagg_storage_elapsed")
	assertExecutionProbeName(t, result.Probes, "relationship_join", "phase_graph_grouped_aggregate_preagg_storage_projection_value_elapsed")
}

func TestLegacyDirectRelationshipQ3OrderRevenueFinalMaterializationPrunesUnorderedLimit(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Result = qsbridge.ResultShape{Kind: qsbridge.ResultQuery, Limit: 2}
	groups := []legacyDirectRelationshipQ3OrderRevenueGroup{
		{orderRow: 1, lineRow: 101, revenue: 100},
		{orderRow: 2, lineRow: 102, revenue: 200},
		{orderRow: 3, lineRow: 103, revenue: 300},
		{orderRow: 4, lineRow: 104, revenue: 400},
	}

	pruned, probe := legacyDirectRelationshipQ3OrderRevenueFinalMaterializationGroups(request, groups)

	if !probe.applied || probe.mode != "unordered_limit" {
		t.Fatalf("prune = %#v, want unordered limit applied", probe)
	}
	if len(pruned) != 2 || pruned[0].orderRow != 1 || pruned[1].orderRow != 2 {
		t.Fatalf("pruned groups = %#v, want first two groups", pruned)
	}
}

func TestLegacyDirectRelationshipQ3OrderRevenuePreAggregatePrunesUnorderedLimit(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Result = qsbridge.ResultShape{Kind: qsbridge.ResultQuery, Limit: 2}
	lineRows := []qsbridge.QuantaRownum{101, 102, 103, 104, 105, 106}
	orderRows := []qsbridge.QuantaRownum{3, 3, 1, 2, 2, 4}

	prunedLineRows, prunedOrderRows, prune := legacyDirectRelationshipQ3OrderRevenuePreAggregateRows(request, lineRows, orderRows)

	if !prune.applied || prune.mode != "unordered_limit" {
		t.Fatalf("prune = %#v, want unordered limit applied", prune)
	}
	if prune.rowsBefore != 6 || prune.rowsAfter != 3 || prune.groupsBefore != 4 || prune.groupsAfter != 2 {
		t.Fatalf("prune = %#v, want rows 6->3 and groups 4->2", prune)
	}
	if got, want := prunedLineRows, []qsbridge.QuantaRownum{101, 102, 103}; !reflect.DeepEqual(got, want) {
		t.Fatalf("line rows = %#v, want %#v", got, want)
	}
	if got, want := prunedOrderRows, []qsbridge.QuantaRownum{3, 3, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("order rows = %#v, want %#v", got, want)
	}
}

func TestLegacyDirectRelationshipQ3OrderRevenuePreAggregateDoesNotPruneOrderedLimit(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.AggregateRef("revenue", 0), Direction: qsbridge.SortDescending}}
	request.Result = qsbridge.ResultShape{Kind: qsbridge.ResultQuery, Limit: 2}
	lineRows := []qsbridge.QuantaRownum{101, 102, 103, 104, 105, 106}
	orderRows := []qsbridge.QuantaRownum{3, 3, 1, 2, 2, 4}

	prunedLineRows, prunedOrderRows, prune := legacyDirectRelationshipQ3OrderRevenuePreAggregateRows(request, lineRows, orderRows)

	if prune.applied || prune.mode != "none" {
		t.Fatalf("prune = %#v, want no pre-aggregate prune", prune)
	}
	if !reflect.DeepEqual(prunedLineRows, lineRows) || !reflect.DeepEqual(prunedOrderRows, orderRows) {
		t.Fatalf("rows = %#v/%#v, want original %#v/%#v", prunedLineRows, prunedOrderRows, lineRows, orderRows)
	}
}

func TestLegacyDirectRelationshipQ3OrderRevenueFinalMaterializationKeepsRevenueCutoffTies(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.AggregateRef("revenue", 0), Direction: qsbridge.SortDescending}}
	request.Result = qsbridge.ResultShape{Kind: qsbridge.ResultQuery, Limit: 2}
	groups := []legacyDirectRelationshipQ3OrderRevenueGroup{
		{orderRow: 1, lineRow: 101, revenue: 100},
		{orderRow: 2, lineRow: 102, revenue: 300},
		{orderRow: 3, lineRow: 103, revenue: 300},
		{orderRow: 4, lineRow: 104, revenue: 500},
	}

	pruned, probe := legacyDirectRelationshipQ3OrderRevenueFinalMaterializationGroups(request, groups)

	if !probe.applied || probe.mode != "revenue_cutoff" {
		t.Fatalf("prune = %#v, want revenue cutoff applied", probe)
	}
	if len(pruned) != 3 {
		t.Fatalf("pruned groups = %#v, want top group plus cutoff tie", pruned)
	}
	got := map[qsbridge.QuantaRownum]bool{}
	for _, group := range pruned {
		got[group.orderRow] = true
	}
	for _, want := range []qsbridge.QuantaRownum{2, 3, 4} {
		if !got[want] {
			t.Fatalf("pruned groups = %#v, missing order row %d", pruned, want)
		}
	}
}

func TestLegacyDirectRelationshipSiblingRootMaterializedValuesHydratesTupleRoles(t *testing.T) {
	tupleRows := RelationshipTupleRowSet{Rows: []RelationshipTupleRow{
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l": 12, "ps": 101}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l": 11, "ps": 101}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l": 12, "ps": 101}},
	}}
	fields := []qsbridge.QuantaProjectionField{
		{Index: "lineitem", Role: "l", Field: "l_quantity", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Role: "ps", Field: "ps_supplycost", Type: qsbridge.DataTypeFloat},
	}
	seen := make(map[string][]qsbridge.QuantaRownum)
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			seen[request.Index] = append([]qsbridge.QuantaRownum(nil), request.Rownums...)
			rowSet := qsbridge.QuantaProjectedRowSet{Index: request.Index, Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...)}
			for _, field := range request.ProjectionFields {
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "l_quantity":
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum * 10)})
					case "ps_supplycost":
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: float64(rownum) / 10})
					default:
						t.Fatalf("unexpected field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}
	values, _, diagnostics, err := executor.legacyDirectRelationshipSiblingRootMaterializedValues(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}), tupleRows, fields)
	if err != nil {
		t.Fatalf("materialize tuple values: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if got := seen["lineitem"]; len(got) != 2 || got[0] != 11 || got[1] != 12 {
		t.Fatalf("lineitem rownums = %#v, want sorted unique [11 12]", got)
	}
	if got := seen["partsupp"]; len(got) != 1 || got[0] != 101 {
		t.Fatalf("partsupp rownums = %#v, want sorted unique [101]", got)
	}
	quantityKey := RelationshipTupleValueKeyForField(fields[0])
	if got := values[quantityKey][12].Value; got != int64(120) {
		t.Fatalf("quantity value = %#v, want 120", got)
	}
	supplyCostKey := RelationshipTupleValueKeyForField(fields[1])
	if got := values[supplyCostKey][101].Value; got != float64(10.1) {
		t.Fatalf("supply cost value = %#v, want 10.1", got)
	}
}

func TestLegacyDirectRelationshipChildRowsByParentFeedsQ9TupleExpansion(t *testing.T) {
	shape, ok, diagnostics := legacyDirectRelationshipSiblingRootGraphShape([]legacyDirectRelationshipEdge{
		{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
		{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
	})
	if diagnostics.BlocksNative() || !ok {
		t.Fatalf("shape diagnostics = %#v ok=%t, want sibling-root shape", diagnostics, ok)
	}
	rootRows := []qsbridge.QuantaRownum{1, 2}
	lineitemRows := []qsbridge.QuantaRownum{11, 12, 21}
	partsuppRows := []qsbridge.QuantaRownum{101, 102, 201}
	rowSet, diagnostics := legacyDirectRelationshipSiblingRootTupleRows(
		shape,
		rootRows,
		[]legacyDirectRelationshipSiblingRootExpansion{
			{
				edge: legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
				childRowsByParent: legacyDirectRelationshipChildRowsByParent(rootRows, lineitemRows, []legacyDirectRelationshipPair{
					{child: 11, parent: 1},
					{child: 12, parent: 1},
					{child: 21, parent: 2},
				}),
			},
			{
				edge: legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
				childRowsByParent: legacyDirectRelationshipChildRowsByParent(rootRows, partsuppRows, []legacyDirectRelationshipPair{
					{child: 101, parent: 1},
					{child: 102, parent: 1},
					{child: 201, parent: 2},
				}),
			},
		},
	)
	if diagnostics.BlocksNative() {
		t.Fatalf("tuple diagnostics = %#v, want none", diagnostics)
	}
	assertRelationshipTupleRows(t, rowSet, []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{
		{"p": 1, "l": 11, "ps": 101},
		{"p": 1, "l": 11, "ps": 102},
		{"p": 1, "l": 12, "ps": 101},
		{"p": 1, "l": 12, "ps": 102},
		{"p": 2, "l": 21, "ps": 201},
	})
}

func TestLegacyDirectRelationshipSiblingRootTupleRowsBuildsQ9Expansion(t *testing.T) {
	shape, ok, diagnostics := legacyDirectRelationshipSiblingRootGraphShape([]legacyDirectRelationshipEdge{
		{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
		{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
	})
	if diagnostics.BlocksNative() || !ok {
		t.Fatalf("shape diagnostics = %#v ok=%t, want sibling-root shape", diagnostics, ok)
	}
	rowSet, diagnostics := legacyDirectRelationshipSiblingRootTupleRows(
		shape,
		[]qsbridge.QuantaRownum{1, 2},
		[]legacyDirectRelationshipSiblingRootExpansion{
			{
				edge: legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem"},
				childRowsByParent: map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
					1: {11, 12},
					2: {21},
				},
			},
			{
				edge: legacyDirectRelationshipEdge{parentRole: "p", parentTable: "part", childRole: "ps", childTable: "partsupp"},
				childRowsByParent: map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
					1: {101, 102},
					2: {201},
				},
			},
		},
	)
	if diagnostics.BlocksNative() {
		t.Fatalf("tuple diagnostics = %#v, want none", diagnostics)
	}
	assertRelationshipTupleRows(t, rowSet, []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{
		{"p": 1, "l": 11, "ps": 101},
		{"p": 1, "l": 11, "ps": 102},
		{"p": 1, "l": 12, "ps": 101},
		{"p": 1, "l": 12, "ps": 102},
		{"p": 2, "l": 21, "ps": 201},
	})
}

func TestLegacyDirectRelationshipSiblingRootTupleRowsRejectsWrongParent(t *testing.T) {
	shape := legacyDirectRelationshipSiblingRootGraph{rootRole: "p", rootTable: "part"}
	_, diagnostics := legacyDirectRelationshipSiblingRootTupleRows(
		shape,
		[]qsbridge.QuantaRownum{1},
		[]legacyDirectRelationshipSiblingRootExpansion{{
			edge: legacyDirectRelationshipEdge{parentRole: "supplier", parentTable: "supplier", childRole: "ps", childTable: "partsupp"},
			childRowsByParent: map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
				1: {101},
			},
		}},
	)
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want wrong-parent blocker", diagnostics)
	}
}

func TestLegacyDirectRelationshipGraphDispatchReportsSiblingRootTupleBlocker(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	cache := core.NewTableCacheStruct()
	cache.TableCache["lineitem"] = &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem"},
		AttributeNameMap: map[string]*core.Attribute{
			"l_partkey": {BasicAttribute: &shared.BasicAttribute{FieldName: "l_partkey", SourceName: "l_partkey", Type: "Integer", MappingStrategy: "ParentRelation", ForeignKey: "part.p_partkey"}},
		},
	}
	cache.TableCache["partsupp"] = &core.Table{
		BasicTable: &shared.BasicTable{Name: "partsupp"},
		AttributeNameMap: map[string]*core.Attribute{
			"ps_partkey": {BasicAttribute: &shared.BasicAttribute{FieldName: "ps_partkey", SourceName: "ps_partkey", Type: "Integer", MappingStrategy: "ParentRelation", ForeignKey: "part.p_partkey"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{TableCache: cache}
	result, err := executor.executeLegacyDirectRelationshipVectorJoinGraph(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}),
		RelationshipVectorJoinRequest{Edges: []qsbridge.RelationshipJoinPlanEdge{
			{
				Left:          qsbridge.FieldRef{Table: lineitem, Name: "l_partkey", Type: qsbridge.DataTypeInt},
				LeftRole:      "l",
				Right:         qsbridge.FieldRef{Table: part, Name: "p_partkey", Type: qsbridge.DataTypeInt},
				RightRole:     "p",
				SQLKind:       qsbridge.JoinKindInner,
				ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
			},
			{
				Left:          qsbridge.FieldRef{Table: partsupp, Name: "ps_partkey", Type: qsbridge.DataTypeInt},
				LeftRole:      "ps",
				Right:         qsbridge.FieldRef{Table: part, Name: "p_partkey", Type: qsbridge.DataTypeInt},
				RightRole:     "p",
				SQLKind:       qsbridge.JoinKindInner,
				ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
			},
		}},
	)
	if err != nil {
		t.Fatalf("graph execution: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want sibling-root tuple blocker", result.Diagnostics)
	}
	if got := result.Diagnostics[0].Message; got != "relationship-vector sibling-root tuple execution is not wired in this slice: p:part->l,ps" {
		t.Fatalf("diagnostic = %q, want sibling-root tuple blocker", got)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_shape", "sibling_root")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_sibling_root", "p:part->l,ps")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_sibling_children", "lineitem,partsupp")
}

func TestLegacyDirectRelationshipPruneRedundantParentEdgesRemovesUnusedUnfilteredParent(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	name := qsbridge.FieldRef{Table: part, Name: "p_name", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{{
		Expr: qsbridge.Binary(
			qsbridge.BinaryOpLike,
			qsbridge.Field(name),
			qsbridge.Literal(qsbridge.ValueString, "%green%"),
		),
		Scope: qsbridge.PredicateScopeWhere,
	}}
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "o", parentTable: "orders", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner},
	}

	pruned, probes := legacyDirectRelationshipPruneRedundantParentEdges(request, edges)

	if len(pruned) != 1 || pruned[0].parentRole != "p" {
		t.Fatalf("pruned edges = %#v, want only part parent edge", pruned)
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_pruned_edges", "1")
	assertExecutionProbe(t, probes, "relationship_join", "graph_prune_applied", "true")
}

func TestLegacyDirectRelationshipPruneRedundantParentEdgesKeepsCountStarTupleGraph(t *testing.T) {
	region := qsbridge.TableInstance{Table: "region", Alias: "r"}
	name := qsbridge.FieldRef{Table: region, Name: "r_name", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "q5_graph_count", Type: qsbridge.DataTypeInt}}
	request.Predicates = []qsbridge.Predicate{{
		Expr: qsbridge.Binary(
			qsbridge.BinaryOpEqual,
			qsbridge.Field(name),
			qsbridge.Literal(qsbridge.ValueString, "ASIA"),
		),
		Scope: qsbridge.PredicateScopeWhere,
	}}
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "r", parentTable: "region", childRole: "n", childTable: "nation", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "n", parentTable: "nation", childRole: "c", childTable: "customer", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "c", parentTable: "customer", childRole: "o", childTable: "orders", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "o", parentTable: "orders", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "s", parentTable: "supplier", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner},
	}

	pruned, probes := legacyDirectRelationshipPruneRedundantParentEdges(request, edges)

	if len(pruned) != len(edges) {
		t.Fatalf("pruned edges = %d, want all %d retained for count(*) tuple multiplicity", len(pruned), len(edges))
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_pruned_edges", "0")
	assertExecutionProbe(t, probes, "relationship_join", "graph_prune_applied", "false")
	assertExecutionProbe(t, probes, "relationship_join", "graph_prune_reason", "result_requires_join_tuple_multiplicity")
}

func TestLegacyDirectRelationshipPruneRedundantParentEdgesRetainsSinkPathForFKBackedCountStarTupleGraph(t *testing.T) {
	region := qsbridge.TableInstance{Table: "region", Alias: "r"}
	name := qsbridge.FieldRef{Table: region, Name: "r_name", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "q5_graph_count", Type: qsbridge.DataTypeInt}}
	request.Predicates = []qsbridge.Predicate{{
		Expr: qsbridge.Binary(
			qsbridge.BinaryOpEqual,
			qsbridge.Field(name),
			qsbridge.Literal(qsbridge.ValueString, "ASIA"),
		),
		Scope: qsbridge.PredicateScopeWhere,
	}}
	capabilities := qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityJoinReduction}
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "c", parentTable: "customer", childRole: "o", childTable: "orders", sqlKind: qsbridge.JoinKindInner, capabilities: capabilities},
		{parentRole: "o", parentTable: "orders", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner, capabilities: capabilities},
		{parentRole: "s", parentTable: "supplier", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner, capabilities: capabilities},
		{parentRole: "n", parentTable: "nation", childRole: "s", childTable: "supplier", sqlKind: qsbridge.JoinKindInner, capabilities: capabilities},
		{parentRole: "r", parentTable: "region", childRole: "n", childTable: "nation", sqlKind: qsbridge.JoinKindInner, capabilities: capabilities},
	}

	pruned, probes := legacyDirectRelationshipPruneRedundantParentEdges(request, edges)

	if len(pruned) != 3 {
		t.Fatalf("pruned edges = %d, want supplier sink path of 3 retained: %#v", len(pruned), pruned)
	}
	for i, want := range []string{"s", "n", "r"} {
		if pruned[i].parentRole != want {
			t.Fatalf("pruned[%d].parentRole = %q, want %q in retained sink path: %#v", i, pruned[i].parentRole, want, pruned)
		}
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_pruned_edges", "2")
	assertExecutionProbe(t, probes, "relationship_join", "graph_prune_applied", "true")
	assertExecutionProbe(t, probes, "relationship_join", "graph_required_roles", "l,r,region")
}

func TestLegacyDirectRelationshipPruneRedundantParentEdgesKeepsGroupedCountStarTupleGraph(t *testing.T) {
	customer := qsbridge.TableInstance{Table: "customers_qa", Alias: "c"}
	city := qsbridge.FieldRef{Table: customer, Name: "city", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(city)}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "line_count", Type: qsbridge.DataTypeInt}}
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "c", parentTable: "customers_qa", childRole: "o", childTable: "orders_qa", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "o", parentTable: "orders_qa", childRole: "l", childTable: "lineitems_qa", sqlKind: qsbridge.JoinKindInner},
	}

	pruned, probes := legacyDirectRelationshipPruneRedundantParentEdges(request, edges)

	if len(pruned) != len(edges) {
		t.Fatalf("pruned edges = %d, want all %d retained for grouped count(*) tuple multiplicity", len(pruned), len(edges))
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_pruned_edges", "0")
	assertExecutionProbe(t, probes, "relationship_join", "graph_prune_applied", "false")
	assertExecutionProbe(t, probes, "relationship_join", "graph_prune_reason", "result_requires_join_tuple_multiplicity")
}

func TestLegacyDirectRelationshipPruneRedundantParentEdgesKeepsRequiredRoleBridgePath(t *testing.T) {
	region := qsbridge.TableInstance{Table: "region", Alias: "r"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	regionName := qsbridge.FieldRef{Table: region, Name: "r_name", Type: qsbridge.DataTypeString}
	extendedPrice := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.ProjectionOrder = []qsbridge.FieldRef{extendedPrice}
	request.Predicates = []qsbridge.Predicate{{
		Expr: qsbridge.Binary(
			qsbridge.BinaryOpEqual,
			qsbridge.Field(regionName),
			qsbridge.Literal(qsbridge.ValueString, "ASIA"),
		),
		Scope: qsbridge.PredicateScopeWhere,
	}}
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "r", parentTable: "region", childRole: "n", childTable: "nation", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "n", parentTable: "nation", childRole: "c", childTable: "customer", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "c", parentTable: "customer", childRole: "o", childTable: "orders", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "o", parentTable: "orders", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "s", parentTable: "supplier", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner},
	}

	pruned, probes := legacyDirectRelationshipPruneRedundantParentEdges(request, edges)

	if len(pruned) != 4 {
		t.Fatalf("pruned edges = %d, want required bridge path of 4 edges retained: %#v", len(pruned), pruned)
	}
	if pruned[0].parentRole != "r" || pruned[1].parentRole != "n" || pruned[2].parentRole != "c" || pruned[3].parentRole != "o" {
		t.Fatalf("pruned path = %#v, want r->n->c->o->l bridge", pruned)
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_pruned_edges", "1")
	assertExecutionProbe(t, probes, "relationship_join", "graph_required_roles", "l,lineitem,r,region")
}

func TestLegacyDirectRelationshipPruneRedundantParentEdgesIgnoresJoinOnlyMaterialization(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	name := qsbridge.FieldRef{Table: part, Name: "p_name", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{{
		Expr: qsbridge.Binary(
			qsbridge.BinaryOpLike,
			qsbridge.Field(name),
			qsbridge.Literal(qsbridge.ValueString, "%green%"),
		),
		Scope: qsbridge.PredicateScopeWhere,
	}}
	request.Materialization = qsbridge.QuantaMaterializationRequest{ProjectionFields: []qsbridge.QuantaProjectionField{
		{Index: "orders", Role: "o", Field: "o_orderkey", Roles: qsbridge.FieldRoleJoinInput},
		{Index: "part", Role: "p", Field: "p_name", Roles: qsbridge.FieldRoleResidualInput},
	}}
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "o", parentTable: "orders", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner},
	}

	pruned, probes := legacyDirectRelationshipPruneRedundantParentEdges(request, edges)

	if len(pruned) != 1 || pruned[0].parentRole != "p" {
		t.Fatalf("pruned edges = %#v, want only part parent edge", pruned)
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_required_roles", "p,part")
}

func TestLegacyDirectRelationshipPruneRedundantParentEdgesKeepsReferencedParent(t *testing.T) {
	orders := qsbridge.TableInstance{Table: "orders", Alias: "o"}
	orderDate := qsbridge.FieldRef{Table: orders, Name: "o_orderdate", Type: qsbridge.DataTypeTime}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.GroupBy = []qsbridge.Expr{qsbridge.Call("year", qsbridge.Field(orderDate))}
	edges := []legacyDirectRelationshipEdge{
		{parentRole: "p", parentTable: "part", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner},
		{parentRole: "o", parentTable: "orders", childRole: "l", childTable: "lineitem", sqlKind: qsbridge.JoinKindInner},
	}

	pruned, probes := legacyDirectRelationshipPruneRedundantParentEdges(request, edges)

	if len(pruned) != 1 || pruned[0].parentRole != "o" {
		t.Fatalf("pruned edges = %#v, want referenced orders parent edge", pruned)
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_pruned_edges", "1")
}

func TestLegacyDirectRelationshipGraphSinkTableRejectsSiblingSinks(t *testing.T) {
	_, diagnostics := legacyDirectRelationshipGraphSinkTable([]legacyDirectRelationshipEdge{
		{parentTable: "part", childTable: "lineitem"},
		{parentTable: "part", childTable: "partsupp"},
	})
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want sibling sink blocker", diagnostics)
	}
	if got := diagnostics[0].Message; got != "relationship-vector graph execution requires a single sink table" {
		t.Fatalf("diagnostic = %q, want single sink blocker", got)
	}
}

func TestLegacyDirectRelationshipIntersectRownumsPreservesLeftOrder(t *testing.T) {
	got := legacyDirectRelationshipIntersectRownums(
		[]qsbridge.QuantaRownum{5, 2, 9, 7},
		[]qsbridge.QuantaRownum{7, 5, 11},
	)
	want := []qsbridge.QuantaRownum{5, 7}
	if len(got) != len(want) {
		t.Fatalf("intersection = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("intersection = %#v, want %#v", got, want)
		}
	}
}

func TestLegacyDirectRelationshipLeftOuterAggregateResultCountsUnmatchedParents(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "q13_left_join_count", Type: qsbridge.DataTypeInt}}
	edge := legacyDirectRelationshipEdge{sqlKind: qsbridge.JoinKindLeftOuter, leftOuterPreservesParent: true}
	executor := LegacyDirectRelationshipVectorJoinExecutor{}

	result, err := executor.legacyDirectRelationshipLeftOuterAggregateResult(
		context.Background(),
		request,
		edge,
		[]qsbridge.QuantaRownum{1, 2, 3},
		[]legacyDirectRelationshipPair{
			{parent: 1, child: 10},
			{parent: 1, child: 11},
			{parent: 2, child: 20},
		},
		ExecutionResult{},
	)
	if err != nil {
		t.Fatalf("left outer aggregate: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 || chunk.Rows[0][0].Value != int64(4) {
		t.Fatalf("rows = %#v, want count 4", chunk.Rows)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "left_outer_unmatched_parents", "1")
}

func TestLegacyDirectRelationshipGraphProjectionMaterializationFieldsIncludesResidualOnlyFields(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	partKey := qsbridge.FieldRef{Table: part, Name: "p_partkey", Type: qsbridge.DataTypeInt}
	supplyCost := qsbridge.FieldRef{Table: partsupp, Name: "ps_supplycost", Type: qsbridge.DataTypeFloat}
	partType := qsbridge.FieldRef{Table: part, Name: "p_type", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "part", Field: "p_partkey", Type: qsbridge.DataTypeInt, Visible: true},
			{Index: "partsupp", Field: "ps_supplycost", Type: qsbridge.DataTypeFloat, Visible: true},
			{Index: "part", Field: "p_type", Type: qsbridge.DataTypeString},
		},
	})
	request.ProjectionOrder = []qsbridge.FieldRef{partKey, supplyCost}
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpLike, qsbridge.Field(partType), qsbridge.Literal(qsbridge.ValueString, "%BRASS")),
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}

	fields := legacyDirectRelationshipGraphProjectionMaterializationFields(request, []qsbridge.QuantaProjectionField{
		{Index: "part", Field: "p_partkey", Type: qsbridge.DataTypeInt, Visible: true},
		{Index: "partsupp", Field: "ps_supplycost", Type: qsbridge.DataTypeFloat, Visible: true},
	})
	got := make(map[string]bool)
	for _, field := range fields {
		got[field.Index+"."+field.Field] = true
	}
	for _, want := range []string{"part.p_partkey", "partsupp.ps_supplycost", "part.p_type"} {
		if !got[want] {
			t.Fatalf("materialization fields = %#v, missing %s", fields, want)
		}
	}
	if len(fields) != 3 {
		t.Fatalf("materialization fields = %#v, want exactly 3", fields)
	}
}

func TestLegacyDirectRelationshipGraphProjectionResultMaterializesSinkFields(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	extendedPrice := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.ProjectionOrder = []qsbridge.FieldRef{extendedPrice}
	request.Result = qsbridge.ResultShape{Limit: 2}
	var materialization qsbridge.QuantaMaterializationRequest
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			materialization = request
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field: request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueFloat, Value: float64(10)},
						{Kind: qsbridge.ValueFloat, Value: float64(20)},
					},
				}},
			}, nil, nil
		}),
	}

	result, err := executor.legacyDirectRelationshipGraphProjectionResult(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{9, 8, 7},
		nil,
		legacyDirectRelationshipGraphScratchpad{},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("graph projection: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if materialization.Index != "lineitem" {
		t.Fatalf("materialization index = %q, want lineitem", materialization.Index)
	}
	if len(materialization.Rownums) != 2 || materialization.Rownums[0] != 9 || materialization.Rownums[1] != 8 {
		t.Fatalf("materialization rownums = %#v, want pushed limit rownums [9 8]", materialization.Rownums)
	}
	if result.Count != 2 {
		t.Fatalf("count = %d, want 2", result.Count)
	}
}

func TestLegacyDirectRelationshipGraphProjectionResultReportsUnalignedAncestorFields(t *testing.T) {
	nation := qsbridge.TableInstance{Table: "nation", Alias: "n"}
	name := qsbridge.FieldRef{Table: nation, Name: "n_name", Type: qsbridge.DataTypeString}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.ProjectionOrder = []qsbridge.FieldRef{name}
	executor := LegacyDirectRelationshipVectorJoinExecutor{}

	result, err := executor.legacyDirectRelationshipGraphProjectionResult(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{9},
		nil,
		legacyDirectRelationshipGraphScratchpad{},
		ExecutionResult{Count: 1},
	)
	if err != nil {
		t.Fatalf("graph projection: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocking unaligned ancestor-field diagnostic", result.Diagnostics)
	}
}

func TestLegacyDirectRelationshipGraphAggregateResultMaterializesSinkInputs(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	price := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	discount := qsbridge.FieldRef{Table: lineitem, Name: "l_discount", Type: qsbridge.DataTypeFloat}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "lineitem", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
			{Index: "lineitem", Field: "l_discount", Type: qsbridge.DataTypeFloat},
		},
	})
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "sum",
		Input: qsbridge.Binary(
			qsbridge.BinaryOpMultiply,
			qsbridge.Field(price),
			qsbridge.Binary(qsbridge.BinaryOpSubtract, qsbridge.Literal(qsbridge.ValueInt, int64(1)), qsbridge.Field(discount)),
		),
		Alias: "total_revenue",
		Type:  qsbridge.DataTypeFloat,
	}}
	materializedFields := make(map[string]struct{})
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			for _, field := range request.ProjectionFields {
				materializedFields[request.Index+"."+field.Field] = struct{}{}
			}
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				vector := qsbridge.QuantaProjectionVector{Field: field}
				switch field.Field {
				case "l_extendedprice":
					vector.Values = []qsbridge.ResultCell{
						{Kind: qsbridge.ValueFloat, Value: float64(100)},
						{Kind: qsbridge.ValueFloat, Value: float64(50)},
					}
				case "l_discount":
					vector.Values = []qsbridge.ResultCell{
						{Kind: qsbridge.ValueFloat, Value: float64(0.10)},
						{Kind: qsbridge.ValueFloat, Value: float64(0.20)},
					}
				default:
					t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}

	result, err := executor.legacyDirectRelationshipGraphAggregateResult(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{11, 12},
		nil,
		legacyDirectRelationshipGraphScratchpad{},
		ExecutionResult{Count: 2},
	)
	if err != nil {
		t.Fatalf("graph aggregate: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if _, ok := materializedFields["lineitem.l_extendedprice"]; !ok {
		t.Fatalf("materialized fields = %#v, want lineitem.l_extendedprice", materializedFields)
	}
	if _, ok := materializedFields["lineitem.l_discount"]; !ok {
		t.Fatalf("materialized fields = %#v, want lineitem.l_discount", materializedFields)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 {
		t.Fatalf("rows = %#v, want one aggregate cell", chunk.Rows)
	}
	if got := chunk.Rows[0][0].Value; got != float64(130) {
		t.Fatalf("sum = %#v, want 130", got)
	}
}

func TestLegacyDirectRelationshipGraphAggregateResultRejectsAncestorInputs(t *testing.T) {
	nation := qsbridge.TableInstance{Table: "nation", Alias: "n"}
	name := qsbridge.FieldRef{Table: nation, Name: "n_nationkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "nation", Field: "n_nationkey", Type: qsbridge.DataTypeInt},
		},
	})
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "sum",
		Input:    qsbridge.Field(name),
		Alias:    "nation_sum",
		Type:     qsbridge.DataTypeFloat,
	}}
	executor := LegacyDirectRelationshipVectorJoinExecutor{}

	result, err := executor.legacyDirectRelationshipGraphAggregateResult(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{11},
		nil,
		legacyDirectRelationshipGraphScratchpad{},
		ExecutionResult{Count: 1},
	)
	if err != nil {
		t.Fatalf("graph aggregate: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocking ancestor-input diagnostic", result.Diagnostics)
	}
}

func TestLegacyDirectRelationshipGraphMaterializedRowSetAlignsAncestorGroupField(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "n_name":
						value := "JAPAN"
						if rownum == 2 {
							value = "CHINA"
						}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: value})
					case "l_extendedprice":
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: float64(rownum * 10)})
					default:
						t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}
	rowSet, probes, diagnostics, err := executor.legacyDirectRelationshipGraphMaterializedRowSet(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{11, 12, 13},
		[]qsbridge.QuantaProjectionField{
			{Index: "nation", Field: "n_name", Type: qsbridge.DataTypeString, Visible: true},
			{Index: "lineitem", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat, Visible: true},
		},
		map[string][]qsbridge.QuantaRownum{
			"lineitem": []qsbridge.QuantaRownum{11, 12, 13},
			"nation":   []qsbridge.QuantaRownum{1, 2, 1},
		},
		nil,
		"test_graph_materialization_",
	)
	if err != nil {
		t.Fatalf("materialize graph row set: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	chunk, diagnostics := rowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if got := chunk.Rows[0][0].Value; got != "JAPAN" {
		t.Fatalf("first group value = %#v, want JAPAN", got)
	}
	if got := chunk.Rows[1][0].Value; got != "CHINA" {
		t.Fatalf("second group value = %#v, want CHINA", got)
	}
	if got := chunk.Rows[2][1].Value; got != float64(130) {
		t.Fatalf("third price value = %#v, want 130", got)
	}
	assertExecutionProbe(t, probes, "relationship_join", "test_graph_materialization_field_1_nation_nation_n_name_rows", "3")
	assertExecutionProbeName(t, probes, "relationship_join", "test_graph_materialization_field_1_nation_nation_n_name_fetch_elapsed")
	assertExecutionProbeName(t, probes, "relationship_join", "test_graph_materialization_field_1_nation_nation_n_name_attach_elapsed")
}

func TestLegacyDirectRelationshipGraphMaterializedRowSetBatchesFieldsByRole(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	calls := 0
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			calls++
			if request.Index != "orders" {
				t.Fatalf("materialization index = %s, want orders", request.Index)
			}
			if len(request.ProjectionFields) != 2 {
				t.Fatalf("projection fields = %#v, want two batched fields", request.ProjectionFields)
			}
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "o_orderdate":
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueTime, Value: int64(rownum + 1000)})
					case "o_shippriority":
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum % 3)})
					default:
						t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}
	rowSet, probes, diagnostics, err := executor.legacyDirectRelationshipGraphMaterializedRowSet(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{11, 12},
		[]qsbridge.QuantaProjectionField{
			{Index: "orders", Role: "o", Field: "o_orderdate", Type: qsbridge.DataTypeTime, Visible: true},
			{Index: "orders", Role: "o", Field: "o_shippriority", Type: qsbridge.DataTypeInt, Visible: true},
		},
		map[string][]qsbridge.QuantaRownum{
			"lineitem": []qsbridge.QuantaRownum{11, 12},
			"orders":   []qsbridge.QuantaRownum{101, 102},
			"o":        []qsbridge.QuantaRownum{101, 102},
		},
		nil,
		"test_graph_materialization_",
	)
	if err != nil {
		t.Fatalf("materialize graph row set: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if calls != 1 {
		t.Fatalf("materialization calls = %d, want one batched role call", calls)
	}
	if len(rowSet.ProjectionVectors) != 2 {
		t.Fatalf("projection vectors = %#v, want two vectors", rowSet.ProjectionVectors)
	}
	if got := rowSet.ProjectionVectors[0].Field.Field; got != "o_orderdate" {
		t.Fatalf("first vector field = %s, want o_orderdate", got)
	}
	if got := rowSet.ProjectionVectors[1].Field.Field; got != "o_shippriority" {
		t.Fatalf("second vector field = %s, want o_shippriority", got)
	}
	assertExecutionProbeName(t, probes, "relationship_join", "test_graph_materialization_field_1_o_orders_o_orderdate_fetch_elapsed")
	assertExecutionProbeName(t, probes, "relationship_join", "test_graph_materialization_field_2_o_orders_o_shippriority_fetch_elapsed")
}

func TestLegacyDirectRelationshipGraphMaterializedRowSetSynthesizesRelationshipEndpoints(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				if field.Field != "l_quantity" {
					t.Fatalf("materializer was asked for synthetic endpoint field %s.%s", request.Index, field.Field)
				}
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(rownum * 10)})
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}
	rowSet, probes, diagnostics, err := executor.legacyDirectRelationshipGraphMaterializedRowSet(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{11, 12},
		[]qsbridge.QuantaProjectionField{
			{Index: "lineitem", Role: "l", Field: "l_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
			{Index: "orders", Role: "o", Field: "o_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
			{Index: "lineitem", Role: "l", Field: "l_quantity", Type: qsbridge.DataTypeInt, Visible: true},
		},
		map[string][]qsbridge.QuantaRownum{
			"lineitem": []qsbridge.QuantaRownum{11, 12},
			"l":        []qsbridge.QuantaRownum{11, 12},
			"orders":   []qsbridge.QuantaRownum{9001, 9002},
			"o":        []qsbridge.QuantaRownum{9001, 9002},
		},
		[]legacyDirectRelationshipEdge{{
			childRole:   "l",
			childTable:  "lineitem",
			childField:  "l_orderkey",
			parentRole:  "o",
			parentTable: "orders",
			parentField: "o_orderkey",
		}},
		"test_graph_materialization_",
	)
	if err != nil {
		t.Fatalf("materialize graph row set: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	chunk, diagnostics := rowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if got := chunk.Rows[0][0].Value; got != int64(9001) {
		t.Fatalf("synthetic child FK = %#v, want 9001", got)
	}
	if got := chunk.Rows[1][1].Value; got != int64(9002) {
		t.Fatalf("synthetic parent PK = %#v, want 9002", got)
	}
	assertExecutionProbe(t, probes, "relationship_join", "test_graph_materialization_field_1_l_lineitem_l_orderkey_source", "synthetic_relationship_endpoint")
	assertExecutionProbe(t, probes, "relationship_join", "test_graph_materialization_field_1_l_lineitem_l_orderkey_source_role", "o")
	assertExecutionProbe(t, probes, "relationship_join", "test_graph_materialization_field_2_o_orders_o_orderkey_source", "synthetic_relationship_endpoint")
}

func TestLegacyDirectRelationshipGraphGroupedAggregateProbesResidualFiltering(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	group := qsbridge.FieldRef{Table: lineitem, Name: "l_returnflag", Type: qsbridge.DataTypeString}
	price := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	flag := qsbridge.FieldRef{Table: lineitem, Name: "l_linenumber", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "lineitem", Field: "l_returnflag", Type: qsbridge.DataTypeString},
			{Index: "lineitem", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
			{Index: "lineitem", Field: "l_linenumber", Type: qsbridge.DataTypeInt},
		},
	})
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(group)}
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(flag), qsbridge.Literal(qsbridge.ValueInt, int64(1))),
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "sum",
		Input:    qsbridge.Field(price),
		Alias:    "total_revenue",
		Type:     qsbridge.DataTypeFloat,
	}}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "l_returnflag":
						value := "A"
						if rownum == 3 {
							value = "B"
						}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: value})
					case "l_extendedprice":
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: float64(rownum * 10)})
					case "l_linenumber":
						value := int64(1)
						if rownum == 2 {
							value = 0
						}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value})
					default:
						t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}

	result, err := executor.legacyDirectRelationshipGraphGroupedAggregateResult(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{1, 2, 3},
		nil,
		legacyDirectRelationshipGraphScratchpad{},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("graph grouped aggregate: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_aligned_roles", "lineitem:3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_materialization_rows", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_materialization_fields", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_materialization_field_list", "lineitem.l_extendedprice,lineitem.l_linenumber,lineitem.l_returnflag")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_residual_predicates", "1")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_residual_rows_before", "3")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_residual_rows_after", "2")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_residual_rows_removed", "1")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_tuple_expansion_skipped", "false")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "roles", "lineitem")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "expanded_rows", "3")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "filtered_rows", "2")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "materialized_fields", "lineitem.l_extendedprice,lineitem.l_linenumber,lineitem.l_returnflag")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "aggregate_alias", "total_revenue")
	assertExecutionProbeName(t, result.Probes, "relationship_join", "phase_graph_grouped_aggregate_materialization_elapsed")
	assertExecutionProbeName(t, result.Probes, "relationship_join", "phase_graph_grouped_aggregate_tuple_expansion_elapsed")
	assertExecutionProbeName(t, result.Probes, "relationship_join", "phase_graph_grouped_aggregate_residual_filter_elapsed")
	assertExecutionProbeName(t, result.Probes, "relationship_join", "phase_graph_grouped_aggregate_execution_elapsed")
	assertExecutionProbeName(t, result.Probes, "relationship_join", "graph_grouped_aggregate_materialization_field_1_lineitem_lineitem_l_returnflag_fetch_elapsed")
	assertExecutionProbeName(t, result.Probes, "relationship_join", "graph_grouped_aggregate_materialization_field_1_lineitem_lineitem_l_returnflag_attach_elapsed")
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two groups after residual filter", chunk.Rows)
	}
}

func TestLegacyDirectRelationshipGraphGroupedAggregateSkipsTupleExpansionWithoutTupleWork(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	group := qsbridge.FieldRef{Table: lineitem, Name: "l_returnflag", Type: qsbridge.DataTypeString}
	price := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "lineitem", Field: "l_returnflag", Type: qsbridge.DataTypeString},
			{Index: "lineitem", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
		},
	})
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(group)}
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "sum",
		Input:    qsbridge.Field(price),
		Alias:    "total_revenue",
		Type:     qsbridge.DataTypeFloat,
	}}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					switch field.Field {
					case "l_returnflag":
						value := "A"
						if rownum == 3 {
							value = "B"
						}
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: value})
					case "l_extendedprice":
						vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: float64(rownum * 10)})
					default:
						t.Fatalf("unexpected materialized field %s.%s", field.Index, field.Field)
					}
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}

	result, err := executor.legacyDirectRelationshipGraphGroupedAggregateResult(
		context.Background(),
		request,
		"lineitem",
		[]qsbridge.QuantaRownum{1, 2, 3},
		nil,
		legacyDirectRelationshipGraphScratchpad{},
		ExecutionResult{Count: 3},
	)
	if err != nil {
		t.Fatalf("graph grouped aggregate: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_tuple_expansion_skipped", "true")
	assertExecutionProbe(t, result.Probes, "relationship_join", "phase_graph_grouped_aggregate_tuple_expansion_elapsed", "0s")
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_residual_predicates", "0")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "groups", "2")
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two groups", chunk.Rows)
	}
}

func TestLegacyDirectRelationshipGraphGroupedAggregateKeepsAncestorGroupField(t *testing.T) {
	customer := qsbridge.TableInstance{Table: "customers_qa", Alias: "c"}
	city := qsbridge.FieldRef{Table: customer, Name: "city", Type: qsbridge.DataTypeString}
	order := qsbridge.TableInstance{Table: "orders_qa", Alias: "o"}
	lineitem := qsbridge.TableInstance{Table: "lineitems_qa", Alias: "l"}
	orderID := qsbridge.FieldRef{Table: order, Name: "order_id", Type: qsbridge.DataTypeInt}
	lineOrderID := qsbridge.FieldRef{Table: lineitem, Name: "order_id", Type: qsbridge.DataTypeInt}
	custID := qsbridge.FieldRef{Table: customer, Name: "cust_id", Type: qsbridge.DataTypeInt}
	orderCustID := qsbridge.FieldRef{Table: order, Name: "cust_id", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "lineitems_qa", Role: "l", Field: "order_id", Type: qsbridge.DataTypeInt},
			{Index: "orders_qa", Role: "o", Field: "order_id", Type: qsbridge.DataTypeInt},
			{Index: "orders_qa", Role: "o", Field: "cust_id", Type: qsbridge.DataTypeInt},
			{Index: "customers_qa", Role: "c", Field: "cust_id", Type: qsbridge.DataTypeInt},
			{Index: "customers_qa", Role: "c", Field: "city", Type: qsbridge.DataTypeString, Visible: true},
		},
	})
	request.Joins = []qsbridge.JoinEdge{
		{
			Left:  lineOrderID,
			Right: orderID,
			On:    []qsbridge.Predicate{{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(lineOrderID), qsbridge.Field(orderID))}},
		},
		{
			Left:  orderCustID,
			Right: custID,
			On:    []qsbridge.Predicate{{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(orderCustID), qsbridge.Field(custID))}},
		},
	}
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(city)}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "line_count", Type: qsbridge.DataTypeInt}}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(city), Type: qsbridge.DataTypeString},
		{Expr: qsbridge.AggregateRef("line_count", 0), Alias: "line_count", Type: qsbridge.DataTypeInt},
	}
	edges := []legacyDirectRelationshipEdge{
		{childRole: "l", childTable: "lineitems_qa", childField: "order_id", parentRole: "o", parentTable: "orders_qa", parentField: "order_id"},
		{childRole: "o", childTable: "orders_qa", childField: "cust_id", parentRole: "c", parentTable: "customers_qa", parentField: "cust_id"},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		ProjectionCache: NewLegacyDirectRelationshipVectorProjectionCache(),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				if field.Field != "city" {
					t.Fatalf("materializer was asked for pruned relationship field %s.%s", field.Index, field.Field)
				}
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					value := "Seattle"
					if rownum == 2 {
						value = "Tacoma"
					}
					vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: value})
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}
	for _, edge := range edges {
		fromTime, toTime := executor.legacyDirectRelationshipBroadVectorProjectionWindow(edge.childTable)
		cacheKey := executor.legacyDirectRelationshipProjectionCacheKey(edge.childTable, edge.childField, fromTime, toTime, nil)
		switch edge.childTable {
		case "lineitems_qa":
			executor.ProjectionCache.Put(cacheKey, testRelationshipVectorBSI(map[uint64]int64{
				11: 101,
				12: 101,
				13: 102,
				14: 201,
			}))
		case "orders_qa":
			executor.ProjectionCache.Put(cacheKey, testRelationshipVectorBSI(map[uint64]int64{
				101: 1,
				102: 1,
				201: 2,
			}))
		}
	}

	result, err := executor.legacyDirectRelationshipGraphGroupedAggregateResult(
		context.Background(),
		request,
		"lineitems_qa",
		[]qsbridge.QuantaRownum{11, 12, 13, 14},
		edges,
		newLegacyDirectRelationshipGraphScratchpad(map[string][]qsbridge.QuantaRownum{
			"l": []qsbridge.QuantaRownum{11, 12, 13, 14},
			"o": []qsbridge.QuantaRownum{101, 102, 201},
			"c": []qsbridge.QuantaRownum{1, 2},
		}, edges),
		ExecutionResult{Count: 4},
	)
	if err != nil {
		t.Fatalf("graph grouped aggregate: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_materialization_field_list", "c.city")
	assertExecutionProbe(t, result.Probes, "grouped_aggregate", "groups", "2")
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want two city groups", chunk.Rows)
	}
	if len(chunk.Rows[0]) != 2 || chunk.Rows[0][0].Value != "Seattle" || chunk.Rows[0][1].Value != int64(3) {
		t.Fatalf("first row = %#v, want Seattle/3", chunk.Rows[0])
	}
	if len(chunk.Rows[1]) != 2 || chunk.Rows[1][0].Value != "Tacoma" || chunk.Rows[1][1].Value != int64(1) {
		t.Fatalf("second row = %#v, want Tacoma/1", chunk.Rows[1])
	}
}

func TestLegacyDirectRelationshipPostReductionFieldsKeepsResidualPredicateFields(t *testing.T) {
	supplier := qsbridge.TableInstance{Table: "supplier", Alias: "s"}
	customer := qsbridge.TableInstance{Table: "customer", Alias: "c"}
	sNation := qsbridge.FieldRef{Table: supplier, Name: "s_nationkey", Type: qsbridge.DataTypeInt}
	cNation := qsbridge.FieldRef{Table: customer, Name: "c_nationkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(sNation), qsbridge.Field(cNation)),
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}

	fields := legacyDirectRelationshipPostReductionFields(request, []qsbridge.QuantaProjectionField{
		{Index: "lineitem", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
		{Index: "supplier", Field: "s_nationkey", Type: qsbridge.DataTypeInt},
		{Index: "customer", Field: "c_nationkey", Type: qsbridge.DataTypeInt},
	})

	if len(fields) != 2 {
		t.Fatalf("fields = %#v, want only residual predicate fields", fields)
	}
	if fields[0].Field != "s_nationkey" || fields[1].Field != "c_nationkey" {
		t.Fatalf("fields = %#v, want supplier/customer nation keys", fields)
	}
}

func assertLegacyDirectRelationshipProjectionField(t *testing.T, fields []qsbridge.QuantaProjectionField, index string, field string) {
	t.Helper()
	for _, candidate := range fields {
		if candidate.Index == index && candidate.Field == field {
			return
		}
	}
	t.Fatalf("fields = %#v, want %s.%s", fields, index, field)
}

type fakeRelationshipVectorAggregateReader struct {
	Result   LegacyDirectRelationshipVectorAggregateResult
	OK       bool
	Requests []LegacyDirectRelationshipVectorAggregateRequest
}

func (r *fakeRelationshipVectorAggregateReader) ReadRelationshipVectorAggregate(_ context.Context, request LegacyDirectRelationshipVectorAggregateRequest) (LegacyDirectRelationshipVectorAggregateResult, qsbridge.DiagnosticSet, bool, error) {
	r.Requests = append(r.Requests, request)
	return r.Result, nil, r.OK, nil
}

func assertNoLegacyDirectRelationshipProjectionField(t *testing.T, fields []qsbridge.QuantaProjectionField, index string, field string) {
	t.Helper()
	for _, candidate := range fields {
		if candidate.Index == index && candidate.Field == field {
			t.Fatalf("fields = %#v, did not want %s.%s", fields, index, field)
		}
	}
}

func assertLegacyDirectRelationshipMaterializationFields(t *testing.T, requests []qsbridge.QuantaMaterializationRequest, index string, fields []string) {
	t.Helper()
	for _, request := range requests {
		if request.Index != index {
			continue
		}
		if len(request.ProjectionFields) != len(fields) {
			t.Fatalf("%s fields = %#v, want %#v", index, request.ProjectionFields, fields)
		}
		for i, field := range fields {
			if request.ProjectionFields[i].Field != field {
				t.Fatalf("%s fields = %#v, want %#v", index, request.ProjectionFields, fields)
			}
		}
		return
	}
	t.Fatalf("materializations = %#v, want request for %s", requests, index)
}

func assertLegacyDirectRelationshipMaterializationRownums(t *testing.T, requests []qsbridge.QuantaMaterializationRequest, index string, rownums []qsbridge.QuantaRownum) {
	t.Helper()
	for _, request := range requests {
		if request.Index != index {
			continue
		}
		if len(request.Rownums) != len(rownums) {
			t.Fatalf("%s rownums = %#v, want %#v", index, request.Rownums, rownums)
		}
		for i, rownum := range rownums {
			if request.Rownums[i] != rownum {
				t.Fatalf("%s rownums = %#v, want %#v", index, request.Rownums, rownums)
			}
		}
		return
	}
	t.Fatalf("materializations = %#v, want request for %s", requests, index)
}

func sameRownumSet(got, want []qsbridge.QuantaRownum) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[qsbridge.QuantaRownum]int, len(want))
	for _, rownum := range want {
		seen[rownum]++
	}
	for _, rownum := range got {
		if seen[rownum] == 0 {
			return false
		}
		seen[rownum]--
	}
	return true
}

func sameLegacyDirectRelationshipPairSet(got, want []legacyDirectRelationshipPair) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[legacyDirectRelationshipPair]int, len(want))
	for _, pair := range want {
		seen[pair]++
	}
	for _, pair := range got {
		if seen[pair] == 0 {
			return false
		}
		seen[pair]--
	}
	return true
}
