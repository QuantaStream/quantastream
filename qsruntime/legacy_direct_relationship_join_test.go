package qsruntime

import (
	"context"
	"math/big"
	"testing"

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

func TestLegacyDirectRelationshipAllRownumRequestPrefersRelationshipEndpointField(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "customers_qa",
			PrimaryKey:       "cust_id",
			TimeQuantumField: "createdAtTimestamp",
		},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "cust_id", Type: "string", MappingStrategy: "StringHashBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "createdAtTimestamp", Type: "DateTime", MappingStrategy: "SysMicroBSI"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "rownum", Type: "int", MappingStrategy: "IntDirect"}},
		},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		TableCache: &core.TableCacheStruct{TableCache: map[string]*core.Table{"customers_qa": table}},
	}

	request := executor.legacyDirectRelationshipAllRownumRequest("customers_qa", "cust_id")

	if len(request.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one relationship endpoint existence fallback", request.Query.Fragments)
	}
	fragment := request.Query.Fragments[0]
	if fragment.Index != "customers_qa" || fragment.Field != "cust_id" {
		t.Fatalf("fragment target = %s.%s, want customers_qa.cust_id", fragment.Index, fragment.Field)
	}
	if !fragment.NullCheck || !fragment.Negate {
		t.Fatalf("fragment = %#v, want not-null endpoint existence fallback", fragment)
	}
	if fragment.BSIOp != "" || fragment.Value != nil {
		t.Fatalf("fragment = %#v, did not expect time BSI fallback", fragment)
	}
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

	if len(request.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one synthetic full time range", request.Query.Fragments)
	}
	fragment := request.Query.Fragments[0]
	if fragment.Index != "orders" || fragment.Field != "o_orderdate" {
		t.Fatalf("fragment target = %s.%s, want orders.o_orderdate", fragment.Index, fragment.Field)
	}
	if fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil || fragment.End == nil {
		t.Fatalf("fragment = %#v, want synthetic full time range", fragment)
	}
	begin, end := legacyDirectRelationshipFullTimeRangeEncoded(table, "o_orderdate")
	if fragment.Begin.Int64() != begin || fragment.End.Int64() != end {
		t.Fatalf("range = %d..%d, want %d..%d", fragment.Begin.Int64(), fragment.End.Int64(), begin, end)
	}
	if fragment.NullCheck || fragment.Negate {
		t.Fatalf("fragment = %#v, did not expect null-check fallback", fragment)
	}
	if len(request.Query.ProjectionFields) != 1 || request.Query.ProjectionFields[0].Field != "o_orderdate" || request.Query.ProjectionFields[0].Type != qsbridge.DataTypeTime {
		t.Fatalf("projection fields = %#v, want time-window metadata", request.Query.ProjectionFields)
	}
}

func TestLegacyDirectRelationshipAllRownumRequestPrefersPrimaryKeyFallbackOverTimeQuantum(t *testing.T) {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "orders",
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

	if len(request.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one existence fallback", request.Query.Fragments)
	}
	fragment := request.Query.Fragments[0]
	if fragment.Index != "orders" || fragment.Field != "o_orderkey" {
		t.Fatalf("fragment target = %s.%s, want primary key fallback instead of full time seed", fragment.Index, fragment.Field)
	}
	if !fragment.NullCheck || !fragment.Negate {
		t.Fatalf("fragment = %#v, want not-null existence fallback", fragment)
	}
}

func TestLegacyDirectRelationshipAllRownumRequestPrefersParentRelationFallbackOverTimeQuantum(t *testing.T) {
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

	if len(request.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one existence fallback", request.Query.Fragments)
	}
	fragment := request.Query.Fragments[0]
	if fragment.Index != "orders" || fragment.Field != "o_custkey" {
		t.Fatalf("fragment target = %s.%s, want parent relation fallback instead of full time seed", fragment.Index, fragment.Field)
	}
	if !fragment.NullCheck || !fragment.Negate {
		t.Fatalf("fragment = %#v, want not-null existence fallback", fragment)
	}
}

func TestLegacyDirectRelationshipAllRownumRequestDerivedTimeDoesNotOverrideEndpointFallback(t *testing.T) {
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

	if len(request.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want endpoint fallback", request.Query.Fragments)
	}
	fragment := request.Query.Fragments[0]
	if fragment.Index != "orders_qa" || fragment.Field != "cust_id" {
		t.Fatalf("fragment target = %s.%s, want endpoint fallback instead of derived time", fragment.Index, fragment.Field)
	}
	if !fragment.NullCheck || !fragment.Negate {
		t.Fatalf("fragment = %#v, want endpoint existence fallback", fragment)
	}
}

func TestLegacyDirectRelationshipAllRownumRequestDerivesTimeQuantumField(t *testing.T) {
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

	if len(request.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one synthetic full time range", request.Query.Fragments)
	}
	fragment := request.Query.Fragments[0]
	if fragment.Index != "lineitem" || fragment.Field != "l_shipdate" {
		t.Fatalf("fragment target = %s.%s, want lineitem.l_shipdate", fragment.Index, fragment.Field)
	}
	if fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil || fragment.End == nil {
		t.Fatalf("fragment = %#v, want synthetic full time range", fragment)
	}
	begin, end := legacyDirectRelationshipFullTimeRangeEncoded(table, "l_shipdate")
	if fragment.Begin.Int64() != begin || fragment.End.Int64() != end {
		t.Fatalf("range = %d..%d, want %d..%d", fragment.Begin.Int64(), fragment.End.Int64(), begin, end)
	}
	if len(request.Query.ProjectionFields) != 1 || request.Query.ProjectionFields[0].Field != "l_shipdate" || request.Query.ProjectionFields[0].Type != qsbridge.DataTypeTime {
		t.Fatalf("projection fields = %#v, want derived time-window metadata", request.Query.ProjectionFields)
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

	if len(request.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one synthetic full time range", request.Query.Fragments)
	}
	fragment := request.Query.Fragments[0]
	if fragment.Index != "orders_qa" || fragment.Field != "order_date" {
		t.Fatalf("fragment target = %s.%s, want orders_qa.order_date", fragment.Index, fragment.Field)
	}
	if fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil || fragment.End == nil {
		t.Fatalf("fragment = %#v, want synthetic full time range", fragment)
	}
	begin, end := legacyDirectRelationshipFullTimeRangeEncoded(table, "order_date")
	if fragment.Begin.Int64() != begin || fragment.End.Int64() != end {
		t.Fatalf("range = %d..%d, want %d..%d", fragment.Begin.Int64(), fragment.End.Int64(), begin, end)
	}
}
func TestLegacyDirectRelationshipAllRownumRequestFallsBackToExistence(t *testing.T) {
	executor := LegacyDirectRelationshipVectorJoinExecutor{}

	request := executor.legacyDirectRelationshipAllRownumRequest("nation", "n_regionkey")

	if len(request.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one existence fallback", request.Query.Fragments)
	}
	fragment := request.Query.Fragments[0]
	if fragment.Index != "nation" || fragment.Field != "n_regionkey" {
		t.Fatalf("fragment target = %s.%s, want nation.n_regionkey", fragment.Index, fragment.Field)
	}
	if !fragment.NullCheck || !fragment.Negate {
		t.Fatalf("fragment = %#v, want not-null existence fallback", fragment)
	}
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

func TestLegacyDirectRelationshipCachedFullFKBSIReusesEqualFoundSet(t *testing.T) {
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

	got, ok := executor.legacyDirectRelationshipCachedFullFKBSI(edge, fromTime, toTime, roaring64.BitmapOf(11, 12))
	if !ok {
		t.Fatalf("full FK BSI cache hit = false, want true for equal foundset")
	}
	if got != full {
		t.Fatalf("cached BSI pointer changed")
	}
	if _, ok := executor.legacyDirectRelationshipCachedFullFKBSI(edge, fromTime, toTime, roaring64.BitmapOf(11)); ok {
		t.Fatalf("full FK BSI cache hit = true, want false for narrowed foundset")
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
	if len(materializations) != 4 {
		t.Fatalf("materializations = %d, want quantity plus three final fields", len(materializations))
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
	if got := result.Diagnostics[0].Message; got != "legacy direct relationship-vector sibling-root tuple execution is not wired in this slice: p:part->l,ps" {
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
	if got := diagnostics[0].Message; got != "legacy direct relationship-vector graph execution requires a single sink table" {
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
	assertExecutionProbe(t, result.Probes, "relationship_join", "graph_grouped_aggregate_materialization_field_list", "c.city,l.order_id,o.cust_id")
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
