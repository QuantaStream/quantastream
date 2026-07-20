package qsruntime

import (
	"context"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestDirectBitmapRuntimeAppliesSameRowResidualBeforeCountAggregate(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}).WithCandidateSet(qsbridge.QuantaCandidateSet{
		Index:   "lineitem",
		Rownums: []qsbridge.QuantaRownum{1, 2, 3},
	})
	request.Predicates = []qsbridge.Predicate{{
		Placement: qsbridge.PredicateResidualScan,
		Expr: qsbridge.Binary(
			qsbridge.BinaryOpGreater,
			qsbridge.Field(qsbridge.FieldRef{Table: lineitem, Name: "l_receiptdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}),
			qsbridge.Field(qsbridge.FieldRef{Table: lineitem, Name: "l_commitdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}),
		),
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "late_receipt_line_count", Type: qsbridge.DataTypeInt}}

	runtime := DirectBitmapRuntime{
		Adapter: BitmapQueryResultAdapter{},
		SameRowComparison: SameRowComparisonKernelFunc(func(_ context.Context, comparison qsbridge.SameRowComparisonRequest) (qsbridge.SameRowComparisonResult, error) {
			if got, want := comparison.Domain.Rownums, []qsbridge.QuantaRownum{1, 2, 3}; !reflect.DeepEqual(got, want) {
				t.Fatalf("comparison rownums = %#v, want %#v", got, want)
			}
			return qsbridge.SameRowComparisonResult{
				ID: comparison.ID,
				Domain: qsbridge.RownumDomainSet{
					Domain:  comparison.Domain.Domain,
					Rownums: []qsbridge.QuantaRownum{2, 3},
				},
				Probes: []qsbridge.ProjectionProbe{{
					Section: "same_row_comparison",
					Name:    comparison.ProbePrefix + "fake",
					Value:   "called",
				}},
			}, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("ExecuteDirect: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if result.Count != 1 {
		t.Fatalf("result count = %d, want one aggregate row", result.Count)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if got := chunk.Rows[0][0].Value; got != int64(2) {
		t.Fatalf("count = %#v, want 2", got)
	}
	assertExecutionProbe(t, result.Probes, "direct_bitmap_same_row", "plan_count", "1")
	assertExecutionProbe(t, result.Probes, "direct_bitmap_same_row", "direct_bitmap_same_row_1_policy", "native_same_row")
	assertExecutionProbe(t, result.Probes, "direct_bitmap_same_row", "direct_bitmap_same_row_1_policy_reason", "native_compares_bsi_values_without_sql_projection")
	assertExecutionProbe(t, result.Probes, "same_row_comparison", "direct_bitmap_same_row_1_fake", "called")
}

func TestDirectBitmapSameRowPruningRefreshesExplicitMaterializationFields(t *testing.T) {
	lineitem := qsbridge.TableInstance{ID: "lineitem_1", Table: "lineitem", Alias: "l"}
	shipmode := qsbridge.FieldRef{Table: lineitem, Name: "l_shipmode", PhysicalName: "l_shipmode", Type: qsbridge.DataTypeString}
	receiptDate := qsbridge.FieldRef{Table: lineitem, Name: "l_receiptdate", PhysicalName: "l_receiptdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}
	commitDate := qsbridge.FieldRef{Table: lineitem, Name: "l_commitdate", PhysicalName: "l_commitdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_shipmode",
			Operation: qsbridge.QuantaOperationIntersect,
			Values:    nil,
		}},
	})
	request.Sources = []qsbridge.TableInstance{lineitem}
	request.SourceIndexes = []string{"lineitem"}
	request.Predicates = []qsbridge.Predicate{
		{
			Placement: qsbridge.PredicatePushdown,
			Scope:     qsbridge.PredicateScopeWhere,
			Expr:      qsbridge.Binary(qsbridge.BinaryOpGreaterEqual, qsbridge.Field(receiptDate), qsbridge.Literal(qsbridge.ValueString, "1994-01-01")),
		},
		{
			Placement: qsbridge.PredicateResidualScan,
			Scope:     qsbridge.PredicateScopeWhere,
			Expr:      qsbridge.Binary(qsbridge.BinaryOpLess, qsbridge.Field(commitDate), qsbridge.Field(receiptDate)),
		},
	}
	request.Projection = []qsbridge.ProjectionColumn{{Expr: qsbridge.Field(shipmode)}}
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(shipmode)}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "line_count", Type: qsbridge.DataTypeInt}}
	request.Materialization = qsbridge.QuantaMaterializationRequest{
		Index: "lineitem",
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "lineitem", Role: "l", Field: "l_shipmode", Visible: true},
			{Index: "lineitem", Role: "l", Field: "l_commitdate"},
			{Index: "lineitem", Role: "l", Field: "l_receiptdate"},
		},
	}

	pruned := directBitmapWithoutSameRowResidualPredicates(request)
	if len(pruned.Predicates) != 1 || pruned.Predicates[0].Placement != qsbridge.PredicatePushdown {
		t.Fatalf("predicates after pruning = %#v, want only pushdown predicate", pruned.Predicates)
	}
	if got := pruned.Materialization.ProjectionFields; len(got) != 1 || got[0].Field != "l_shipmode" {
		t.Fatalf("materialization fields = %#v, want only l_shipmode", got)
	}
}

func TestDirectBitmapRuntimeAppliesSameRowResidualBeforeMembership(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	orders := qsbridge.TableInstance{Table: "orders", Alias: "o"}
	orderKey := qsbridge.FieldRef{Table: lineitem, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	orderOrderKey := qsbridge.FieldRef{Table: orders, Name: "o_orderkey", PhysicalName: "o_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	receiptDate := qsbridge.FieldRef{Table: lineitem, Name: "l_receiptdate", PhysicalName: "l_receiptdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}
	commitDate := qsbridge.FieldRef{Table: lineitem, Name: "l_commitdate", PhysicalName: "l_commitdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}
	sameRowCalled := false
	runtime := DirectBitmapRuntime{
		Adapter: BitmapQueryResultAdapter{},
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					index, ok := request.RootIndex()
					if !ok {
						t.Fatalf("query request missing root index")
					}
					switch index {
					case "lineitem":
						return BitmapQueryResult{Success: true, Count: 3, Rownums: []qsbridge.QuantaRownum{1, 2, 3}}, nil, nil
					case "orders":
						return BitmapQueryResult{Success: true, Count: 1, Rownums: []qsbridge.QuantaRownum{101}}, nil, nil
					default:
						t.Fatalf("unexpected query root index %q", index)
						return BitmapQueryResult{}, nil, nil
					}
				},
			}, nil, nil
		}),
		SameRowComparison: SameRowComparisonKernelFunc(func(_ context.Context, comparison qsbridge.SameRowComparisonRequest) (qsbridge.SameRowComparisonResult, error) {
			sameRowCalled = true
			if got, want := comparison.Domain.Rownums, []qsbridge.QuantaRownum{1, 2, 3}; !reflect.DeepEqual(got, want) {
				t.Fatalf("same-row candidates = %#v, want %#v", got, want)
			}
			return qsbridge.SameRowComparisonResult{
				ID: comparison.ID,
				Domain: qsbridge.RownumDomainSet{
					Domain:  comparison.Domain.Domain,
					Rownums: []qsbridge.QuantaRownum{2, 3},
				},
			}, nil
		}),
		Materialization: qsruntimeMaterializationKernelFunc(func(_ context.Context, request qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			if request.RequestCount() != 1 {
				t.Fatalf("materialization request count = %d, want one", request.RequestCount())
			}
			materializationRequest := request.Requests[0]
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   materializationRequest.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), materializationRequest.Rownums...),
			}
			for _, field := range materializationRequest.ProjectionFields {
				if field.Field == "l_receiptdate" || field.Field == "l_commitdate" {
					t.Fatalf("same-row field %s should have been removed before membership materialization", field.Field)
				}
				values := make([]qsbridge.ResultCell, 0, len(materializationRequest.Rownums))
				for _, rownum := range materializationRequest.Rownums {
					values = append(values, sameRowMembershipIntCell(materializationRequest.Index, rownum, field.Field))
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{Field: field, Values: values})
			}
			if materializationRequest.Index == "lineitem" && !reflect.DeepEqual(materializationRequest.Rownums, []qsbridge.QuantaRownum{2, 3}) {
				t.Fatalf("lineitem membership materialization rownums = %#v, want reduced candidates", materializationRequest.Rownums)
			}
			return qsbridge.ProjectionMaterializationKernelResult{
				ID: request.ID,
				Results: []qsbridge.ProjectionMaterializationResult{{
					ID:      materializationRequest.DependencyID,
					Request: materializationRequest,
					RowSet:  rowSet,
				}},
			}, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	request.Predicates = []qsbridge.Predicate{{
		Placement: qsbridge.PredicateResidualScan,
		Expr:      qsbridge.Binary(qsbridge.BinaryOpGreater, qsbridge.Field(receiptDate), qsbridge.Field(commitDate)),
	}}
	request.Memberships = []qsbridge.MembershipEdge{{
		Left:  orderKey,
		Right: orderOrderKey,
		Kind:  qsbridge.MembershipSemi,
		Legal: true,
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "late_order_count", Type: qsbridge.DataTypeInt}}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("ExecuteDirect: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if !sameRowCalled {
		t.Fatalf("same-row comparison was not called")
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if got := chunk.Rows[0][0].Value; got != int64(1) {
		t.Fatalf("count = %#v, want 1", got)
	}
}

func sameRowMembershipIntCell(index string, rownum qsbridge.QuantaRownum, field string) qsbridge.ResultCell {
	values := map[string]map[qsbridge.QuantaRownum]int64{
		"lineitem.l_orderkey": {
			2: 200,
			3: 300,
		},
		"orders.o_orderkey": {
			101: 200,
		},
	}
	value, ok := values[index+"."+field][rownum]
	if !ok {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull}
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value}
}
