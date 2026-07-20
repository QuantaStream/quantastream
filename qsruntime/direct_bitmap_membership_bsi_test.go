package qsruntime

import (
	"context"
	"math/big"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestDirectBitmapRuntimeAppliesCorrelatedSiblingMembershipWithRawBSIVectors(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}

	for _, test := range []struct {
		name string
		kind qsbridge.MembershipKind
		want int64
	}{
		{name: "semi", kind: qsbridge.MembershipSemi, want: 2},
		{name: "anti", kind: qsbridge.MembershipAnti, want: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := fakeMembershipProjectionBSIReader{
				Values: map[string]map[uint64]int64{
					"l_orderkey": {1: 10, 2: 10, 3: 20, 4: 30},
					"l_suppkey":  {1: 1, 2: 2, 3: 5, 4: 9},
				},
			}
			orderKeys := reader.Values["l_orderkey"]
			runtime := DirectBitmapRuntime{
				Adapter:             BitmapQueryResultAdapter{},
				ProjectionBSIReader: reader,
				Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
					t.Fatalf("correlated sibling BSI fast path should not materialize rows for %s", request.Index)
					return qsbridge.QuantaProjectedRowSet{}, nil, nil
				}),
				Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
					return DirectSessionHandleFunc{
						QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
							values := directBitmapTestBatchEQValues(request.Query.Fragments, "l_orderkey")
							rownums := make([]qsbridge.QuantaRownum, 0, len(orderKeys))
							for _, rownum := range []qsbridge.QuantaRownum{1, 2, 3, 4} {
								if len(values) > 0 {
									if _, ok := values[orderKeys[uint64(rownum)]]; !ok {
										continue
									}
								}
								rownums = append(rownums, rownum)
							}
							return BitmapQueryResult{Success: true, Count: uint64(len(rownums)), Rownums: rownums}, nil, nil
						},
					}, nil, nil
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
			request.Memberships = []qsbridge.MembershipEdge{{
				Left:  l1OrderKey,
				Right: l2OrderKey,
				Kind:  test.kind,
				Legal: true,
				Predicates: []qsbridge.Predicate{
					{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l2OrderKey), qsbridge.Field(l1OrderKey))},
					{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
				},
			}}
			request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "qualified_rows", Type: qsbridge.DataTypeInt}}

			result, err := runtime.ExecuteDirect(context.Background(), request)
			if err != nil {
				t.Fatalf("execute direct: %v", err)
			}
			if result.Diagnostics.BlocksNative() {
				t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
			}
			if result.RowSet.CandidateCount() != 1 || len(result.RowSet.ProjectionVectors) != 1 || len(result.RowSet.ProjectionVectors[0].Values) != 1 {
				t.Fatalf("rowset = %#v, want one count aggregate cell", result.RowSet)
			}
			if got := result.RowSet.ProjectionVectors[0].Values[0].Value; got != test.want {
				t.Fatalf("count aggregate = %#v, want %d", got, test.want)
			}
			assertExecutionProbeName(t, result.Probes, "direct_bitmap_membership", "correlated_sibling_bsi_fast_path_applied")
		})
	}
}

type fakeMembershipProjectionBSIReader struct {
	Values map[string]map[uint64]int64
}

func (r fakeMembershipProjectionBSIReader) ReadProjectionBSI(ctx context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	results, diagnostics, err := r.ReadProjectionBSIs(ctx, []NativeProjectionBSIReadRequest{request})
	if len(results) == 0 {
		return NativeProjectionBSIReadResult{}, diagnostics, err
	}
	return results[0], diagnostics, err
}

func (r fakeMembershipProjectionBSIReader) ReadProjectionBSIs(ctx context.Context, requests []NativeProjectionBSIReadRequest) ([]NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	results := make([]NativeProjectionBSIReadResult, 0, len(requests))
	for _, request := range requests {
		bsi := roaring64.NewDefaultBSI()
		values := r.Values[request.PhysicalField]
		for _, rownum := range request.Rownums {
			value, ok := values[uint64(rownum)]
			if !ok {
				continue
			}
			bsi.SetBigValue(uint64(rownum), big.NewInt(value))
		}
		results = append(results, NativeProjectionBSIReadResult{BSI: bsi})
	}
	return results, nil, nil
}

func directBitmapTestBatchEQValues(fragments []qsbridge.QuantaQueryFragment, field string) map[int64]struct{} {
	values := make(map[int64]struct{})
	for _, fragment := range fragments {
		if fragment.Field != field || fragment.BSIOp != qsbridge.QuantaBSIOpBatchEQ {
			continue
		}
		for _, value := range fragment.Values {
			if value == nil || !value.IsInt64() {
				continue
			}
			values[value.Int64()] = struct{}{}
		}
	}
	return values
}
