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
