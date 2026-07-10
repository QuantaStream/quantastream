package qsfixture

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestProjectionMaterializationFixtureKernelMaterializesColumnarRowSets(t *testing.T) {
	kernel := NewProjectionMaterializationFixtureKernel(map[string]map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell{
		"orders": {
			7: {
				"o_orderkey":   {Kind: qsbridge.ValueInt, Value: int64(1001)},
				"o_totalprice": {Kind: qsbridge.ValueFloat, Value: float64(42.5)},
			},
			8: {
				"o_orderkey":   {Kind: qsbridge.ValueInt, Value: int64(1002)},
				"o_totalprice": {Kind: qsbridge.ValueFloat, Value: float64(64.25)},
			},
		},
	})

	result, err := kernel.MaterializeProjectionBatches(context.Background(), qsbridge.ProjectionMaterializationKernelRequest{
		ID:          "projection_materialization",
		ProbePrefix: "projection_materialization_",
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:        "orders",
			DependencyID: "materialization_batch.1.orders",
			Batch:        qsbridge.ProjectionBatch{Size: 2, Final: true},
			Rownums:      []qsbridge.QuantaRownum{7, 8},
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Index: "orders", Field: "o_orderkey", Visible: true},
				{Index: "orders", Field: "o_totalprice", Visible: true},
			},
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if result.ID != "projection_materialization" || len(result.Results) != 1 {
		t.Fatalf("result = %#v, want one materialized batch", result)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	rowSet := result.Results[0].RowSet
	if rowSet.Index != "orders" || rowSet.CandidateCount() != 2 || rowSet.ProjectionCount() != 2 {
		t.Fatalf("rowSet = %#v, want orders/2 candidates/2 projections", rowSet)
	}
	if rowSet.DependencyID != "materialization_batch.1.orders" || !rowSet.Batch.Final {
		t.Fatalf("rowSet metadata = %#v, want dependency and batch copied", rowSet)
	}
	if got := rowSet.ProjectionVectors[0].Values[1].Value; got != int64(1002) {
		t.Fatalf("second order key = %#v, want 1002", got)
	}
	if len(result.Probes) != 2 || len(result.Results[0].Probes) != 1 {
		t.Fatalf("probes = %#v/%#v, want request/result observations", result.Probes, result.Results[0].Probes)
	}
}

func TestProjectionMaterializationFixtureKernelCopiesInputRows(t *testing.T) {
	rows := map[string]map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell{
		"orders": {
			7: {"o_orderkey": {Kind: qsbridge.ValueInt, Value: int64(1001)}},
		},
	}
	kernel := NewProjectionMaterializationFixtureKernel(rows)
	rows["orders"][7]["o_orderkey"] = qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(9999)}

	result, err := kernel.MaterializeProjectionBatches(context.Background(), qsbridge.ProjectionMaterializationKernelRequest{
		Requests: []qsbridge.QuantaMaterializationRequest{{
			Index:            "orders",
			Rownums:          []qsbridge.QuantaRownum{7},
			ProjectionFields: []qsbridge.QuantaProjectionField{{Field: "o_orderkey", Visible: true}},
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if got := result.Results[0].RowSet.ProjectionVectors[0].Values[0].Value; got != int64(1001) {
		t.Fatalf("materialized value = %#v, want copied fixture value 1001", got)
	}
}
