package qsbridge

import (
	"context"
	"testing"
)

func TestProjectionMaterializerFuncUsesNativeContract(t *testing.T) {
	materializer := ProjectionMaterializerFunc(func(ctx context.Context, request QuantaMaterializationRequest) (QuantaProjectedRowSet, DiagnosticSet, error) {
		return QuantaProjectedRowSet{
			Index:   request.Index,
			Rownums: append([]QuantaRownum(nil), request.Rownums...),
			ProjectionVectors: []QuantaProjectionVector{
				{
					Field: request.ProjectionFields[0],
					Values: []ResultCell{
						{Kind: ValueString, Value: "first"},
						{Kind: ValueString, Value: "second"},
					},
				},
			},
		}, nil, nil
	})

	rowSet, diagnostics, err := materializer.Materialize(context.Background(), QuantaMaterializationRequest{
		Index:   "orders",
		Rownums: []QuantaRownum{7, 8},
		ProjectionFields: []QuantaProjectionField{
			{Index: "orders", Field: "o_orderpriority", Visible: true},
		},
	})
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if rowSet.Index != "orders" || rowSet.CandidateCount() != 2 || rowSet.ProjectionCount() != 1 {
		t.Fatalf("rowSet = %#v, want orders/2 candidates/1 projection", rowSet)
	}
	if rowSet.ProjectionVectors[0].Values[1].Value != "second" {
		t.Fatalf("second value = %#v, want second", rowSet.ProjectionVectors[0].Values[1])
	}
}

func TestProjectionMaterializationKernelResultReturnsCopiedRowSets(t *testing.T) {
	result := ProjectionMaterializationKernelResult{
		ID: "projection_materialization",
		Results: []ProjectionMaterializationResult{{
			ID: "materialization.1.orders",
			RowSet: QuantaProjectedRowSet{
				Index:   "orders",
				Rownums: []QuantaRownum{7, 8},
				ProjectionVectors: []QuantaProjectionVector{{
					Field: QuantaProjectionField{Index: "orders", Field: "o_orderkey", Visible: true},
					Values: []ResultCell{
						{Kind: ValueInt, Value: int64(1001)},
						{Kind: ValueInt, Value: int64(1002)},
					},
				}},
			},
		}},
	}

	rowSets := result.RowSets()
	rowSets[0].Rownums[0] = 99
	rowSets[0].ProjectionVectors[0].Values[0] = ResultCell{Kind: ValueInt, Value: int64(9999)}

	again := result.RowSets()
	if again[0].Rownums[0] != 7 {
		t.Fatalf("rowsets leaked mutable rownums: %#v", again[0].Rownums)
	}
	if again[0].ProjectionVectors[0].Values[0].Value != int64(1001) {
		t.Fatalf("rowsets leaked mutable projection values: %#v", again[0].ProjectionVectors[0].Values)
	}
}

func TestAssembleProjectionMaterializationResultMergesRowSetsAndChunks(t *testing.T) {
	field := QuantaProjectionField{Index: "orders", Field: "o_orderkey", Visible: true}
	assembly := AssembleProjectionMaterializationResult(ProjectionResultAssemblyRequest{
		ID:          "projection_assembly",
		ProbePrefix: "projection_assembly_",
		RowSets: []QuantaProjectedRowSet{
			{
				Index:   "orders",
				Rownums: []QuantaRownum{7},
				ProjectionVectors: []QuantaProjectionVector{{
					Field:  field,
					Values: []ResultCell{{Kind: ValueInt, Value: int64(1001)}},
				}},
			},
			{
				Index:   "orders",
				Rownums: []QuantaRownum{8},
				ProjectionVectors: []QuantaProjectionVector{{
					Field:  field,
					Values: []ResultCell{{Kind: ValueInt, Value: int64(1002)}},
				}},
			},
		},
	})

	if assembly.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", assembly.Diagnostics)
	}
	if assembly.RowSet.CandidateCount() != 2 || assembly.RowSet.ProjectionCount() != 1 {
		t.Fatalf("rowset = %#v, want merged two-row one-vector rowset", assembly.RowSet)
	}
	if got := assembly.RowSet.ProjectionVectors[0].Values[1].Value; got != int64(1002) {
		t.Fatalf("second merged value = %#v, want 1002", got)
	}
	if len(assembly.Chunks) != 2 || assembly.Chunks[0].Final || !assembly.Chunks[1].Final {
		t.Fatalf("chunks = %#v, want two chunks with final marker on last", assembly.Chunks)
	}
	if got := assembly.Chunks[0].Rows[0][0].Value; got != int64(1001) {
		t.Fatalf("first chunk cell = %#v, want 1001", got)
	}
}

func TestProjectionMaterializationKernelRequestCountsGroupedReads(t *testing.T) {
	request := ProjectionMaterializationKernelRequest{
		Requests: []QuantaMaterializationRequest{
			{Index: "orders"},
			{Index: "customer"},
		},
	}
	if request.RequestCount() != 2 {
		t.Fatalf("RequestCount() = %d, want 2", request.RequestCount())
	}
}

func TestProjectionProbeStaysRuntimeNeutral(t *testing.T) {
	probe := ProjectionProbe{
		Section: "materialization",
		Name:    "field_fetch_elapsed",
		Value:   "1ms",
		Detail:  "orders.o_orderdate",
	}
	if probe.Section != "materialization" || probe.Name == "" || probe.Value == "" || probe.Detail == "" {
		t.Fatalf("probe = %#v, want complete materialization observation", probe)
	}
}

func TestProjectionBatchCapturesTimeToFirstByteIntent(t *testing.T) {
	batch := ProjectionBatch{
		Size:     256,
		Sequence: 3,
		Final:    true,
		Intent:   ProjectionBatchIntentTimeToFirstByte,
	}

	if !batch.TimeToFirstByte() {
		t.Fatalf("batch = %#v, want time-to-first-byte intent", batch)
	}
	if batch.Size != 256 || batch.Sequence != 3 || !batch.Final {
		t.Fatalf("batch = %#v, want stable batch window metadata", batch)
	}
}

func TestCandidateSetFromBitmapResultCopiesRownums(t *testing.T) {
	result := QuantaBitmapQueryResult{
		Rownums: []QuantaRownum{7, 8},
	}
	candidates := CandidateSetFromBitmapResult("orders", result)
	result.Rownums[0] = 99

	if candidates.Index != "orders" {
		t.Fatalf("index = %q, want orders", candidates.Index)
	}
	if got, want := candidates.CandidateCount(), 2; got != want {
		t.Fatalf("candidate count = %d, want %d", got, want)
	}
	if candidates.Rownums[0] != 7 {
		t.Fatalf("candidate rownums = %#v, want copied rownums", candidates.Rownums)
	}
}
