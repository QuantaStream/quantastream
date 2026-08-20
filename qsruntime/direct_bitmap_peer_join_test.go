package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestDirectBitmapRuntimeMaterializesNonEquiPeerJoinGroupedCount(t *testing.T) {
	region := qsbridge.TableInstance{ID: "r", Table: "region", Alias: "r"}
	nation := qsbridge.TableInstance{ID: "n", Table: "nation", Alias: "n"}
	regionKey := qsbridge.FieldRef{Table: region, Name: "r_regionkey", Type: qsbridge.DataTypeInt}
	regionName := qsbridge.FieldRef{Table: region, Name: "r_name", Type: qsbridge.DataTypeString}
	nationRegionKey := qsbridge.FieldRef{Table: nation, Name: "n_regionkey", Type: qsbridge.DataTypeInt}
	nationKey := qsbridge.FieldRef{Table: nation, Name: "n_nationkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"region", "nation"}
	request.Sources = []qsbridge.TableInstance{region, nation}
	request.Joins = []qsbridge.JoinEdge{{
		Left:      nationRegionKey,
		Right:     regionKey,
		Operator:  qsbridge.BinaryOpGreaterEqual,
		Kind:      qsbridge.JoinKindInner,
		Direction: qsbridge.JoinPeerEquality,
		Legal:     true,
	}}
	request.GroupBy = []qsbridge.Expr{qsbridge.Field(regionName)}
	request.SQLAggregates = []qsbridge.Aggregate{{
		Function: "count",
		Input:    qsbridge.Field(nationKey),
		Alias:    "nation_count",
	}}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: qsbridge.Field(regionName), Alias: "region_name", Type: qsbridge.DataTypeString},
		{Expr: qsbridge.AggregateRef("nation_count", 0), Alias: "nation_count", Type: qsbridge.DataTypeInt},
	}
	request.OrderBy = []qsbridge.SortSpec{{Expr: qsbridge.Field(regionName), Direction: qsbridge.SortAscending}}

	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(_ context.Context, req ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			index, _ := req.RootIndex()
			return DirectSessionHandleFunc{
				QueryFunc: func(context.Context, ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					switch index {
					case "region":
						return BitmapQueryResult{Success: true, Count: 2, Rownums: []qsbridge.QuantaRownum{1, 2}}, nil, nil
					case "nation":
						return BitmapQueryResult{Success: true, Count: 3, Rownums: []qsbridge.QuantaRownum{1, 2, 3}}, nil, nil
					default:
						t.Fatalf("unexpected root index %q", index)
					}
					return BitmapQueryResult{}, nil, nil
				},
			}, nil, nil
		}),
		Materialization: qsruntimeMaterializationKernelFunc(func(_ context.Context, kernelRequest qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			result := qsbridge.ProjectionMaterializationKernelResult{ID: kernelRequest.ID}
			for _, materialization := range kernelRequest.Requests {
				rowSet := qsbridge.QuantaProjectedRowSet{
					Index:   materialization.Index,
					Rownums: append([]qsbridge.QuantaRownum(nil), materialization.Rownums...),
				}
				for _, field := range materialization.ProjectionFields {
					rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
						Field:  field,
						Values: peerJoinTestValues(materialization.Index, field.Field),
					})
				}
				result.Results = append(result.Results, qsbridge.ProjectionMaterializationResult{Request: materialization, RowSet: rowSet})
			}
			return result, nil
		}),
	}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("ExecuteDirect error = %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v", diagnostics)
	}
	if len(chunk.Rows) != 2 {
		t.Fatalf("rows = %#v, want 2 grouped rows", chunk.Rows)
	}
	if chunk.Rows[0][0].Value != "AFRICA" || chunk.Rows[0][1].Value != int64(3) {
		t.Fatalf("first row = %#v, want AFRICA/3", chunk.Rows[0])
	}
	if chunk.Rows[1][0].Value != "AMERICA" || chunk.Rows[1][1].Value != int64(2) {
		t.Fatalf("second row = %#v, want AMERICA/2", chunk.Rows[1])
	}
}

func peerJoinTestValues(index string, field string) []qsbridge.ResultCell {
	switch index + "." + field {
	case "region.r_regionkey":
		return []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(0)}, {Kind: qsbridge.ValueInt, Value: int64(1)}}
	case "region.r_name":
		return []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "AFRICA"}, {Kind: qsbridge.ValueString, Value: "AMERICA"}}
	case "nation.n_regionkey":
		return []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(0)}, {Kind: qsbridge.ValueInt, Value: int64(1)}, {Kind: qsbridge.ValueInt, Value: int64(2)}}
	case "nation.n_nationkey":
		return []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(10)}, {Kind: qsbridge.ValueInt, Value: int64(20)}, {Kind: qsbridge.ValueInt, Value: int64(30)}}
	default:
		return nil
	}
}
