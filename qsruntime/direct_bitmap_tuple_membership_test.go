package qsruntime

import (
	"context"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestDirectBitmapRuntimeAppliesTupleMembership(t *testing.T) {
	nation := qsbridge.TableInstance{Table: "nation", Alias: "n"}
	region := qsbridge.TableInstance{Table: "region", Alias: "r"}
	nRegionKey := qsbridge.FieldRef{Table: nation, Name: "n_regionkey", PhysicalName: "n_regionkey", Type: qsbridge.DataTypeInt}
	nNationKey := qsbridge.FieldRef{Table: nation, Name: "n_nationkey", PhysicalName: "n_nationkey", Type: qsbridge.DataTypeInt}
	rRegionKey := qsbridge.FieldRef{Table: region, Name: "r_regionkey", PhysicalName: "r_regionkey", Type: qsbridge.DataTypeInt}
	rName := qsbridge.FieldRef{Table: region, Name: "r_name", PhysicalName: "r_name", Type: qsbridge.DataTypeString}

	runtime := DirectBitmapRuntime{
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					root, ok := request.RootIndex()
					if !ok || root != "region" {
						t.Fatalf("membership RHS root index = %q/%t, want region/true", root, ok)
					}
					return BitmapQueryResult{
						Success: true,
						Count:   2,
						Rownums: []qsbridge.QuantaRownum{10, 20},
					}, nil, nil
				},
				ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet { return nil },
			}, nil, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			return tupleMembershipTestMaterializedRowSet(t, request), nil, nil
		}),
	}
	membership := qsbridge.MembershipEdge{
		Left:       nRegionKey,
		Right:      rRegionKey,
		LeftTuple:  []qsbridge.Expr{qsbridge.Field(nRegionKey), qsbridge.Field(nNationKey)},
		RightTuple: []qsbridge.Expr{qsbridge.Field(rRegionKey), qsbridge.Literal(qsbridge.ValueInt, int64(1))},
		Kind:       qsbridge.MembershipSemi,
		Legal:      true,
		Predicates: []qsbridge.Predicate{{
			Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(rName), qsbridge.Literal(qsbridge.ValueString, "AMERICA")),
			Placement: qsbridge.PredicateResidualScan,
			Scope:     qsbridge.PredicateScopeWhere,
		}},
	}

	filtered, _, diagnostics, err := runtime.directBitmapApplyTupleMembership(context.Background(), BitmapQueryResult{
		Success: true,
		Count:   3,
		Rownums: []qsbridge.QuantaRownum{1, 2, 3},
	}, membership)
	if err != nil {
		t.Fatalf("tuple membership: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !reflect.DeepEqual(filtered.Rownums, []qsbridge.QuantaRownum{1}) {
		t.Fatalf("filtered rownums = %#v, want [1]", filtered.Rownums)
	}
}

func tupleMembershipTestMaterializedRowSet(t *testing.T, request qsbridge.QuantaMaterializationRequest) qsbridge.QuantaProjectedRowSet {
	t.Helper()
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   request.Index,
		Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
	}
	for _, field := range request.ProjectionFields {
		vector := qsbridge.QuantaProjectionVector{Field: field}
		for _, rownum := range request.Rownums {
			cell, ok := tupleMembershipTestCell(request.Index, field.Field, rownum)
			if !ok {
				t.Fatalf("unexpected materialization field %s.%s row=%d", request.Index, field.Field, rownum)
			}
			vector.Values = append(vector.Values, cell)
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
	}
	return rowSet
}

func tupleMembershipTestCell(index string, field string, rownum qsbridge.QuantaRownum) (qsbridge.ResultCell, bool) {
	switch index {
	case "nation":
		rows := map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell{
			1: {"n_regionkey": {Kind: qsbridge.ValueInt, Value: int64(1)}, "n_nationkey": {Kind: qsbridge.ValueInt, Value: int64(1)}},
			2: {"n_regionkey": {Kind: qsbridge.ValueInt, Value: int64(1)}, "n_nationkey": {Kind: qsbridge.ValueInt, Value: int64(2)}},
			3: {"n_regionkey": {Kind: qsbridge.ValueInt, Value: int64(2)}, "n_nationkey": {Kind: qsbridge.ValueInt, Value: int64(1)}},
		}
		cell, ok := rows[rownum][field]
		return cell, ok
	case "region":
		rows := map[qsbridge.QuantaRownum]map[string]qsbridge.ResultCell{
			10: {"r_regionkey": {Kind: qsbridge.ValueInt, Value: int64(1)}, "r_name": {Kind: qsbridge.ValueString, Value: "AMERICA"}},
			20: {"r_regionkey": {Kind: qsbridge.ValueInt, Value: int64(2)}, "r_name": {Kind: qsbridge.ValueString, Value: "ASIA"}},
		}
		cell, ok := rows[rownum][field]
		return cell, ok
	default:
		return qsbridge.ResultCell{}, false
	}
}
