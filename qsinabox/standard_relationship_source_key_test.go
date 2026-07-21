package qsinabox

import (
	"context"
	"math/big"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestStandardRelationshipVectorSourceKeyReaderProjectsParentKeysFromRownums(t *testing.T) {
	ctx := qsruntime.WithQueryScratchpad(context.Background())
	cache := qsruntime.ProjectionBSICacheFromContext(ctx)
	if cache == nil {
		t.Fatalf("ProjectionBSICacheFromContext = nil")
	}

	sourceRows := []qsbridge.QuantaRownum{101, 102}
	sourceField := qsbridge.FieldRef{
		Table:        qsbridge.TableInstance{Table: "orders", Alias: "o"},
		Name:         "o_orderkey",
		PhysicalName: "o_orderkey",
		Type:         qsbridge.DataTypeInt,
		Index:        qsbridge.IndexBSI,
	}
	projectionRequest := qsruntime.NativeProjectionBSIReadRequest{
		Index:         "orders",
		Field:         standardRelationshipVectorSourceProjectionField(sourceField),
		PhysicalField: "o_orderkey",
		Rownums:       sourceRows,
	}
	parentKeyBSI := roaring64.NewDefaultBSI()
	parentKeyBSI.SetBigValue(101, big.NewInt(9001))
	parentKeyBSI.SetBigValue(102, big.NewInt(9002))
	fromTime, toTime := standardProjectionWindowNanos(nil, "orders", 0, 0)
	cache.Set(qsruntime.ProjectionBSICacheKeyFor(projectionRequest, fromTime, toTime), standardProjectionBitmap(sourceRows), parentKeyBSI)

	values, diagnostics, err := (StandardRelationshipVectorSourceKeyReader{
		Reader: StandardProjectionBSIReader{},
	}).ReadRelationshipVectorSourceKeyValues(ctx, qsruntime.LegacyDirectRelationshipVectorReadRequest{
		SourceCandidates: qsbridge.QuantaCandidateSet{Index: "orders", Rownums: sourceRows},
		SourceDomain:     "orders",
		TargetDomain:     "lineitem",
		VectorIndex:      "lineitem",
		VectorField:      "l_orderkey",
		Edge: qsbridge.RelationshipJoinPlanEdge{
			Left: qsbridge.FieldRef{
				Table:        qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
				Name:         "l_orderkey",
				PhysicalName: "l_orderkey",
				Type:         qsbridge.DataTypeInt,
				Index:        qsbridge.IndexBSI,
			},
			Right: sourceField,
		},
	})
	if err != nil {
		t.Fatalf("ReadRelationshipVectorSourceKeyValues error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	want := []int64{9001, 9002}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("values = %#v, want %#v", values, want)
	}
}
