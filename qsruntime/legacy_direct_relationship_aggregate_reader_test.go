package qsruntime

import (
	"context"
	"math/big"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestLegacyDirectSharedRelationshipVectorAggregateReaderAlignedSum(t *testing.T) {
	bsi := roaring64.NewDefaultBSI()
	bsi.SetValue(101, 100)
	bsi.SetValue(102, 250)
	bsi.SetValue(103, 99)
	bsi.SetValue(104, 200)

	projection := &fakeRelationshipAggregateProjectionSource{
		Result: NativeProjectionBSIReadResult{BSI: bsi},
	}
	reader := LegacyDirectSharedRelationshipVectorAggregateReader{Source: projection}
	result, diagnostics, ok, err := reader.ReadRelationshipVectorAggregate(context.Background(), LegacyDirectRelationshipVectorAggregateRequest{
		VectorIndex: "lineitem",
		VectorField: "l_orderkey",
		ValueIndex:  "lineitem",
		ValueField:  "l_extendedprice",
		ChildRows:   []qsbridge.QuantaRownum{101, 102, 103, 104, 104},
		ParentRows:  []qsbridge.QuantaRownum{1, 1, 2, 3, 1},
	})
	if err != nil || diagnostics.BlocksNative() || !ok {
		t.Fatalf("ReadRelationshipVectorAggregate ok/error/diagnostics = %t/%v/%v, want true/nil/none", ok, err, diagnostics)
	}
	if got, want := len(projection.Requests), 1; got != want {
		t.Fatalf("projection requests = %d, want %d", got, want)
	}
	projectionRequest := projection.Requests[0]
	if projectionRequest.Index != "lineitem" || projectionRequest.PhysicalField != "l_extendedprice" {
		t.Fatalf("projection request = %#v, want lineitem.l_extendedprice", projectionRequest)
	}
	if got, want := result.Mode, "shared_projection_aligned_sum"; got != want {
		t.Fatalf("mode = %q, want %q", got, want)
	}
	if got, want := result.Rows, uint64(5); got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if got, want := result.SourceValues, 3; got != want {
		t.Fatalf("source values = %d, want %d", got, want)
	}
	if got, want := result.TargetRows, uint64(5); got != want {
		t.Fatalf("target rows = %d, want %d", got, want)
	}
	if got, want := result.Values, uint64(3); got != want {
		t.Fatalf("values = %d, want %d", got, want)
	}
	assertRelationshipAggregateGroup(t, result.Groups, 1, 101, 3, 550)
	assertRelationshipAggregateGroup(t, result.Groups, 2, 103, 1, 99)
	assertRelationshipAggregateGroup(t, result.Groups, 3, 104, 1, 200)
}

func TestLegacyDirectSharedRelationshipVectorAggregateReaderUsesRemoteAlignedSum(t *testing.T) {
	projection := &fakeRelationshipAggregateProjectionSource{}
	remote := &fakeRelationshipAggregateRemoteSource{
		Result: LegacyDirectRelationshipVectorAggregateResult{
			Mode:       "shared_remote_aligned_sum",
			Rows:       2,
			TargetRows: 2,
			Groups: []LegacyDirectRelationshipVectorAggregateGroup{{
				ParentRow:              7,
				RepresentativeChildRow: 101,
				Count:                  2,
				Sum:                    big.NewInt(300),
			}},
		},
		OK: true,
	}
	reader := LegacyDirectSharedRelationshipVectorAggregateReader{
		Source: projection,
		Remote: remote,
	}
	result, diagnostics, ok, err := reader.ReadRelationshipVectorAggregate(context.Background(), LegacyDirectRelationshipVectorAggregateRequest{
		VectorIndex: "lineitem",
		ValueIndex:  "lineitem",
		ValueField:  "l_extendedprice",
		ChildRows:   []qsbridge.QuantaRownum{101, 102},
		ParentRows:  []qsbridge.QuantaRownum{7, 7},
	})
	if err != nil || diagnostics.BlocksNative() || !ok {
		t.Fatalf("ReadRelationshipVectorAggregate ok/error/diagnostics = %t/%v/%v, want true/nil/none", ok, err, diagnostics)
	}
	if got, want := len(remote.Requests), 1; got != want {
		t.Fatalf("remote requests = %d, want %d", got, want)
	}
	if got, want := len(projection.Requests), 0; got != want {
		t.Fatalf("projection requests = %d, want %d", got, want)
	}
	if got, want := result.Mode, "shared_remote_aligned_sum"; got != want {
		t.Fatalf("mode = %q, want %q", got, want)
	}
	assertRelationshipAggregateGroup(t, result.Groups, 7, 101, 2, 300)
}

func TestLegacyDirectSharedRelationshipVectorAggregateReaderFallsBackWhenRemoteEmpty(t *testing.T) {
	bsi := roaring64.NewDefaultBSI()
	bsi.SetValue(101, 100)
	bsi.SetValue(102, 200)
	projection := &fakeRelationshipAggregateProjectionSource{
		Result: NativeProjectionBSIReadResult{BSI: bsi},
	}
	reader := LegacyDirectSharedRelationshipVectorAggregateReader{
		Source: projection,
		Remote: &fakeRelationshipAggregateRemoteSource{
			Result: LegacyDirectRelationshipVectorAggregateResult{
				Mode: "shared_remote_aligned_sum",
				Rows: 2,
			},
			OK: true,
		},
	}
	result, diagnostics, ok, err := reader.ReadRelationshipVectorAggregate(context.Background(), LegacyDirectRelationshipVectorAggregateRequest{
		VectorIndex: "lineitem",
		ValueIndex:  "lineitem",
		ValueField:  "l_extendedprice",
		ChildRows:   []qsbridge.QuantaRownum{101, 102},
		ParentRows:  []qsbridge.QuantaRownum{7, 7},
	})
	if err != nil || diagnostics.BlocksNative() || !ok {
		t.Fatalf("ReadRelationshipVectorAggregate ok/error/diagnostics = %t/%v/%v, want true/nil/none", ok, err, diagnostics)
	}
	if got, want := len(projection.Requests), 1; got != want {
		t.Fatalf("projection requests = %d, want %d", got, want)
	}
	if got, want := result.Mode, "shared_projection_aligned_sum"; got != want {
		t.Fatalf("mode = %q, want %q", got, want)
	}
	assertRelationshipAggregateGroup(t, result.Groups, 7, 101, 2, 300)
}

func TestLegacyDirectSharedRelationshipVectorAggregateReaderDeclinesCrossIndex(t *testing.T) {
	reader := LegacyDirectSharedRelationshipVectorAggregateReader{Source: &fakeRelationshipAggregateProjectionSource{}}
	_, diagnostics, ok, err := reader.ReadRelationshipVectorAggregate(context.Background(), LegacyDirectRelationshipVectorAggregateRequest{
		VectorIndex: "lineitem",
		ValueIndex:  "orders",
		ValueField:  "o_totalprice",
		ChildRows:   []qsbridge.QuantaRownum{1},
		ParentRows:  []qsbridge.QuantaRownum{10},
	})
	if err != nil || diagnostics.BlocksNative() || ok {
		t.Fatalf("ReadRelationshipVectorAggregate ok/error/diagnostics = %t/%v/%v, want false/nil/none", ok, err, diagnostics)
	}
}

func assertRelationshipAggregateGroup(t *testing.T, groups []LegacyDirectRelationshipVectorAggregateGroup, parent qsbridge.QuantaRownum, representative qsbridge.QuantaRownum, count uint64, sum int64) {
	t.Helper()
	for _, group := range groups {
		if group.ParentRow != parent {
			continue
		}
		if group.RepresentativeChildRow != representative || group.Count != count || group.Sum == nil || group.Sum.Cmp(big.NewInt(sum)) != 0 {
			t.Fatalf("group for parent %d = %#v, want representative=%d count=%d sum=%d", parent, group, representative, count, sum)
		}
		return
	}
	t.Fatalf("missing group for parent %d in %#v", parent, groups)
}

type fakeRelationshipAggregateProjectionSource struct {
	Result   NativeProjectionBSIReadResult
	Requests []NativeProjectionBSIReadRequest
}

func (s *fakeRelationshipAggregateProjectionSource) ReadProjectionBSI(_ context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	s.Requests = append(s.Requests, request)
	return s.Result, nil, nil
}

type fakeRelationshipAggregateRemoteSource struct {
	Result   LegacyDirectRelationshipVectorAggregateResult
	OK       bool
	Requests []LegacyDirectRelationshipVectorAggregateRequest
}

func (s *fakeRelationshipAggregateRemoteSource) ReadRelationshipVectorAggregate(_ context.Context, request LegacyDirectRelationshipVectorAggregateRequest) (LegacyDirectRelationshipVectorAggregateResult, qsbridge.DiagnosticSet, bool, error) {
	s.Requests = append(s.Requests, request)
	return s.Result, nil, s.OK, nil
}
