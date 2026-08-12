package qsruntime

import (
	"context"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestLegacyDirectSharedRelationshipSiblingDiversityReaderUsesWholeBucket(t *testing.T) {
	parentBSI := roaring64.NewDefaultBSI()
	valueBSI := roaring64.NewDefaultBSI()
	setBSIValue(parentBSI, 10, 100)
	setBSIValue(parentBSI, 11, 100)
	setBSIValue(parentBSI, 12, 100)
	setBSIValue(parentBSI, 20, 200)
	setBSIValue(parentBSI, 21, 200)
	setBSIValue(parentBSI, 30, 300)
	setBSIValue(valueBSI, 10, 1)
	setBSIValue(valueBSI, 11, 1)
	setBSIValue(valueBSI, 12, 2)
	setBSIValue(valueBSI, 20, 3)
	setBSIValue(valueBSI, 21, 3)
	setBSIValue(valueBSI, 30, 4)

	session := &fakeSiblingDiversitySession{
		Rows: []qsbridge.QuantaRownum{10, 11, 12, 20, 21, 30},
	}
	projection := &fakeSiblingDiversityProjectionSource{
		BSIByField: map[string]*roaring64.BSI{
			"l_orderkey": parentBSI,
			"l_suppkey":  valueBSI,
		},
	}
	reader := &LegacyDirectSharedRelationshipSiblingDiversityReader{
		Sessions: DirectSessionProviderFunc(func(context.Context, ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return session, nil, nil
		}),
		Projection: projection,
	}

	result, diagnostics, ok, err := reader.ReadRelationshipSiblingDiversityCandidates(context.Background(), RelationshipSiblingDiversityReadRequest{
		Index:         "lineitem",
		ParentField:   "l_orderkey",
		ValueField:    "l_suppkey",
		CandidateRows: []qsbridge.QuantaRownum{10, 20, 30},
	})
	if err != nil || diagnostics.BlocksNative() || !ok {
		t.Fatalf("ReadRelationshipSiblingDiversityCandidates ok/error/diagnostics = %t/%v/%v, want true/nil/none", ok, err, diagnostics)
	}
	if got, want := result.Mode, "shared_projection_sibling_diversity_build"; got != want {
		t.Fatalf("mode = %q, want %q", got, want)
	}
	if result.CacheHit {
		t.Fatalf("cache hit on initial read = true, want false")
	}
	if got, want := result.Candidates.Rownums, []qsbridge.QuantaRownum{10}; !reflect.DeepEqual(got, want) {
		t.Fatalf("candidate rows = %v, want %v", got, want)
	}
	if got, want := result.Rows, uint64(6); got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if got, want := result.Groups, uint64(3); got != want {
		t.Fatalf("groups = %d, want %d", got, want)
	}
	if got, want := result.DiverseGroups, uint64(1); got != want {
		t.Fatalf("diverse groups = %d, want %d", got, want)
	}
	if got, want := session.QueryCount, 1; got != want {
		t.Fatalf("query count = %d, want %d", got, want)
	}
	if got, want := len(projection.BatchRequests), 1; got != want {
		t.Fatalf("projection batch count = %d, want %d", got, want)
	}
	requests := projection.BatchRequests[0]
	if got, want := []string{requests[0].PhysicalField, requests[1].PhysicalField}, []string{"l_orderkey", "l_suppkey"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("projection fields = %v, want %v", got, want)
	}

	cached, diagnostics, ok, err := reader.ReadRelationshipSiblingDiversityCandidates(context.Background(), RelationshipSiblingDiversityReadRequest{
		Index:         "lineitem",
		ParentField:   "l_orderkey",
		ValueField:    "l_suppkey",
		CandidateRows: []qsbridge.QuantaRownum{11, 12, 20},
	})
	if err != nil || diagnostics.BlocksNative() || !ok {
		t.Fatalf("cached ReadRelationshipSiblingDiversityCandidates ok/error/diagnostics = %t/%v/%v, want true/nil/none", ok, err, diagnostics)
	}
	if got, want := cached.Candidates.Rownums, []qsbridge.QuantaRownum{11, 12}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cached candidate rows = %v, want %v", got, want)
	}
	if got, want := cached.Mode, "shared_projection_sibling_diversity_cache_hit"; got != want {
		t.Fatalf("cached mode = %q, want %q", got, want)
	}
	if !cached.CacheHit {
		t.Fatalf("cached cache hit = false, want true")
	}
	if got, want := session.QueryCount, 1; got != want {
		t.Fatalf("query count after cached read = %d, want %d", got, want)
	}
	if got, want := len(projection.BatchRequests), 1; got != want {
		t.Fatalf("projection batch count after cached read = %d, want %d", got, want)
	}
}

func setBSIValue(bsi *roaring64.BSI, row qsbridge.QuantaRownum, value int64) {
	bsi.SetValue(uint64(row), value)
}

type fakeSiblingDiversitySession struct {
	Rows       []qsbridge.QuantaRownum
	QueryCount int
	Released   bool
}

func (s *fakeSiblingDiversitySession) QueryBitmap(_ context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	s.QueryCount++
	if len(request.Query.Fragments) != 1 {
		return BitmapQueryResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "expected one sibling diversity parent fragment"),
		}, nil
	}
	fragment := request.Query.Fragments[0]
	if fragment.Index != "lineitem" || fragment.Field != "l_orderkey" || !fragment.NullCheck || !fragment.Negate {
		return BitmapQueryResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "unexpected sibling diversity parent fragment"),
		}, nil
	}
	return BitmapQueryResult{
		Rownums: append([]qsbridge.QuantaRownum(nil), s.Rows...),
		Count:   uint64(len(s.Rows)),
		Success: true,
	}, nil, nil
}

func (s *fakeSiblingDiversitySession) ExecuteMutation(context.Context, ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	return qsbridge.StatementResult{}, nil, nil
}

func (s *fakeSiblingDiversitySession) InsertRows(context.Context, ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	return qsbridge.StatementResult{}, nil, nil
}

func (s *fakeSiblingDiversitySession) Release(context.Context) qsbridge.DiagnosticSet {
	s.Released = true
	return nil
}

type fakeSiblingDiversityProjectionSource struct {
	BSIByField    map[string]*roaring64.BSI
	BatchRequests [][]NativeProjectionBSIReadRequest
}

func (s *fakeSiblingDiversityProjectionSource) ReadProjectionBSI(_ context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	return NativeProjectionBSIReadResult{BSI: s.BSIByField[request.PhysicalField]}, nil, nil
}

func (s *fakeSiblingDiversityProjectionSource) ReadProjectionBSIs(_ context.Context, requests []NativeProjectionBSIReadRequest) ([]NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	s.BatchRequests = append(s.BatchRequests, append([]NativeProjectionBSIReadRequest(nil), requests...))
	results := make([]NativeProjectionBSIReadResult, 0, len(requests))
	for _, request := range requests {
		results = append(results, NativeProjectionBSIReadResult{BSI: s.BSIByField[request.PhysicalField]})
	}
	return results, nil, nil
}
