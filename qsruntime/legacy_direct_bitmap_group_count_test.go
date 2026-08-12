package qsruntime

import (
	"context"
	"math/big"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestLegacyDirectBitmapGroupCountReaderUsesCountOnlyQueries(t *testing.T) {
	sessions := &fakeLegacyDirectBitmapGroupCountSessions{
		Counts: map[string]uint64{
			"1/10": 2,
			"2/10": 1,
		},
	}
	reader := LegacyDirectBitmapGroupCountReader{
		Sessions:   sessions,
		TableCache: testBitmapGroupAggregateTableCache(),
	}
	result, diagnostics, ok, err := reader.ReadBitmapGroupCounts(context.Background(), BitmapGroupCountReadRequest{
		Index: "lineitem",
		Fields: []qsbridge.FieldRef{
			testBitmapGroupAggregateStringEnumField("l_returnflag"),
			testBitmapGroupAggregateStringEnumField("l_linestatus"),
		},
		BaseQuery: qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}}},
		CandidateRows: []qsbridge.QuantaRownum{101, 102, 201},
	})
	if err != nil || diagnostics.BlocksNative() || !ok {
		t.Fatalf("ReadBitmapGroupCounts ok/error/diagnostics = %t/%v/%v, want true/nil/none", ok, err, diagnostics)
	}
	if got, want := result.Mode, "legacy_direct_bitmap_count_only"; got != want {
		t.Fatalf("mode = %q, want %q", got, want)
	}
	if got, want := len(sessions.Requests), 4; got != want {
		t.Fatalf("count-only queries = %d, want %d", got, want)
	}
	for _, request := range sessions.Requests {
		if got, want := len(request.Query.Fragments), 3; got != want {
			t.Fatalf("fragments = %d, want base filter plus 2 group fragments in %#v", got, request.Query.Fragments)
		}
	}
	assertBitmapGroupCountGroup(t, result.Groups, "R\x00F", 2)
	assertBitmapGroupCountGroup(t, result.Groups, "A\x00F", 1)
	if got, want := len(result.Groups), 2; got != want {
		t.Fatalf("groups = %d, want zero-count combinations skipped leaving %d", got, want)
	}
}

func TestLegacyDirectBitmapGroupCountReaderDeclinesWithoutCountOnlySession(t *testing.T) {
	reader := LegacyDirectBitmapGroupCountReader{
		Sessions: DirectSessionProviderFunc(func(context.Context, ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{}, nil, nil
		}),
		TableCache: testBitmapGroupAggregateTableCache(),
	}
	_, diagnostics, ok, err := reader.ReadBitmapGroupCounts(context.Background(), BitmapGroupCountReadRequest{
		Index:  "lineitem",
		Fields: []qsbridge.FieldRef{testBitmapGroupAggregateStringEnumField("l_returnflag")},
	})
	if err != nil || diagnostics.BlocksNative() || ok {
		t.Fatalf("ReadBitmapGroupCounts ok/error/diagnostics = %t/%v/%v, want false/nil/none", ok, err, diagnostics)
	}
}

func TestLegacyDirectBitmapGroupCountReaderDeclinesFilteredBaseQuery(t *testing.T) {
	sessions := &fakeLegacyDirectBitmapGroupCountSessions{Counts: map[string]uint64{"1": 2}}
	reader := LegacyDirectBitmapGroupCountReader{
		Sessions:   sessions,
		TableCache: testBitmapGroupAggregateTableCache(),
	}
	_, diagnostics, ok, err := reader.ReadBitmapGroupCounts(context.Background(), BitmapGroupCountReadRequest{
		Index:  "lineitem",
		Fields: []qsbridge.FieldRef{testBitmapGroupAggregateStringEnumField("l_returnflag")},
		BaseQuery: qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_shipdate",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpLT,
			Value:     big.NewInt(100),
		}}},
	})
	if err != nil || diagnostics.BlocksNative() || ok {
		t.Fatalf("ReadBitmapGroupCounts ok/error/diagnostics = %t/%v/%v, want false/nil/none", ok, err, diagnostics)
	}
	if len(sessions.Requests) != 0 {
		t.Fatalf("count-only requests = %d, want filtered query to decline before storage", len(sessions.Requests))
	}
}

type fakeLegacyDirectBitmapGroupCountSessions struct {
	Counts   map[string]uint64
	Requests []ExecutionRequest
}

func (s *fakeLegacyDirectBitmapGroupCountSessions) BorrowDirectSession(context.Context, ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
	return &fakeLegacyDirectBitmapGroupCountHandle{Sessions: s}, nil, nil
}

type fakeLegacyDirectBitmapGroupCountHandle struct {
	Sessions *fakeLegacyDirectBitmapGroupCountSessions
}

func (h *fakeLegacyDirectBitmapGroupCountHandle) QueryBitmap(context.Context, ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	return BitmapQueryResult{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "unexpected rownum bitmap query"),
	}, nil
}

func (h *fakeLegacyDirectBitmapGroupCountHandle) QueryBitmapCountOnly(_ context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	h.Sessions.Requests = append(h.Sessions.Requests, request)
	return BitmapQueryResult{Success: true, Count: h.Sessions.Counts[fakeBitmapGroupAggregateKey(request)]}, nil, nil
}

func (h *fakeLegacyDirectBitmapGroupCountHandle) ExecuteMutation(context.Context, ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	return qsbridge.StatementResult{}, nil, nil
}

func (h *fakeLegacyDirectBitmapGroupCountHandle) InsertRows(context.Context, ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	return qsbridge.StatementResult{}, nil, nil
}

func (h *fakeLegacyDirectBitmapGroupCountHandle) Release(context.Context) qsbridge.DiagnosticSet {
	return nil
}

func assertBitmapGroupCountGroup(t *testing.T, groups []BitmapGroupCountReadGroup, key string, count uint64) {
	t.Helper()
	for _, group := range groups {
		if group.Key == key {
			if group.Count != count {
				t.Fatalf("group %q count = %d, want %d", key, group.Count, count)
			}
			return
		}
	}
	t.Fatalf("missing group %q in %#v", key, groups)
}
