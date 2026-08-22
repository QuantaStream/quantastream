package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// DirectSessionHandle is a table-scoped direct execution handle.
type DirectSessionHandle interface {
	QueryBitmap(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error)
	ExecuteMutation(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error)
	InsertRows(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error)
	Release(ctx context.Context) qsbridge.DiagnosticSet
}

// DirectCandidateBitmapSessionHandle optionally evaluates a bitmap query against
// an already-narrowed candidate set without expanding the full bitmap first.
type DirectCandidateBitmapSessionHandle interface {
	QueryBitmapWithCandidateSet(ctx context.Context, request ExecutionRequest, candidates qsbridge.QuantaCandidateSet) (BitmapQueryResult, qsbridge.DiagnosticSet, bool, error)
}

// DirectCountOnlyBitmapSessionHandle optionally serves bitmap cardinality
// without expanding the result bitmap into rownums.
type DirectCountOnlyBitmapSessionHandle interface {
	QueryBitmapCountOnly(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error)
}

// DirectStringSearchSessionHandle optionally exposes the string-search index
// through a borrowed direct session for SQL preflight materialization.
type DirectStringSearchSessionHandle interface {
	SearchStringIndex(ctx context.Context, terms string) (map[uint64]struct{}, qsbridge.DiagnosticSet, error)
}

// DirectSessionProvider borrows table-scoped direct execution handles.
type DirectSessionProvider interface {
	BorrowDirectSession(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error)
}

// DirectTimeBucketBoundsProvider optionally reports observed time-bucket bounds
// for a physical timestamp field. Runtimes use this only as an optimization;
// absence must preserve the conservative full-window behavior.
type DirectTimeBucketBoundsProvider interface {
	TimeBucketYearBounds(ctx context.Context, request ExecutionRequest, field qsbridge.FieldRef) (int, int, bool)
}

// DirectSessionProviderFunc adapts a function to DirectSessionProvider.
type DirectSessionProviderFunc func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error)

// BorrowDirectSession calls f(ctx, request).
func (f DirectSessionProviderFunc) BorrowDirectSession(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
	return f(ctx, request)
}

// DirectSessionHandleFunc adapts functions to DirectSessionHandle for tests and simple adapters.
type DirectSessionHandleFunc struct {
	QueryFunc          func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error)
	CandidateQueryFunc func(ctx context.Context, request ExecutionRequest, candidates qsbridge.QuantaCandidateSet) (BitmapQueryResult, qsbridge.DiagnosticSet, bool, error)
	MutationFunc       func(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error)
	InsertFunc         func(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error)
	SearchFunc         func(ctx context.Context, terms string) (map[uint64]struct{}, qsbridge.DiagnosticSet, error)
	ReleaseFunc        func(ctx context.Context) qsbridge.DiagnosticSet
}

// QueryBitmap calls h.QueryFunc when present.
func (h DirectSessionHandleFunc) QueryBitmap(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	if h.QueryFunc == nil {
		return BitmapQueryResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"direct session handle has no bitmap query function",
			),
		}, nil
	}
	return h.QueryFunc(ctx, request)
}

// QueryBitmapWithCandidateSet calls h.CandidateQueryFunc when present.
func (h DirectSessionHandleFunc) QueryBitmapWithCandidateSet(ctx context.Context, request ExecutionRequest, candidates qsbridge.QuantaCandidateSet) (BitmapQueryResult, qsbridge.DiagnosticSet, bool, error) {
	if h.CandidateQueryFunc == nil {
		return BitmapQueryResult{}, nil, false, nil
	}
	return h.CandidateQueryFunc(ctx, request, candidates)
}

// ExecuteMutation calls h.MutationFunc when present and falls back to INSERT compatibility.
func (h DirectSessionHandleFunc) ExecuteMutation(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.MutationFunc != nil {
		return h.MutationFunc(ctx, request)
	}
	if request.Mutation.Kind == qsbridge.MutationInsert {
		return h.InsertRows(ctx, request)
	}
	return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedMutation,
			qsbridge.PhaseExecute,
			"direct session handle has no mutation function",
		),
	}, nil
}

// InsertRows calls h.InsertFunc when present.
func (h DirectSessionHandleFunc) InsertRows(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.InsertFunc == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"direct session handle has no insert function",
			),
		}, nil
	}
	return h.InsertFunc(ctx, request)
}

// SearchStringIndex calls h.SearchFunc when present.
func (h DirectSessionHandleFunc) SearchStringIndex(ctx context.Context, terms string) (map[uint64]struct{}, qsbridge.DiagnosticSet, error) {
	if h.SearchFunc == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"direct session handle has no string search function",
			),
		}, nil
	}
	return h.SearchFunc(ctx, terms)
}

// Release calls h.ReleaseFunc when present.
func (h DirectSessionHandleFunc) Release(ctx context.Context) qsbridge.DiagnosticSet {
	if h.ReleaseFunc == nil {
		return nil
	}
	return h.ReleaseFunc(ctx)
}
