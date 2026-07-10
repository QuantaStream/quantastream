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

// DirectSessionProvider borrows table-scoped direct execution handles.
type DirectSessionProvider interface {
	BorrowDirectSession(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error)
}

// DirectSessionProviderFunc adapts a function to DirectSessionProvider.
type DirectSessionProviderFunc func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error)

// BorrowDirectSession calls f(ctx, request).
func (f DirectSessionProviderFunc) BorrowDirectSession(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
	return f(ctx, request)
}

// DirectSessionHandleFunc adapts functions to DirectSessionHandle for tests and simple adapters.
type DirectSessionHandleFunc struct {
	QueryFunc    func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error)
	MutationFunc func(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error)
	InsertFunc   func(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error)
	ReleaseFunc  func(ctx context.Context) qsbridge.DiagnosticSet
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

// Release calls h.ReleaseFunc when present.
func (h DirectSessionHandleFunc) Release(ctx context.Context) qsbridge.DiagnosticSet {
	if h.ReleaseFunc == nil {
		return nil
	}
	return h.ReleaseFunc(ctx)
}
