package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestDirectSessionProviderFuncBorrowsSession(t *testing.T) {
	want := DirectSessionHandleFunc{}
	provider := DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
		if _, ok := request.RootIndex(); !ok {
			t.Fatalf("request root index not found")
		}
		return want, nil, nil
	})

	session, diagnostics, err := provider.BorrowDirectSession(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{Index: "orders"}},
	}))
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if session == nil {
		t.Fatalf("session = nil, want handle")
	}
}

func TestDirectSessionHandleFuncReportsMissingQueryFunc(t *testing.T) {
	_, diagnostics, err := DirectSessionHandleFunc{}.QueryBitmap(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("query bitmap: %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected missing query function diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

func TestDirectSessionHandleFuncReleaseIsOptional(t *testing.T) {
	if diagnostics := (DirectSessionHandleFunc{}).Release(context.Background()); diagnostics.BlocksNative() {
		t.Fatalf("unexpected release diagnostics: %#v", diagnostics)
	}
}
