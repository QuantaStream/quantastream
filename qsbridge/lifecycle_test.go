package qsbridge

import "testing"

func TestExecutionRequestCancelRequestRequiresCancelableRequestID(t *testing.T) {
	request := ExecutionRequest{
		Options: ExecutionOptions{
			RequestID:  "req-1",
			Cancelable: true,
		},
	}

	cancel := request.CancelRequest(CancellationClientRequest, "mysql kill query")
	if !cancel.Supported() {
		t.Fatalf("cancel = %#v, want supported request", cancel)
	}
	if cancel.RequestID != "req-1" || cancel.Reason != CancellationClientRequest {
		t.Fatalf("cancel = %#v, want request id and reason", cancel)
	}
}

func TestExecutionRequestCancelRequestReportsInvalidMetadata(t *testing.T) {
	cancel := ExecutionRequest{}.CancelRequest("", "")
	if cancel.Supported() {
		t.Fatalf("expected unsupported cancellation request")
	}
	if cancel.Reason != CancellationClientRequest {
		t.Fatalf("reason = %q, want default client request", cancel.Reason)
	}
	if !containsDiagnosticCode(cancel.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", cancel.Diagnostics.Codes())
	}
}

func TestBatchExecutionRequestCancelRequestUsesBatchOptions(t *testing.T) {
	request := BatchExecutionRequest{
		Options: ExecutionOptions{
			RequestID:  "batch-1",
			Cancelable: true,
		},
	}

	cancel := request.CancelRequest(CancellationTimeout, "deadline exceeded")
	if !cancel.Supported() {
		t.Fatalf("cancel = %#v, want supported batch cancellation", cancel)
	}
	if cancel.RequestID != "batch-1" || cancel.Reason != CancellationTimeout {
		t.Fatalf("cancel = %#v, want batch request id and timeout reason", cancel)
	}
}

func TestExecutionResultStatusTracksChunksAndCancellation(t *testing.T) {
	request := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "req-1", Cancelable: true},
		Result:  ResultShape{Kind: ResultQuery},
	}
	result := request.EmptyResult()
	if result.Status != ExecutionPending {
		t.Fatalf("status = %q, want pending", result.Status)
	}

	result = result.WithChunk(ResultChunk{Rows: []ResultRow{{{Kind: ValueInt, Value: 1}}}})
	if result.Status != ExecutionStreaming || result.Complete {
		t.Fatalf("status/complete = %q/%v, want streaming/incomplete", result.Status, result.Complete)
	}

	cancel := request.CancelRequest(CancellationClientRequest, "stop")
	result = result.WithCancellation(cancel)
	if result.Status != ExecutionCanceled || !result.Complete {
		t.Fatalf("status/complete = %q/%v, want canceled/complete", result.Status, result.Complete)
	}
	if result.Cancellation.RequestID != "req-1" {
		t.Fatalf("cancellation = %#v, want request id req-1", result.Cancellation)
	}
}

func TestExecutionResultStatusTracksFailureAndFinalChunk(t *testing.T) {
	failed := ExecutionRequest{
		Result: ResultShape{Kind: ResultStatement},
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "bad option"),
		},
	}.EmptyResult()
	if failed.Status != ExecutionFailed || failed.Complete {
		t.Fatalf("failed status/complete = %q/%v, want failed/incomplete", failed.Status, failed.Complete)
	}

	result := ExecutionResult{Kind: ResultQuery}.WithChunk(ResultChunk{Final: true})
	if result.Status != ExecutionComplete || !result.Complete {
		t.Fatalf("final status/complete = %q/%v, want complete/true", result.Status, result.Complete)
	}
}
