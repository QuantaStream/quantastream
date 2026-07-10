package qsbridge

import "testing"

func TestExecutionRequestCursorDescriptorUsesForwardOnlyOptions(t *testing.T) {
	request := ExecutionRequest{
		Options: ExecutionOptions{
			RequestID: "req-1",
			Cursor:    CursorForwardOnly,
			BatchSize: 25,
			MaxRows:   100,
		},
		Result: ResultShape{Kind: ResultQuery},
	}

	cursor := request.CursorDescriptor()
	if !cursor.Open() {
		t.Fatalf("cursor = %#v, want open cursor", cursor)
	}
	if cursor.ID != "req-1" || cursor.Mode != CursorForwardOnly || cursor.BatchSize != 25 || cursor.MaxRows != 100 {
		t.Fatalf("cursor = %#v, want request-derived cursor metadata", cursor)
	}
}

func TestExecutionRequestCursorDescriptorUsesStreamingAsForwardOnlyIntent(t *testing.T) {
	request := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "stream-1", Streaming: true},
		Result:  ResultShape{Kind: ResultQuery},
	}

	cursor := request.CursorDescriptor()
	if cursor.Mode != CursorForwardOnly || cursor.ID != "stream-1" {
		t.Fatalf("cursor = %#v, want streaming forward-only cursor", cursor)
	}
}

func TestExecutionRequestCursorDescriptorIgnoresStatementAndUncursorredQuery(t *testing.T) {
	if cursor := (ExecutionRequest{Result: ResultShape{Kind: ResultStatement}}).CursorDescriptor(); cursor.State != CursorStateNone {
		t.Fatalf("statement cursor = %#v, want none", cursor)
	}
	if cursor := (ExecutionRequest{Result: ResultShape{Kind: ResultQuery}}).CursorDescriptor(); cursor.State != CursorStateNone {
		t.Fatalf("query cursor = %#v, want none without cursor intent", cursor)
	}
}

func TestExecutionResultCursorTracksChunkProgress(t *testing.T) {
	request := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "req-1", Cursor: CursorForwardOnly},
		Result:  ResultShape{Kind: ResultQuery},
	}
	result := request.EmptyResult()
	if !result.Cursor.Open() {
		t.Fatalf("cursor = %#v, want open result cursor", result.Cursor)
	}

	result = result.WithChunk(ResultChunk{
		Rows: []ResultRow{
			{{Kind: ValueInt, Value: 1}},
			{{Kind: ValueInt, Value: 2}},
		},
	})
	if result.Cursor.Position != 2 || result.Cursor.State != CursorStateOpen {
		t.Fatalf("cursor = %#v, want position 2/open", result.Cursor)
	}

	result = result.WithChunk(ResultChunk{Final: true})
	if result.Cursor.Position != 2 || result.Cursor.State != CursorStateExhausted {
		t.Fatalf("cursor = %#v, want position 2/exhausted", result.Cursor)
	}
}

func TestBatchExecutionRequestCursorDescriptorUsesOptions(t *testing.T) {
	request := BatchExecutionRequest{
		Options: ExecutionOptions{RequestID: "batch-1", Cursor: CursorForwardOnly},
		Result:  ResultShape{Kind: ResultQuery},
	}
	cursor := request.CursorDescriptor()
	if cursor.ID != "batch-1" || !cursor.Open() {
		t.Fatalf("cursor = %#v, want batch cursor", cursor)
	}
}
