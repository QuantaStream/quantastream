package qsbridge

import "testing"

func TestExecutionRequestEmptyResultBuildsQueryEnvelope(t *testing.T) {
	request := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "req-1"},
		Result:  ResultShape{Kind: ResultQuery},
		ResultColumns: []ResultColumn{{
			Name: "o_orderkey",
			Type: DataTypeInt,
		}},
	}

	result := request.EmptyResult()
	if result.RequestID != "req-1" || result.Kind != ResultQuery {
		t.Fatalf("result request/kind = %q/%q, want req-1/query", result.RequestID, result.Kind)
	}
	if result.Complete {
		t.Fatalf("query empty result should not be complete before row chunks")
	}
	if len(result.Columns) != 1 || result.Columns[0].Name != "o_orderkey" {
		t.Fatalf("columns = %#v, want o_orderkey", result.Columns)
	}
}

func TestExecutionRequestEmptyResultBuildsStatementEnvelope(t *testing.T) {
	request := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "stmt-1"},
		Result:  ResultShape{Kind: ResultStatement},
		Statement: StatementResult{
			AffectedRows: 3,
			Status:       "Rows matched: 3",
		},
		SessionActions: []SessionAction{{
			Kind:  SessionActionUseSchema,
			Value: "analytics",
		}},
	}

	result := request.EmptyResult()
	if result.Kind != ResultStatement || !result.Complete {
		t.Fatalf("result kind/complete = %q/%v, want complete statement", result.Kind, result.Complete)
	}
	if result.Statement.AffectedRows != 3 || result.Statement.Status != "Rows matched: 3" {
		t.Fatalf("statement = %#v, want OK metadata", result.Statement)
	}
	if len(result.SessionActions) != 1 || result.SessionActions[0].Value != "analytics" {
		t.Fatalf("session actions = %#v, want analytics", result.SessionActions)
	}
}

func TestExecutionResultWithChunkAppendsRowsAndCompletesOnFinal(t *testing.T) {
	result := ExecutionResult{Kind: ResultQuery}
	result = result.WithChunk(ResultChunk{
		Sequence: 1,
		Rows: []ResultRow{
			{{Kind: ValueInt, Value: 1}},
			{{Kind: ValueInt, Value: 2}},
		},
	})
	if result.RowsReturned != 2 || result.Complete {
		t.Fatalf("rows/complete = %d/%v, want 2/incomplete", result.RowsReturned, result.Complete)
	}

	result = result.WithChunk(ResultChunk{Sequence: 2, Final: true})
	if result.RowsReturned != 2 || !result.Complete {
		t.Fatalf("rows/complete = %d/%v, want 2/complete", result.RowsReturned, result.Complete)
	}
}

func TestExecutionResultWithProjectedRowSetAppendsChunk(t *testing.T) {
	result := ExecutionResult{Kind: ResultQuery}
	result = result.WithProjectedRowSet(QuantaProjectedRowSet{
		Index:   "orders",
		Rownums: []QuantaRownum{1001, 1002},
		ProjectionVectors: []QuantaProjectionVector{{
			Field: QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: DataTypeInt, Visible: true},
			Values: []ResultCell{
				{Kind: ValueInt, Value: int64(1001)},
				{Kind: ValueInt, Value: int64(1002)},
			},
		}},
	}, 7, true)

	if result.Status != ExecutionComplete || !result.Complete {
		t.Fatalf("status/complete = %q/%v, want complete/true", result.Status, result.Complete)
	}
	if result.RowsReturned != 2 {
		t.Fatalf("rows returned = %d, want 2", result.RowsReturned)
	}
	if len(result.Chunks) != 1 || result.Chunks[0].Sequence != 7 || !result.Chunks[0].Final {
		t.Fatalf("chunks = %#v, want one final sequence 7 chunk", result.Chunks)
	}
	if got := result.Chunks[0].Rows[1][0].Value; got != int64(1002) {
		t.Fatalf("row value = %v, want 1002", got)
	}
}

func TestExecutionResultWithProjectedRowSetFailsInvalidShape(t *testing.T) {
	result := ExecutionResult{Kind: ResultQuery}
	result = result.WithProjectedRowSet(QuantaProjectedRowSet{
		Index:   "orders",
		Rownums: []QuantaRownum{1001, 1002},
		ProjectionVectors: []QuantaProjectionVector{{
			Field: QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: DataTypeInt, Visible: true},
			Values: []ResultCell{
				{Kind: ValueInt, Value: int64(1001)},
			},
		}},
	}, 1, false)

	if result.Status != ExecutionFailed || !result.Complete {
		t.Fatalf("status/complete = %q/%v, want failed/true", result.Status, result.Complete)
	}
	if len(result.Chunks) != 0 || result.RowsReturned != 0 {
		t.Fatalf("chunks/rows = %d/%d, want 0/0", len(result.Chunks), result.RowsReturned)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocking shape diagnostic", result.Diagnostics)
	}
}

func TestExecutionResultCopiesMutableMetadata(t *testing.T) {
	request := ExecutionRequest{
		Result:        ResultShape{Kind: ResultStatement},
		ResultColumns: []ResultColumn{{Name: "original"}},
		Statement: StatementResult{
			SessionActions: []SessionAction{{Kind: SessionActionSetVariable, Name: "autocommit", Value: "1"}},
		},
		SessionActions: []SessionAction{{Kind: SessionActionSetVariable, Name: "autocommit", Value: "1"}},
	}

	result := request.EmptyResult()
	result.Columns[0].Name = "mutated"
	result.SessionActions[0].Value = "0"
	result.Statement.SessionActions[0].Value = "0"
	if request.ResultColumns[0].Name != "original" {
		t.Fatalf("request columns were mutated: %#v", request.ResultColumns)
	}
	if request.SessionActions[0].Value != "1" || request.Statement.SessionActions[0].Value != "1" {
		t.Fatalf("request session actions were mutated")
	}

	result = result.WithChunk(ResultChunk{Rows: []ResultRow{{{Kind: ValueString, Value: "original"}}}})
	result.Chunks[0].Rows[0][0].Value = "mutated"
	second := ExecutionResult{}.WithChunk(ResultChunk{Rows: []ResultRow{{{Kind: ValueString, Value: "original"}}}})
	if second.Chunks[0].Rows[0][0].Value != "original" {
		t.Fatalf("chunk clone leaked mutation")
	}
}

func TestExecutionRequestEmptyResultCarriesDiagnostics(t *testing.T) {
	request := ExecutionRequest{
		Result: ResultShape{Kind: ResultStatement},
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "bad option"),
		},
	}

	result := request.EmptyResult()
	if result.Supported() || result.Complete {
		t.Fatalf("result supported/complete = %v/%v, want false/false", result.Supported(), result.Complete)
	}
	if got := result.Diagnostics.Codes()[0]; got != DiagnosticInvalidExecutionOption {
		t.Fatalf("diagnostic code = %q, want %q", got, DiagnosticInvalidExecutionOption)
	}
}

func TestExecutionResultTracksFailureAndProtocolErrors(t *testing.T) {
	result := ExecutionResult{
		Status: ExecutionFailed,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticParameterTypeMismatch, PhaseBind, "bad parameter"),
		},
	}
	if result.Supported() {
		t.Fatalf("result supported = true, want false")
	}
	errors := result.ProtocolErrors()
	if len(errors) != 1 {
		t.Fatalf("protocol errors = %#v, want one error", errors)
	}
	protocol, ok := result.FirstProtocolError()
	if !ok {
		t.Fatalf("expected first protocol error")
	}
	if protocol.SQLState != SQLStateInvalidParameter || protocol.VendorCode != mysqlErrorInvalidParameter {
		t.Fatalf("protocol = %#v, want invalid parameter", protocol)
	}
}

func TestBatchExecutionRequestEmptyResultBuildsBatchEnvelope(t *testing.T) {
	request := BatchExecutionRequest{
		Options:       ExecutionOptions{RequestID: "batch-1"},
		Result:        ResultShape{Kind: ResultStatement},
		ParameterSets: []ParameterBindingSet{{}, {}},
		SessionActions: []SessionAction{{
			Kind:  SessionActionSetVariable,
			Name:  "autocommit",
			Value: "1",
		}},
	}

	result := request.EmptyResult()
	if result.RequestID != "batch-1" || result.Kind != ResultStatement {
		t.Fatalf("result request/kind = %q/%q, want batch-1/statement", result.RequestID, result.Kind)
	}
	if result.Status != ExecutionPending || result.Complete {
		t.Fatalf("status/complete = %q/%v, want pending/incomplete", result.Status, result.Complete)
	}
	if len(result.SessionActions) != 1 || result.SessionActions[0].Name != "autocommit" {
		t.Fatalf("session actions = %#v, want copied action", result.SessionActions)
	}
}

func TestBatchExecutionResultWithItemAggregatesRowsAndAffectedRows(t *testing.T) {
	result := BatchExecutionResult{Status: ExecutionPending}
	first := ExecutionResult{
		Status:       ExecutionComplete,
		RowsReturned: 2,
		Statement:    StatementResult{AffectedRows: 3},
		SessionActions: []SessionAction{{
			Kind:  SessionActionSetVariable,
			Name:  "last_insert_id",
			Value: "3",
		}},
	}
	second := ExecutionResult{
		Status:       ExecutionComplete,
		RowsReturned: 1,
		Statement:    StatementResult{AffectedRows: 4},
	}

	result = result.WithItem(first).WithItem(second)
	if result.RowsReturned != 3 || result.RowsAffected != 7 {
		t.Fatalf("rows returned/affected = %d/%d, want 3/7", result.RowsReturned, result.RowsAffected)
	}
	if len(result.Items) != 2 || len(result.SessionActions) != 1 {
		t.Fatalf("items/actions = %d/%d, want 2/1", len(result.Items), len(result.SessionActions))
	}
	result = result.WithComplete()
	if result.Status != ExecutionComplete || !result.Complete {
		t.Fatalf("status/complete = %q/%v, want complete/true", result.Status, result.Complete)
	}
}

func TestBatchExecutionResultTracksFailureAndProtocolErrors(t *testing.T) {
	result := BatchExecutionResult{}.WithItem(ExecutionResult{
		Status: ExecutionFailed,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticParameterTypeMismatch, PhaseBind, "bad parameter"),
		},
	})
	if result.Status != ExecutionFailed || !result.Complete || result.Supported() {
		t.Fatalf("status/complete/supported = %q/%v/%v, want failed/true/false", result.Status, result.Complete, result.Supported())
	}
	protocol, ok := result.FirstProtocolError()
	if !ok {
		t.Fatalf("expected protocol error")
	}
	if protocol.SQLState != SQLStateInvalidParameter || protocol.VendorCode != mysqlErrorInvalidParameter {
		t.Fatalf("protocol = %#v, want invalid parameter", protocol)
	}
}

func TestBatchExecutionResultWithCancellationUsesBatchCancellation(t *testing.T) {
	cancel := CancellationRequest{
		RequestID: "batch-1",
		Reason:    CancellationClientRequest,
	}
	result := BatchExecutionResult{}.WithCancellation(cancel)
	if result.Status != ExecutionCanceled || !result.Complete {
		t.Fatalf("status/complete = %q/%v, want canceled/complete", result.Status, result.Complete)
	}
	if result.Cancellation.RequestID != "batch-1" {
		t.Fatalf("cancellation = %#v, want request id batch-1", result.Cancellation)
	}
}

func TestBatchExecutionResultCopiesMutableItemMetadata(t *testing.T) {
	item := ExecutionResult{
		Columns: []ResultColumn{{Name: "original"}},
		Chunks: []ResultChunk{{
			Rows: []ResultRow{{{Kind: ValueString, Value: "original"}}},
		}},
		SessionActions: []SessionAction{{Kind: SessionActionUseSchema, Value: "analytics"}},
	}

	result := BatchExecutionResult{}.WithItem(item)
	result.Items[0].Columns[0].Name = "mutated"
	result.Items[0].Chunks[0].Rows[0][0].Value = "mutated"
	result.Items[0].SessionActions[0].Value = "mutated"
	if item.Columns[0].Name != "original" {
		t.Fatalf("item columns were mutated: %#v", item.Columns)
	}
	if item.Chunks[0].Rows[0][0].Value != "original" {
		t.Fatalf("item chunks were mutated: %#v", item.Chunks)
	}
	if item.SessionActions[0].Value != "analytics" {
		t.Fatalf("item session actions were mutated: %#v", item.SessionActions)
	}
}
