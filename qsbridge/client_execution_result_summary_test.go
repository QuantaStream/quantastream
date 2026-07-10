package qsbridge

import "testing"

func TestPlanningServiceListClientExecutionResultSummaryReturnsRow(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	execution := ExecutionResult{
		RequestID:    "result-1",
		Status:       ExecutionStreaming,
		Kind:         ResultQuery,
		Columns:      []ResultColumn{{Name: "o_orderkey", Type: DataTypeInt}},
		Complete:     false,
		RowsReturned: 2,
		Statement:    StatementResult{AffectedRows: 3},
		Chunks: []ResultChunk{{
			Sequence: 1,
			Rows: []ResultRow{
				{metadataIntCell(1)},
				{metadataIntCell(2)},
			},
		}},
		SessionActions: []SessionAction{{
			Kind:  SessionActionSetVariable,
			Name:  "last_insert_id",
			Value: "3",
		}},
		Cursor: CursorDescriptor{
			ID:    "result-1",
			State: CursorStateOpen,
		},
		Profile: ExecutionProfile{
			RequestID:      "result-1",
			AccessIntent:   PhysicalAccessRead,
			Lifecycle:      ClientPlanLifecycleSelect,
			LifecycleSteps: 7,
			IncludeProfile: true,
		},
	}

	exchange := service.ListClientExecutionResultSummary(connection, execution)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported execution result summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one execution result row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.RequestID != "result-1" || row.Kind != ResultQuery || row.Status != ExecutionStreaming {
		t.Fatalf("row = %#v, want result identity", row)
	}
	if row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want profile lifecycle metadata", row)
	}
	if row.RowsReturned != 2 || row.AffectedRows != 3 || row.ResultColumns != 1 || row.ResultChunks != 1 || row.SessionActions != 1 {
		t.Fatalf("row = %#v, want result counts", row)
	}
	if row.Cursor != "result-1" || row.CursorState != CursorStateOpen || !row.Profiled || row.Canceled {
		t.Fatalf("row = %#v, want cursor/profile state", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 17 {
		t.Fatalf("result/schema = %#v/%#v, want summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "result-1" || resultRow[3].Value != string(PhysicalAccessRead) || resultRow[7].Value != 2 || resultRow[10].Value != 1 || resultRow[15].Value != true {
		t.Fatalf("result row = %#v, want summary cells", resultRow)
	}
}

func TestPlanningServiceListClientExecutionResultSummaryCarriesDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	execution := ExecutionResult{
		RequestID: "result-1",
		Status:    ExecutionFailed,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseExecute, "executor failed"),
		},
	}

	exchange := service.ListClientExecutionResultSummary(connection, execution)
	if exchange.Supported() {
		t.Fatalf("expected execution diagnostics to block summary")
	}
	if len(exchange.Rows) != 1 || !containsDiagnosticCode(exchange.Rows[0].Diagnostics, DiagnosticInternalInvariant) {
		t.Fatalf("rows = %#v, want execution diagnostics", exchange.Rows)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed summary envelope", exchange.Result)
	}
}

func TestPlanningServiceListClientExecutionResultSummaryCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	execution := ExecutionResult{
		RequestID:    "result-1",
		Status:       ExecutionComplete,
		Kind:         ResultStatement,
		Complete:     true,
		Statement:    StatementResult{AffectedRows: 1},
		RowsReturned: 1,
		Columns:      []ResultColumn{{Name: "affected", Type: DataTypeInt}},
		Chunks: []ResultChunk{{
			Final: true,
			Rows:  []ResultRow{{metadataIntCell(1)}},
		}},
	}

	exchange := service.ListClientExecutionResultSummary(connection, execution)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Execution.RequestID = "mutated"
	exchange.Rows[0].RequestID = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientExecutionResultSummary(connection, execution)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Execution.RequestID != "result-1" || again.Rows[0].RequestID != "result-1" {
		t.Fatalf("execution summary leaked mutation: execution=%#v rows=%#v", again.Execution, again.Rows)
	}
	if again.Result.Columns[0].Name != "Request_id" || again.ResultSchema.Columns[0].Name != "Request_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "result-1" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
