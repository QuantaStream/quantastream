package qsbridge

import "testing"

func TestPlanningServiceListClientBatchResultSummaryReturnsItemRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{
		RequestID: "batch-1",
		Status:    ExecutionComplete,
		Kind:      ResultStatement,
		Complete:  true,
	}.WithItem(ExecutionResult{
		RequestID:    "item-1",
		Status:       ExecutionComplete,
		Kind:         ResultStatement,
		Complete:     true,
		RowsReturned: 2,
		Statement:    StatementResult{AffectedRows: 3},
		Profile: ExecutionProfile{
			AccessIntent:   PhysicalAccessRead,
			Lifecycle:      ClientPlanLifecycleSelect,
			LifecycleSteps: 7,
		},
		SessionActions: []SessionAction{{
			Kind:  SessionActionSetVariable,
			Name:  "last_insert_id",
			Value: "3",
		}},
	}).WithItem(ExecutionResult{
		RequestID:    "item-2",
		Status:       ExecutionComplete,
		Kind:         ResultStatement,
		Complete:     true,
		RowsReturned: 1,
		Statement:    StatementResult{AffectedRows: 4},
	})

	exchange := service.ListClientBatchResultSummary(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch result summary", exchange)
	}
	if len(exchange.Rows) != 2 {
		t.Fatalf("rows = %#v, want two batch items", exchange.Rows)
	}
	if exchange.Rows[0].Item != 0 || exchange.Rows[0].RequestID != "item-1" || exchange.Rows[0].AffectedRows != 3 || exchange.Rows[0].SessionActions != 1 {
		t.Fatalf("first row = %#v, want first item metadata", exchange.Rows[0])
	}
	if exchange.Rows[0].AccessIntent != PhysicalAccessRead || exchange.Rows[0].Lifecycle != ClientPlanLifecycleSelect || exchange.Rows[0].LifecycleSteps != 7 {
		t.Fatalf("first row = %#v, want first item lifecycle metadata", exchange.Rows[0])
	}
	if exchange.Rows[1].Item != 1 || exchange.Rows[1].RowsReturned != 1 || exchange.Rows[1].AffectedRows != 4 {
		t.Fatalf("second row = %#v, want second item metadata", exchange.Rows[1])
	}
	if len(exchange.ResultSchema.Columns) != 12 || exchange.Result.RowsReturned != 2 {
		t.Fatalf("result/schema = %#v/%#v, want batch summary rows", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[1].Value != "item-1" || resultRow[4].Value != string(PhysicalAccessRead) || resultRow[5].Value != string(ClientPlanLifecycleSelect) || resultRow[8].Value != 2 || resultRow[9].Value != 3 {
		t.Fatalf("result row = %#v, want first item counts", resultRow)
	}
}

func TestPlanningServiceListClientBatchResultSummaryCarriesDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Status:    ExecutionFailed,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticParameterTypeMismatch, PhaseBind, "bad parameter"),
		},
	})

	exchange := service.ListClientBatchResultSummary(connection, batch)
	if exchange.Supported() {
		t.Fatalf("expected batch diagnostics to block summary")
	}
	if len(exchange.Rows) != 1 || !containsDiagnosticCode(exchange.Rows[0].Diagnostics, DiagnosticParameterTypeMismatch) {
		t.Fatalf("rows = %#v, want item diagnostics", exchange.Rows)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed summary envelope", exchange.Result)
	}
}

func TestPlanningServiceListClientBatchResultSummaryCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	batch := BatchExecutionResult{}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Status:    ExecutionComplete,
		Kind:      ResultStatement,
		Complete:  true,
		Statement: StatementResult{AffectedRows: 1},
	})

	exchange := service.ListClientBatchResultSummary(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].RequestID = "mutated"
	exchange.Rows[0].RequestID = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.ListClientBatchResultSummary(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].RequestID != "item-1" || again.Rows[0].RequestID != "item-1" {
		t.Fatalf("batch summary leaked mutation: batch=%#v rows=%#v", again.Batch, again.Rows)
	}
	if again.Result.Columns[0].Name != "Item" || again.ResultSchema.Columns[0].Name != "Item" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != "item-1" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
