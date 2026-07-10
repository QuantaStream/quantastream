package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientBatchSessionStateChangesReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityStatementResults,
		ProtocolCapabilitySessionActions,
	)
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		SessionActions: []SessionAction{
			{Kind: SessionActionUseSchema, Value: "analytics"},
			{Kind: SessionActionSetVariable, Name: "autocommit", Value: "0"},
			{Kind: SessionActionResetConnection},
		},
	}).WithItem(ExecutionResult{
		RequestID: "item-2",
		Statement: StatementResult{
			SessionActions: []SessionAction{
				{Kind: SessionActionSetSQLMode, Value: "ANSI_QUOTES"},
				{Kind: SessionActionSetTimeZone, Value: "UTC"},
				{Kind: SessionActionBeginTransaction},
				{Kind: SessionActionChangeUser, Value: "reporting"},
			},
		},
	}).WithItem(ExecutionResult{RequestID: "item-3"})

	exchange := service.SummarizeClientBatchSessionStateChanges(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch session-state summary", exchange)
	}
	row := exchange.Row
	if row.ItemCount != 3 || row.ChangedItemCount != 2 || row.ChangeCount != 7 {
		t.Fatalf("row = %#v, want item and change counts", row)
	}
	if row.SchemaChangeCount != 1 || row.VariableChangeCount != 3 || row.TransactionCount != 1 || row.ResetConnectionCount != 1 || row.ChangeUserCount != 1 {
		t.Fatalf("row = %#v, want session-state kind counts", row)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("result/schema = %#v/%#v, want batch session-state summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 3 || resultRow[1].Value != 2 || resultRow[2].Value != 7 || resultRow[4].Value != 3 {
		t.Fatalf("result row = %#v, want batch session-state summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientBatchSessionStateChangesSupportsEmptyBatch(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	exchange := service.SummarizeClientBatchSessionStateChanges(connection, BatchExecutionResult{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported empty batch session-state summary", exchange)
	}
	if exchange.Row.ItemCount != 0 || exchange.Row.ChangeCount != 0 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("row/result = %#v/%#v, want empty summary row", exchange.Row, exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientBatchSessionStateChangesRequiresSessionActionCapability(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID:      "item-1",
		SessionActions: []SessionAction{{Kind: SessionActionUseSchema, Value: "analytics"}},
	})

	exchange := service.SummarizeClientBatchSessionStateChanges(connection, batch)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing session-action capability diagnostic", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	if exchange.Row.ChangeCount != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("row/result/schema = %#v/%#v/%#v, want failed summary envelope", exchange.Row, exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientBatchSessionStateChangesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID:      "item-1",
		SessionActions: []SessionAction{{Kind: SessionActionSetTimeZone, Value: "UTC"}},
	})

	exchange := service.SummarizeClientBatchSessionStateChanges(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].RequestID = "mutated"
	exchange.Batch.Items[0].SessionActions[0].Value = "mutated"
	exchange.Row.ChangeCount = 99
	exchange.Result.Chunks[0].Rows[0][2].Value = 99

	again := service.SummarizeClientBatchSessionStateChanges(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].RequestID != "item-1" || again.Batch.Items[0].SessionActions[0].Value != "UTC" {
		t.Fatalf("batch metadata leaked mutation: %#v", again.Batch)
	}
	if again.Row.ChangeCount != 1 || again.Row.VariableChangeCount != 1 || again.Result.Chunks[0].Rows[0][2].Value != 1 {
		t.Fatalf("batch session-state summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}
