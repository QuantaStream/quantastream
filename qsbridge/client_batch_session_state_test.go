package qsbridge

import "testing"

func TestPlanningServiceListClientBatchSessionStateChangesReturnsRows(t *testing.T) {
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
		},
	}).WithItem(ExecutionResult{
		RequestID: "item-2",
		Statement: StatementResult{
			SessionActions: []SessionAction{
				{Kind: SessionActionSetSQLMode, Value: "ANSI_QUOTES"},
				{Kind: SessionActionBeginTransaction},
			},
		},
	})

	exchange := service.ListClientBatchSessionStateChanges(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch session-state metadata", exchange)
	}
	if len(exchange.Changes) != 4 {
		t.Fatalf("changes = %#v, want four changes", exchange.Changes)
	}
	if exchange.Changes[0].Item != 0 || exchange.Changes[0].RequestID != "item-1" || exchange.Changes[0].Kind != ClientSessionStateSchema || exchange.Changes[0].Value != "analytics" {
		t.Fatalf("first change = %#v, want item-1 database change", exchange.Changes[0])
	}
	if exchange.Changes[2].Item != 1 || exchange.Changes[2].Name != "sql_mode" || exchange.Changes[2].Value != "ANSI_QUOTES" {
		t.Fatalf("third change = %#v, want item-2 sql_mode change", exchange.Changes[2])
	}
	if exchange.Changes[3].Kind != ClientSessionStateTransaction || exchange.Changes[3].Value != "BEGIN" {
		t.Fatalf("fourth change = %#v, want begin transaction", exchange.Changes[3])
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 4 || len(exchange.ResultSchema.Columns) != 5 {
		t.Fatalf("result/schema = %#v/%#v, want batch session-state rows", exchange.Result, exchange.ResultSchema)
	}
	row := exchange.Result.Chunks[0].Rows[1]
	if row[0].Value != 0 || row[1].Value != "item-1" || row[3].Value != "autocommit" || row[4].Value != "0" {
		t.Fatalf("autocommit row = %#v, want item-1 system variable row", row)
	}
}

func TestPlanningServiceListClientBatchSessionStateChangesSupportsEmptyBatch(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	exchange := service.ListClientBatchSessionStateChanges(connection, BatchExecutionResult{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported empty batch session-state metadata", exchange)
	}
	if len(exchange.Changes) != 0 || exchange.Result.RowsReturned != 0 || len(exchange.Result.Chunks) != 1 {
		t.Fatalf("exchange/result = %#v/%#v, want empty session-state result", exchange, exchange.Result)
	}
}

func TestPlanningServiceListClientBatchSessionStateChangesRequiresSessionActionCapability(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID:      "item-1",
		SessionActions: []SessionAction{{Kind: SessionActionUseSchema, Value: "analytics"}},
	})

	exchange := service.ListClientBatchSessionStateChanges(connection, batch)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing session-action capability diagnostic", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	if len(exchange.Changes) != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("changes/result = %#v/%#v, want failed envelope without rows", exchange.Changes, exchange.Result)
	}
}

func TestPlanningServiceListClientBatchSessionStateChangesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID:      "item-1",
		SessionActions: []SessionAction{{Kind: SessionActionSetTimeZone, Value: "UTC"}},
	})

	exchange := service.ListClientBatchSessionStateChanges(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].RequestID = "mutated"
	exchange.Batch.Items[0].SessionActions[0].Value = "mutated"
	exchange.Changes[0].Value = "mutated"
	exchange.Result.Chunks[0].Rows[0][4].Value = "mutated"

	again := service.ListClientBatchSessionStateChanges(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].RequestID != "item-1" || again.Batch.Items[0].SessionActions[0].Value != "UTC" {
		t.Fatalf("batch session-state metadata leaked mutation: %#v", again.Batch)
	}
	if again.Changes[0].Value != "UTC" || again.Result.Chunks[0].Rows[0][4].Value != "UTC" {
		t.Fatalf("session-state rows leaked mutation: %#v/%#v", again.Changes, again.Result.Chunks)
	}
}
