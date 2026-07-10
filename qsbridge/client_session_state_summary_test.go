package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientSessionStateChangesReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityStatementResults,
		ProtocolCapabilitySessionActions,
	)
	response := StatementResult{
		SessionActions: []SessionAction{
			{Kind: SessionActionUseSchema, Value: "analytics"},
			{Kind: SessionActionSetVariable, Name: "autocommit", Value: "0"},
			{Kind: SessionActionSetSQLMode, Value: "ANSI_QUOTES"},
			{Kind: SessionActionSetTimeZone, Value: "UTC"},
			{Kind: SessionActionBeginTransaction},
			{Kind: SessionActionResetConnection},
			{Kind: SessionActionChangeUser, Value: "reporting"},
		},
	}.ProtocolStatementResponse(connection.Protocol)

	exchange := service.SummarizeClientSessionStateChanges(connection, response)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported session-state summary", exchange)
	}
	row := exchange.Row
	if row.ChangeCount != 7 || row.SchemaChangeCount != 1 || row.VariableChangeCount != 3 {
		t.Fatalf("row = %#v, want session-state change counts", row)
	}
	if row.TransactionCount != 1 || row.ResetConnectionCount != 1 || row.ChangeUserCount != 1 {
		t.Fatalf("row = %#v, want transaction and general action counts", row)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 6 {
		t.Fatalf("result/schema = %#v/%#v, want session-state summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 7 || resultRow[2].Value != 3 || resultRow[4].Value != 1 {
		t.Fatalf("result row = %#v, want session-state summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientSessionStateChangesSupportsEmptyResponse(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	exchange := service.SummarizeClientSessionStateChanges(connection, ProtocolStatementResponse{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported empty session-state summary", exchange)
	}
	if exchange.Row.ChangeCount != 0 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("row/result = %#v/%#v, want empty session-state summary row", exchange.Row, exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientSessionStateChangesRequiresSessionActionCapability(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	response := StatementResult{
		SessionActions: []SessionAction{{Kind: SessionActionUseSchema, Value: "analytics"}},
	}.ProtocolStatementResponse(NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions))

	exchange := service.SummarizeClientSessionStateChanges(connection, response)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing session-action capability diagnostic", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	if exchange.Row.ChangeCount != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 6 {
		t.Fatalf("row/result/schema = %#v/%#v/%#v, want failed summary envelope", exchange.Row, exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientSessionStateChangesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	response := StatementResult{
		SessionActions: []SessionAction{{Kind: SessionActionSetTimeZone, Value: "UTC"}},
	}.ProtocolStatementResponse(connection.Protocol)

	exchange := service.SummarizeClientSessionStateChanges(connection, response)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Response.SessionActions[0].Value = "mutated"
	exchange.Row.ChangeCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientSessionStateChanges(connection, response)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Response.SessionActions[0].Value != "UTC" {
		t.Fatalf("response metadata leaked mutation: %#v", again.Response.SessionActions)
	}
	if again.Row.ChangeCount != 1 || again.Row.VariableChangeCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("session-state summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}
