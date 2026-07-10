package qsbridge

import "testing"

func TestPlanningServiceListClientSessionStateChangesReturnsRows(t *testing.T) {
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
			{Kind: SessionActionBeginTransaction},
		},
	}.ProtocolStatementResponse(connection.Protocol)

	exchange := service.ListClientSessionStateChanges(connection, response)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported session-state metadata", exchange)
	}
	if len(exchange.Changes) != 4 {
		t.Fatalf("changes = %#v, want four changes", exchange.Changes)
	}
	if exchange.Changes[0].Kind != ClientSessionStateSchema || exchange.Changes[0].Name != "database" || exchange.Changes[0].Value != "analytics" {
		t.Fatalf("schema change = %#v, want database change", exchange.Changes[0])
	}
	if exchange.Changes[2].Name != "sql_mode" || exchange.Changes[2].Value != "ANSI_QUOTES" {
		t.Fatalf("sql mode change = %#v, want sql_mode system variable", exchange.Changes[2])
	}
	if exchange.Changes[3].Kind != ClientSessionStateTransaction || exchange.Changes[3].Value != "BEGIN" {
		t.Fatalf("transaction change = %#v, want begin transaction", exchange.Changes[3])
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 4 || len(exchange.ResultSchema.Columns) != 3 {
		t.Fatalf("result/schema = %#v/%#v, want session-state rows", exchange.Result, exchange.ResultSchema)
	}
	if exchange.Result.Chunks[0].Rows[1][1].Value != "autocommit" || exchange.Result.Chunks[0].Rows[1][2].Value != "0" {
		t.Fatalf("autocommit row = %#v, want system variable row", exchange.Result.Chunks[0].Rows[1])
	}
}

func TestPlanningServiceListClientSessionStateChangesSupportsEmptyResponse(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	exchange := service.ListClientSessionStateChanges(connection, ProtocolStatementResponse{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported empty session-state metadata", exchange)
	}
	if len(exchange.Changes) != 0 || exchange.Result.RowsReturned != 0 || len(exchange.Result.Chunks) != 1 {
		t.Fatalf("exchange/result = %#v/%#v, want empty session-state result", exchange, exchange.Result)
	}
}

func TestPlanningServiceListClientSessionStateChangesRequiresSessionActionCapability(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	response := StatementResult{
		SessionActions: []SessionAction{{Kind: SessionActionUseSchema, Value: "analytics"}},
	}.ProtocolStatementResponse(NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions))

	exchange := service.ListClientSessionStateChanges(connection, response)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing session-action capability diagnostic", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed session-state envelope", exchange.Result)
	}
}

func TestPlanningServiceListClientSessionStateChangesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	response := StatementResult{
		SessionActions: []SessionAction{{Kind: SessionActionSetTimeZone, Value: "UTC"}},
	}.ProtocolStatementResponse(connection.Protocol)

	exchange := service.ListClientSessionStateChanges(connection, response)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Response.SessionActions[0].Value = "mutated"
	exchange.Changes[0].Value = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.ListClientSessionStateChanges(connection, response)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Response.SessionActions[0].Value != "UTC" || again.Changes[0].Value != "UTC" || again.Result.Chunks[0].Rows[0][2].Value != "UTC" {
		t.Fatalf("session-state metadata leaked mutation: %#v/%#v/%#v", again.Response.SessionActions, again.Changes, again.Result.Chunks)
	}
}
