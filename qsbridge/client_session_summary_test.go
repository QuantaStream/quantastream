package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientSessionActionsReturnsSummaryRow(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Session.CurrentSchema = "quanta"
	connection.Session.SQLModes = []SQLMode{"ANSI_QUOTES"}
	connection.Session.Variables = map[string]string{"autocommit": "1"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilitySessionActions)
	registry.Put(connection.Session)

	session := service.PrepareClientSessionActions(connection, registry, []SessionAction{
		{Kind: SessionActionUseSchema, Value: "analytics"},
		{Kind: SessionActionSetVariable, Name: "autocommit", Value: "0"},
		{Kind: SessionActionSetTimeZone, Value: "UTC"},
	}, ClientSessionActionOptions{Apply: true})
	exchange := service.SummarizeClientSessionActions(session)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported session action summary", exchange)
	}
	row := exchange.Rows[0]
	if row.SessionID != "session-1" || row.User != "moli" || row.BeforeSchema != "quanta" || row.AfterSchema != "analytics" {
		t.Fatalf("row = %#v, want session before/after metadata", row)
	}
	if row.Actions != 3 || row.SchemaActions != 1 || row.VariableActions != 1 || row.TimeZoneActions != 1 || !row.Applied || !row.Supported {
		t.Fatalf("row = %#v, want action counts", row)
	}
	if row.BeforeSQLModes != 1 || row.AfterSQLModes != 1 || row.BeforeVariables != 1 || row.AfterVariables != 1 {
		t.Fatalf("row = %#v, want session shape counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 19 {
		t.Fatalf("result/schema = %#v/%#v, want session action summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "session-1" || resultRow[3].Value != "analytics" || resultRow[8].Value != 3 || resultRow[16].Value != true {
		t.Fatalf("result row = %#v, want summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientSessionActionsReportsProtocolDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Session.CurrentSchema = "quanta"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	session := service.PrepareClientSessionActions(connection, NewMemorySessionRegistry(), []SessionAction{
		{Kind: SessionActionUseSchema, Value: "analytics"},
	}, ClientSessionActionOptions{Apply: true})
	exchange := service.SummarizeClientSessionActions(session)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, session diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported session action row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientSessionActionsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}
	session := service.PrepareClientSessionActions(connection, nil, []SessionAction{
		{Kind: SessionActionUseSchema, Value: "analytics"},
	}, ClientSessionActionOptions{})

	exchange := service.SummarizeClientSessionActions(session)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless summary", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientSessionActionsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Session.ID = "session-1"
	connection.Session.Roles = []RoleName{"reader"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilitySessionActions)
	session := service.PrepareClientSessionActions(connection, nil, []SessionAction{
		{Kind: SessionActionSetSQLMode, Value: "ANSI_QUOTES"},
	}, ClientSessionActionOptions{})

	exchange := service.SummarizeClientSessionActions(session)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Session.Transition.Actions[0].Value = "mutated"
	exchange.Session.Transition.After.SQLModes[0] = "mutated"
	exchange.Rows[0].AfterSchema = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][5].Value = "mutated"

	again := service.SummarizeClientSessionActions(session)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Session.Transition.Actions[0].Value != "ANSI_QUOTES" {
		t.Fatalf("action metadata leaked mutation: %#v", again.Session.Transition.Actions)
	}
	if again.Session.Transition.After.SQLModes[0] != "ANSI_QUOTES" {
		t.Fatalf("transition metadata leaked mutation: %#v", again.Session.Transition.After.SQLModes)
	}
	if again.Rows[0].AfterSchema == "mutated" {
		t.Fatalf("row leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Session_id" || again.ResultSchema.Columns[0].Name != "Session_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][5].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
