package qsbridge

import "testing"

func TestPlanningServiceListClientSessionsReturnsRegistryRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	registry := NewMemorySessionRegistry()
	registry.Put(SessionContext{
		ID:            "session-b",
		User:          "bob",
		CurrentSchema: "analytics",
		TimeZone:      "UTC",
		Roles:         []RoleName{"reader"},
		SQLModes:      []SQLMode{"ANSI"},
		Variables:     map[string]string{"autocommit": "1"},
	})
	registry.Put(SessionContext{
		ID:            "session-a",
		User:          "alice",
		CurrentSchema: "quanta",
		TimeZone:      "America/Costa_Rica",
		Roles:         []RoleName{"admin", "writer"},
		SQLModes:      []SQLMode{"STRICT"},
		Variables:     map[string]string{"sql_mode": "STRICT", "autocommit": "1"},
	})

	exchange := service.ListClientSessions(connection, registry)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported session inventory", exchange)
	}
	if len(exchange.Rows) != 2 {
		t.Fatalf("rows = %#v, want two session rows", exchange.Rows)
	}
	first := exchange.Rows[0]
	if first.SessionID != "session-a" || first.User != "alice" || first.Schema != "quanta" {
		t.Fatalf("first row = %#v, want ordered session-a", first)
	}
	if len(first.Roles) != 2 || first.Variables != 2 {
		t.Fatalf("first row = %#v, want roles and variable count", first)
	}
	if exchange.Rows[1].SessionID != "session-b" {
		t.Fatalf("rows = %#v, want sessions sorted by id", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 7 || exchange.ResultSchema.Columns[0].Name != "Session_id" || exchange.Result.RowsReturned != 2 {
		t.Fatalf("result/schema = %#v/%#v, want session status result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "session-a" || resultRow[4].Value != "admin,writer" {
		t.Fatalf("result row = %#v, want session-a role list", resultRow)
	}
}

func TestPlanningServiceListClientSessionsReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.ListClientSessions(clientStatementConnection(), nil)

	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing registry to block inventory", exchange)
	}
	if !containsDiagnosticCode(exchange.ExchangeDiagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.ExchangeDiagnostics.Codes())
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed complete result", exchange.Result)
	}
}

func TestPlanningServiceListClientSessionsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientSessions(connection, NewMemorySessionRegistry())
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block inventory", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless inventory", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientSessionsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	registry := NewMemorySessionRegistry()
	registry.Put(SessionContext{
		ID:            "session-a",
		User:          "alice",
		CurrentSchema: "quanta",
		Roles:         []RoleName{"admin"},
		Variables:     map[string]string{"autocommit": "1"},
	})

	exchange := service.ListClientSessions(connection, registry)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].SessionID = "mutated"
	exchange.Rows[0].Roles[0] = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientSessions(connection, registry)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].SessionID != "session-a" || again.Rows[0].Roles[0] != "admin" {
		t.Fatalf("row leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Session_id" || again.ResultSchema.Columns[0].Name != "Session_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "session-a" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
