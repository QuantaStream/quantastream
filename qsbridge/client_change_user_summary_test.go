package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientChangeUserReturnsChangeRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := testConnectionContext()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	registry.Put(connection.Session)
	request := NewConnectionRequest(
		"ignored-new-session",
		connection.Protocol,
		AuthenticationRequest{User: "ana", DefaultSchema: "analytics", Method: AuthenticationMethodMySQLPassword},
		ClientCapabilitySessionTracking,
	)
	changed := service.PrepareClientChangeUser(connection, request, staticAuthenticator{
		principal: AuthenticationPrincipal{
			User:          "ana",
			Roles:         []RoleName{"analyst"},
			DefaultSchema: "analytics",
			Attributes:    map[string]string{"tenant": "analytics"},
		},
	}, registry, ClientChangeUserOptions{
		ApplySession: true,
		CapabilityPolicy: ConnectionCapabilityPolicy{
			Optional: ClientCapabilities{ClientCapabilitySessionTracking},
		},
	})

	exchange := service.SummarizeClientChangeUser(connection, changed)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported change-user summary", exchange)
	}
	row := exchange.Rows[0]
	if row.PreviousUser != connection.Session.User || row.NextUser != "ana" {
		t.Fatalf("row = %#v, want user transition metadata", row)
	}
	if row.PreviousSchema != connection.Session.CurrentSchema || row.NextSchema != "analytics" {
		t.Fatalf("row = %#v, want schema transition metadata", row)
	}
	if row.AcceptedCapabilities != string(ClientCapabilitySessionTracking) || !row.Applied || row.SessionActions != 1 || row.Status != "User changed" || !row.Supported {
		t.Fatalf("row = %#v, want applied change-user response metadata", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want change-user summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[1].Value != "ana" || resultRow[4].Value != string(ClientCapabilitySessionTracking) || resultRow[7].Value != "User changed" {
		t.Fatalf("result row = %#v, want change-user cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientChangeUserReportsChangeDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := testConnectionContext()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	request := NewConnectionRequest(
		"ignored-new-session",
		connection.Protocol,
		AuthenticationRequest{User: "ana"},
	)
	changed := service.PrepareClientChangeUser(connection, request, denyingAuthenticator{}, NewMemorySessionRegistry(), ClientChangeUserOptions{ApplySession: true})

	exchange := service.SummarizeClientChangeUser(connection, changed)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, change-user diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported || exchange.Rows[0].Applied {
		t.Fatalf("rows = %#v, want denied change-user row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientChangeUserFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := testConnectionContext()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientChangeUser(connection, ClientChangeUserExchange{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block change-user summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientChangeUserCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := testConnectionContext()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	request := NewConnectionRequest(
		"ignored-new-session",
		connection.Protocol,
		AuthenticationRequest{
			User:          "ana",
			DefaultSchema: "analytics",
			Attributes:    map[string]string{"client": "mysql"},
		},
		ClientCapabilitySessionTracking,
	)
	changed := service.PrepareClientChangeUser(connection, request, allowingAuthenticator{}, nil, ClientChangeUserOptions{})

	exchange := service.SummarizeClientChangeUser(connection, changed)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.ChangeUser.Connection.Attributes["client"] = "mutated"
	exchange.ChangeUser.Response.SessionActions[0].Kind = SessionActionSetVariable
	exchange.Rows[0].NextUser = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.SummarizeClientChangeUser(connection, changed)
	if again.Connection.Attributes["client"] != "mysql" || again.ChangeUser.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v/%#v", again.Connection.Attributes, again.ChangeUser.Connection.Attributes)
	}
	if again.ChangeUser.Response.SessionActions[0].Kind != SessionActionChangeUser || again.Rows[0].NextUser != "ana" {
		t.Fatalf("change-user summary leaked mutation: %#v/%#v", again.ChangeUser.Response, again.Rows)
	}
	if again.Result.Columns[0].Name != "Previous_user" || again.ResultSchema.Columns[0].Name != "Previous_user" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != "ana" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
