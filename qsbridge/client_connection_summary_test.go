package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientConnectionReturnsAcceptRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults),
		AuthenticationRequest{User: "moli", DefaultSchema: "quanta"},
		ClientCapabilitySessionTracking,
	)
	accepted := service.PrepareClientConnection(request, staticAuthenticator{
		principal: AuthenticationPrincipal{
			User:          "moli",
			Roles:         []RoleName{"reader"},
			DefaultSchema: "quanta",
		},
	}, registry, ClientConnectionOptions{
		RegisterSession: true,
		CapabilityPolicy: ConnectionCapabilityPolicy{
			Optional: ClientCapabilities{ClientCapabilitySessionTracking},
		},
	})

	exchange := service.SummarizeClientConnection(accepted)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported connection summary", exchange)
	}
	row := exchange.Rows[0]
	if row.Operation != ClientConnectionSummaryAccept || row.SessionID != "session-1" || row.User != "moli" || row.Schema != "quanta" {
		t.Fatalf("row = %#v, want accepted connection identity", row)
	}
	if row.AcceptedCapabilities != string(ClientCapabilitySessionTracking) || !row.Registered || !row.Supported {
		t.Fatalf("row = %#v, want registered accepted capability metadata", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 11 {
		t.Fatalf("result/schema = %#v/%#v, want connection summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(ClientConnectionSummaryAccept) || resultRow[2].Value != "moli" || resultRow[5].Value != true {
		t.Fatalf("result row = %#v, want accept cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientConnectionReportsAuthenticationDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults),
		AuthenticationRequest{User: "moli"},
	)
	accepted := service.PrepareClientConnection(request, denyingAuthenticator{}, NewMemorySessionRegistry(), ClientConnectionOptions{RegisterSession: true})

	exchange := service.SummarizeClientConnection(accepted)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, authentication diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported || exchange.Rows[0].Registered {
		t.Fatalf("rows = %#v, want denied accept row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientConnectionCloseReturnsCloseRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := testConnectionContext()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	registry.Put(connection.Session)
	closed := service.PrepareClientConnectionClose(connection, registry, ClientConnectionCloseOptions{RemoveSession: true})

	exchange := service.SummarizeClientConnectionClose(connection, closed)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported close summary", exchange)
	}
	row := exchange.Rows[0]
	if row.Operation != ClientConnectionSummaryClose || row.SessionID != connection.Session.ID || !row.CloseConnection || !row.RemovedSession {
		t.Fatalf("row = %#v, want connection close metadata", row)
	}
	if row.Status != "Connection close requested" || !row.Supported {
		t.Fatalf("row = %#v, want close response metadata", row)
	}
}

func TestPlanningServiceSummarizeClientConnectionCloseFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := testConnectionContext()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientConnectionClose(connection, ClientConnectionCloseExchange{CloseConnection: true})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block close summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientConnectionCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults),
		AuthenticationRequest{User: "moli", DefaultSchema: "quanta", Attributes: map[string]string{"client": "mysql"}},
		ClientCapabilitySessionTracking,
	)
	accepted := service.PrepareClientConnection(request, allowingAuthenticator{}, nil, ClientConnectionOptions{})

	exchange := service.SummarizeClientConnection(accepted)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].User = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.SummarizeClientConnection(accepted)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].User != "moli" {
		t.Fatalf("row leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Operation" || again.ResultSchema.Columns[0].Name != "Operation" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != "moli" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
