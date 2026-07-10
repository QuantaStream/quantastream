package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientHandshakeReturnsGreetingRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults),
		AuthenticationRequest{User: "moli", Method: AuthenticationMethodMySQLPassword},
		ClientCapabilityTLS,
		ClientCapabilitySessionTracking,
	)
	handshake := service.PrepareClientHandshake(request, ClientHandshakeOptions{
		ServerVersion: "quanta-1.0",
		AuthPlugin:    string(AuthenticationPluginMySQLNativePassword),
		StatusFlags: []ClientHandshakeStatusFlag{
			ClientHandshakeStatusAutocommit,
			ClientHandshakeStatusTLSAvailable,
		},
		CapabilityPolicy: ConnectionCapabilityPolicy{
			Required: ClientCapabilities{ClientCapabilityTLS},
			Optional: ClientCapabilities{ClientCapabilitySessionTracking},
		},
	})

	exchange := service.SummarizeClientHandshake(handshake)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported handshake summary", exchange)
	}
	row := exchange.Rows[0]
	if row.SessionID != "session-1" || row.Protocol != ProtocolMySQL || row.Driver != "mysql" {
		t.Fatalf("row = %#v, want protocol identity", row)
	}
	if row.ServerVersion != "quanta-1.0" || row.AuthPlugin != string(AuthenticationPluginMySQLNativePassword) || row.CharacterSet != "utf8mb4" {
		t.Fatalf("row = %#v, want greeting metadata", row)
	}
	if row.AcceptedCapabilities != "tls,session_tracking" || !row.Supported {
		t.Fatalf("row = %#v, want accepted client capabilities", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 11 {
		t.Fatalf("result/schema = %#v/%#v, want handshake summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "session-1" || resultRow[3].Value != "quanta-1.0" || resultRow[9].Value != true {
		t.Fatalf("result row = %#v, want handshake summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientHandshakeReportsHandshakeDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	handshake := service.PrepareClientHandshake(ConnectionRequest{
		Authentication: AuthenticationRequest{User: "moli"},
	}, ClientHandshakeOptions{})

	exchange := service.SummarizeClientHandshake(handshake)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, handshake diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported handshake row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientHandshakeCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults),
		AuthenticationRequest{User: "moli", Method: AuthenticationMethodMySQLPassword},
		ClientCapabilitySessionTracking,
	)
	handshake := service.PrepareClientHandshake(request, ClientHandshakeOptions{
		StatusFlags: []ClientHandshakeStatusFlag{ClientHandshakeStatusAutocommit},
	})

	exchange := service.SummarizeClientHandshake(handshake)
	exchange.Handshake.Request.Capabilities[0] = ClientCapabilityTLS
	exchange.Handshake.Greeting.Protocol.Capabilities[0] = ProtocolCapabilityBatchExecution
	exchange.Rows[0].StatusFlags[0] = ClientHandshakeStatusTLSAvailable
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][3].Value = "mutated"

	again := service.SummarizeClientHandshake(handshake)
	if again.Handshake.Request.Capabilities[0] != ClientCapabilitySessionTracking {
		t.Fatalf("request leaked mutation: %#v", again.Handshake.Request.Capabilities)
	}
	if again.Handshake.Greeting.Protocol.Capabilities[0] != ProtocolCapabilityStatementResults {
		t.Fatalf("protocol leaked mutation: %#v", again.Handshake.Greeting.Protocol.Capabilities)
	}
	if again.Rows[0].StatusFlags[0] != ClientHandshakeStatusAutocommit {
		t.Fatalf("row leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Session_id" || again.ResultSchema.Columns[0].Name != "Session_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][3].Value != "quanta" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
