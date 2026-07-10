package qsbridge

import "testing"

func TestPlanningServicePrepareClientHandshakeBuildsGreetingAndNegotiation(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults),
		AuthenticationRequest{
			User:   "moli",
			Method: AuthenticationMethodMySQLPassword,
		},
		ClientCapabilityTLS,
		ClientCapabilitySessionTracking,
	)

	exchange := service.PrepareClientHandshake(request, ClientHandshakeOptions{
		ServerVersion: "quanta-1.0",
		AuthPlugin:    "mysql_native_password",
		StatusFlags: []ClientHandshakeStatusFlag{
			ClientHandshakeStatusAutocommit,
			ClientHandshakeStatusTLSAvailable,
		},
		CapabilityPolicy: ConnectionCapabilityPolicy{
			Required: ClientCapabilities{ClientCapabilityTLS},
			Optional: ClientCapabilities{ClientCapabilitySessionTracking},
		},
	})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported handshake", exchange)
	}
	if exchange.Greeting.SessionID != "session-1" || exchange.Greeting.ServerVersion != "quanta-1.0" || exchange.Greeting.AuthPlugin != "mysql_native_password" {
		t.Fatalf("greeting = %#v, want adapter-supplied greeting metadata", exchange.Greeting)
	}
	if exchange.Greeting.CharacterSet != "utf8mb4" || exchange.Greeting.Collation != "utf8mb4_0900_ai_ci" {
		t.Fatalf("greeting = %#v, want default charset and collation", exchange.Greeting)
	}
	if len(exchange.Negotiation.Accepted) != 2 || !exchange.Negotiation.Accepted.Has(ClientCapabilityTLS) || !exchange.Negotiation.Accepted.Has(ClientCapabilitySessionTracking) {
		t.Fatalf("negotiation = %#v, want required and optional accepted capabilities", exchange.Negotiation)
	}
}

func TestPlanningServicePrepareClientHandshakeDefaultsAuthPluginFromMethod(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults),
		AuthenticationRequest{
			User:   "moli",
			Method: AuthenticationMethodMySQLPassword,
		},
	)

	exchange := service.PrepareClientHandshake(request, ClientHandshakeOptions{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported handshake", exchange)
	}
	if exchange.Greeting.AuthPlugin != string(AuthenticationPluginCachingSHA2Password) {
		t.Fatalf("auth plugin = %q, want caching sha2 password", exchange.Greeting.AuthPlugin)
	}
}

func TestPlanningServicePrepareClientHandshakeReportsInvalidMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := ConnectionRequest{
		Authentication: AuthenticationRequest{User: "moli"},
	}

	exchange := service.PrepareClientHandshake(request, ClientHandshakeOptions{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want invalid handshake metadata", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	if _, ok := exchange.FirstProtocolError(); !ok {
		t.Fatalf("expected protocol error")
	}
}

func TestPlanningServicePrepareClientHandshakeRejectsMissingRequiredCapability(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql"),
		AuthenticationRequest{User: "moli"},
		ClientCapabilitySessionTracking,
	)

	exchange := service.PrepareClientHandshake(request, ClientHandshakeOptions{
		CapabilityPolicy: ConnectionCapabilityPolicy{
			Required: ClientCapabilities{ClientCapabilityTLS},
			Optional: ClientCapabilities{ClientCapabilitySessionTracking},
		},
	})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing required capability diagnostic", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	if !exchange.Negotiation.Accepted.Has(ClientCapabilitySessionTracking) {
		t.Fatalf("negotiation = %#v, want optional accepted capability preserved", exchange.Negotiation)
	}
}

func TestPlanningServicePrepareClientHandshakeCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	protocol := NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	request := NewConnectionRequest(
		"session-1",
		protocol,
		AuthenticationRequest{User: "moli", Method: AuthenticationMethodMySQLPassword},
		ClientCapabilitySessionTracking,
	)
	options := ClientHandshakeOptions{
		StatusFlags: []ClientHandshakeStatusFlag{ClientHandshakeStatusAutocommit},
	}

	exchange := service.PrepareClientHandshake(request, options)
	exchange.Request.Capabilities[0] = ClientCapabilityTLS
	exchange.Greeting.Protocol.Capabilities[0] = ProtocolCapabilityBatchExecution
	exchange.Greeting.StatusFlags[0] = ClientHandshakeStatusTLSAvailable

	again := service.PrepareClientHandshake(request, options)
	if again.Request.Capabilities[0] != ClientCapabilitySessionTracking {
		t.Fatalf("request leaked mutation: %#v", again.Request.Capabilities)
	}
	if again.Greeting.Protocol.Capabilities[0] != ProtocolCapabilityStatementResults {
		t.Fatalf("protocol leaked mutation: %#v", again.Greeting.Protocol.Capabilities)
	}
	if again.Greeting.StatusFlags[0] != ClientHandshakeStatusAutocommit {
		t.Fatalf("status flags leaked mutation: %#v", again.Greeting.StatusFlags)
	}
}
