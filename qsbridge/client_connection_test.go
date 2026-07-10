package qsbridge

import "testing"

func TestPlanningServicePrepareClientConnectionAuthenticatesAndRegistersSession(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements),
		AuthenticationRequest{
			User:          "moli",
			DefaultSchema: "quanta",
			Method:        AuthenticationMethodMySQLPassword,
			Attributes:    map[string]string{"program": "mysql"},
		},
		ClientCapabilityPreparedStatements,
		ClientCapabilitySessionTracking,
	)

	exchange := service.PrepareClientConnection(request, staticAuthenticator{
		principal: AuthenticationPrincipal{
			User:          "moli",
			Roles:         []RoleName{"reader"},
			DefaultSchema: "quanta",
			Attributes:    map[string]string{"tenant": "qa"},
		},
	}, registry, ClientConnectionOptions{RegisterSession: true})
	if !exchange.Supported() || !exchange.Registered {
		t.Fatalf("exchange = %#v, want supported registered connection", exchange)
	}
	if exchange.Connection.Session.ID != "session-1" || exchange.Connection.Session.User != "moli" || exchange.Connection.Session.CurrentSchema != "quanta" {
		t.Fatalf("session = %#v, want authenticated session metadata", exchange.Connection.Session)
	}
	if !exchange.Connection.Supports(ClientCapabilityPreparedStatements) || !exchange.Connection.Protocol.Supports(ProtocolCapabilityPreparedStatements) {
		t.Fatalf("connection = %#v, want client and protocol capabilities", exchange.Connection)
	}
	stored, ok := registry.Get("session-1")
	if !ok || stored.User != "moli" || stored.Variables["tenant"] != "qa" {
		t.Fatalf("stored = %#v ok=%v, want registered authenticated session", stored, ok)
	}
}

func TestPlanningServicePrepareClientConnectionReportsAuthenticationFailure(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql"),
		AuthenticationRequest{User: "moli"},
	)

	exchange := service.PrepareClientConnection(request, denyingAuthenticator{}, NewMemorySessionRegistry(), ClientConnectionOptions{RegisterSession: true})
	if exchange.Supported() || exchange.Registered {
		t.Fatalf("exchange = %#v, want denied unregistered connection", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", exchange.Diagnostics)
	}
	if _, ok := exchange.FirstProtocolError(); !ok {
		t.Fatalf("expected protocol error")
	}
}

func TestPlanningServicePrepareClientConnectionReportsMissingSessionRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql"),
		AuthenticationRequest{User: "moli"},
	)

	exchange := service.PrepareClientConnection(request, allowingAuthenticator{}, nil, ClientConnectionOptions{RegisterSession: true})
	if exchange.Supported() || exchange.Registered {
		t.Fatalf("exchange = %#v, want missing registry diagnostic", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
}

func TestPlanningServicePrepareClientConnectionAppliesCapabilityPolicy(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql"),
		AuthenticationRequest{User: "moli"},
		ClientCapabilityTLS,
		ClientCapabilitySessionTracking,
		ClientCapabilityCompression,
	)

	exchange := service.PrepareClientConnection(request, allowingAuthenticator{}, nil, ClientConnectionOptions{
		CapabilityPolicy: ConnectionCapabilityPolicy{
			Required: ClientCapabilities{ClientCapabilityTLS},
			Optional: ClientCapabilities{ClientCapabilitySessionTracking},
		},
	})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported negotiated connection", exchange)
	}
	if len(exchange.Negotiation.Accepted) != 2 || !exchange.Negotiation.Accepted.Has(ClientCapabilityTLS) || !exchange.Negotiation.Accepted.Has(ClientCapabilitySessionTracking) {
		t.Fatalf("negotiation = %#v, want accepted required and optional capabilities", exchange.Negotiation)
	}
	if exchange.Connection.Supports(ClientCapabilityCompression) {
		t.Fatalf("connection capabilities = %#v, did not expect unaccepted compression", exchange.Connection.Capabilities)
	}
}

func TestPlanningServicePrepareClientConnectionRejectsMissingRequiredCapability(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql"),
		AuthenticationRequest{User: "moli"},
		ClientCapabilitySessionTracking,
	)

	exchange := service.PrepareClientConnection(request, allowingAuthenticator{}, registry, ClientConnectionOptions{
		RegisterSession: true,
		CapabilityPolicy: ConnectionCapabilityPolicy{
			Required: ClientCapabilities{ClientCapabilityTLS},
			Optional: ClientCapabilities{ClientCapabilitySessionTracking},
		},
	})
	if exchange.Supported() || exchange.Registered {
		t.Fatalf("exchange = %#v, want missing required capability rejection", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	if _, ok := registry.Get("session-1"); ok {
		t.Fatalf("did not expect rejected connection to register session")
	}
}

func TestPlanningServicePrepareClientConnectionCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	auth := AuthenticationRequest{
		User:       "moli",
		Attributes: map[string]string{"program": "mysql"},
	}
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements),
		auth,
		ClientCapabilityPreparedStatements,
	)

	exchange := service.PrepareClientConnection(request, staticAuthenticator{
		principal: AuthenticationPrincipal{
			User:       "moli",
			Roles:      []RoleName{"reader"},
			Attributes: map[string]string{"tenant": "qa"},
		},
	}, nil, ClientConnectionOptions{})
	exchange.Request.Authentication.Attributes["program"] = "mutated"
	exchange.Connection.Session.Roles[0] = "mutated"
	exchange.Connection.Session.Variables["tenant"] = "mutated"
	exchange.Connection.Protocol.Capabilities[0] = ProtocolCapabilityBatchExecution
	exchange.Negotiation.Accepted[0] = ClientCapabilityBatching

	again := service.PrepareClientConnection(request, staticAuthenticator{
		principal: AuthenticationPrincipal{
			User:       "moli",
			Roles:      []RoleName{"reader"},
			Attributes: map[string]string{"tenant": "qa"},
		},
	}, nil, ClientConnectionOptions{})
	if again.Request.Authentication.Attributes["program"] != "mysql" {
		t.Fatalf("request attributes leaked mutation: %#v", again.Request.Authentication.Attributes)
	}
	if again.Connection.Session.Roles[0] != "reader" || again.Connection.Session.Variables["tenant"] != "qa" {
		t.Fatalf("session leaked mutation: %#v", again.Connection.Session)
	}
	if !again.Connection.Protocol.Supports(ProtocolCapabilityPreparedStatements) || again.Connection.Protocol.Supports(ProtocolCapabilityBatchExecution) {
		t.Fatalf("protocol leaked mutation: %#v", again.Connection.Protocol)
	}
	if !again.Negotiation.Accepted.Has(ClientCapabilityPreparedStatements) || again.Negotiation.Accepted.Has(ClientCapabilityBatching) {
		t.Fatalf("negotiation leaked mutation: %#v", again.Negotiation)
	}
}

func TestPlanningServicePrepareClientConnectionCloseRemovesRegisteredSession(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := testConnectionContext()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	registry.Put(connection.Session)

	closed := service.PrepareClientConnectionClose(connection, registry, ClientConnectionCloseOptions{RemoveSession: true})
	if !closed.Supported() || !closed.CloseConnection || !closed.RemovedSession {
		t.Fatalf("closed = %#v, want supported close with session cleanup", closed)
	}
	if closed.Response.Status != "Connection close requested" {
		t.Fatalf("status = %q, want close requested", closed.Response.Status)
	}
	if _, ok := registry.Get(connection.Session.ID); ok {
		t.Fatalf("expected session to be removed")
	}
}

func TestPlanningServicePrepareClientConnectionCloseCanLeaveSessionRegistered(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := testConnectionContext()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	registry.Put(connection.Session)

	closed := service.PrepareClientConnectionClose(connection, registry, ClientConnectionCloseOptions{})
	if !closed.Supported() || closed.RemovedSession {
		t.Fatalf("closed = %#v, want supported close without cleanup", closed)
	}
	if _, ok := registry.Get(connection.Session.ID); !ok {
		t.Fatalf("expected session to remain registered")
	}
}

func TestPlanningServicePrepareClientConnectionCloseReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := testConnectionContext()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)

	closed := service.PrepareClientConnectionClose(connection, nil, ClientConnectionCloseOptions{RemoveSession: true})
	if closed.Supported() || closed.RemovedSession {
		t.Fatalf("closed = %#v, want missing registry diagnostic", closed)
	}
	if !containsDiagnosticCode(closed.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", closed.Diagnostics)
	}
}

func TestPlanningServicePrepareClientChangeUserAuthenticatesAndAppliesSession(t *testing.T) {
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
	if !changed.Supported() || !changed.Applied {
		t.Fatalf("changed = %#v, want supported applied change-user metadata", changed)
	}
	if changed.Connection.Session.ID != connection.Session.ID || changed.Connection.Session.User != "ana" || changed.Connection.Session.CurrentSchema != "analytics" {
		t.Fatalf("session = %#v, want changed user on existing session id", changed.Connection.Session)
	}
	if !changed.Negotiation.Accepted.Has(ClientCapabilitySessionTracking) || !changed.Connection.Supports(ClientCapabilitySessionTracking) {
		t.Fatalf("negotiation/connection = %#v/%#v, want accepted session tracking", changed.Negotiation, changed.Connection)
	}
	if changed.Response.Status != "User changed" || !protocolStatusFlagsContain(changed.Response.Flags, ProtocolStatusSessionStateChanged) {
		t.Fatalf("response = %#v, want user changed session status", changed.Response)
	}
	stored, ok := registry.Get(connection.Session.ID)
	if !ok || stored.User != "ana" || stored.Variables["tenant"] != "analytics" {
		t.Fatalf("stored = %#v ok=%v, want changed registered session", stored, ok)
	}
}

func TestPlanningServicePrepareClientChangeUserReportsAuthenticationFailure(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := testConnectionContext()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	request := NewConnectionRequest(
		"ignored-new-session",
		connection.Protocol,
		AuthenticationRequest{User: "ana"},
	)

	changed := service.PrepareClientChangeUser(connection, request, denyingAuthenticator{}, NewMemorySessionRegistry(), ClientChangeUserOptions{ApplySession: true})
	if changed.Supported() || changed.Applied {
		t.Fatalf("changed = %#v, want denied change-user metadata", changed)
	}
	if !containsDiagnosticCode(changed.Diagnostics.Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", changed.Diagnostics)
	}
}

func TestPlanningServicePrepareClientChangeUserReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := testConnectionContext()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	request := NewConnectionRequest(
		"ignored-new-session",
		connection.Protocol,
		AuthenticationRequest{User: "ana"},
	)

	changed := service.PrepareClientChangeUser(connection, request, allowingAuthenticator{}, nil, ClientChangeUserOptions{ApplySession: true})
	if changed.Supported() || changed.Applied {
		t.Fatalf("changed = %#v, want missing registry diagnostic", changed)
	}
	if !containsDiagnosticCode(changed.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", changed.Diagnostics)
	}
}

func TestPlanningServicePrepareClientConnectionCloseCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := testConnectionContext()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)

	closed := service.PrepareClientConnectionClose(connection, nil, ClientConnectionCloseOptions{})
	closed.Connection.Session.Roles[0] = "mutated"
	closed.Response.Profile.Capabilities[0] = ProtocolCapabilityBatchExecution

	again := service.PrepareClientConnectionClose(connection, nil, ClientConnectionCloseOptions{})
	if again.Connection.Session.Roles[0] != "reader" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Session)
	}
	if !again.Response.Profile.Supports(ProtocolCapabilityStatementResults) || again.Response.Profile.Supports(ProtocolCapabilityBatchExecution) {
		t.Fatalf("response profile leaked mutation: %#v", again.Response.Profile)
	}
}
