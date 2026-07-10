package qsbridge

import "testing"

func TestPlanningServicePrepareClientCommandPingAndQuit(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	registry.Put(connection.Session)

	ping := service.PrepareClientCommand(connection, nil, ClientCommandPing, "", ClientCommandOptions{})
	if !ping.Supported() || ping.CloseConnection {
		t.Fatalf("ping = %#v, want supported non-closing command", ping)
	}
	if ping.Response.Status != "OK" {
		t.Fatalf("ping status = %q, want OK", ping.Response.Status)
	}

	quit := service.PrepareClientCommand(connection, registry, ClientCommandQuit, "", ClientCommandOptions{RemoveSession: true})
	if !quit.Supported() || !quit.CloseConnection {
		t.Fatalf("quit = %#v, want supported close command", quit)
	}
	if quit.Response.Status != "Connection close requested" {
		t.Fatalf("quit status = %q, want close requested", quit.Response.Status)
	}
	if _, ok := registry.Get(connection.Session.ID); ok {
		t.Fatalf("expected quit command to remove registered session")
	}
}

func TestPlanningServicePrepareClientCommandInitSchemaDelegatesSchemaSelection(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Session.CurrentSchema = "quanta"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	registry.Put(connection.Session)

	command := service.PrepareClientCommand(connection, registry, ClientCommandInitSchema, "analytics", ClientCommandOptions{ApplySession: true})
	if !command.Supported() || !command.Session.Applied {
		t.Fatalf("command = %#v, want applied init schema", command)
	}
	if command.Response.Status != "Database changed" {
		t.Fatalf("status = %q, want database changed", command.Response.Status)
	}
	stored, ok := registry.Get("session-1")
	if !ok || stored.CurrentSchema != "analytics" {
		t.Fatalf("stored = %#v ok=%v, want analytics schema", stored, ok)
	}
}

func TestPlanningServicePrepareClientCommandResetConnectionCanApplySessionReset(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session = SessionContext{
		ID:            "session-1",
		User:          "moli",
		Roles:         []RoleName{"reader"},
		CurrentSchema: "quanta",
		TimeZone:      "UTC",
		SQLModes:      []SQLMode{"ANSI_QUOTES"},
		Variables:     map[string]string{"autocommit": "0"},
	}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	registry.Put(connection.Session)

	command := service.PrepareClientCommand(connection, registry, ClientCommandResetConnection, "", ClientCommandOptions{ApplySession: true})
	if !command.Supported() || !command.Session.Applied {
		t.Fatalf("command = %#v, want applied reset command", command)
	}
	if command.Response.Status != "Connection reset" {
		t.Fatalf("status = %q, want connection reset", command.Response.Status)
	}
	if command.Session.Transition.Before.CurrentSchema != "quanta" || command.Session.Transition.After.CurrentSchema != "" {
		t.Fatalf("transition = %#v, want cleared schema", command.Session.Transition)
	}
	stored, ok := registry.Get("session-1")
	if !ok {
		t.Fatalf("expected reset session in registry")
	}
	if stored.User != "moli" || len(stored.Roles) != 1 || stored.Roles[0] != "reader" {
		t.Fatalf("stored identity = %#v, want user and roles preserved", stored)
	}
	if stored.CurrentSchema != "" || stored.TimeZone != "" || len(stored.SQLModes) != 0 || len(stored.Variables) != 0 {
		t.Fatalf("stored session = %#v, want cleared volatile session state", stored)
	}
}

func TestPlanningServicePrepareClientCommandRejectsUnsupportedCommand(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)

	command := service.PrepareClientCommand(connection, nil, ClientCommandKind("unknown"), "", ClientCommandOptions{})
	if command.Supported() {
		t.Fatalf("expected unsupported command")
	}
	if !containsDiagnosticCode(command.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", command.Diagnostics)
	}
	if _, ok := command.FirstProtocolError(); !ok {
		t.Fatalf("expected protocol error")
	}
}

func TestPlanningServicePrepareClientCommandCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	command := service.PrepareClientCommand(connection, nil, ClientCommandResetConnection, "", ClientCommandOptions{})
	command.Connection.Attributes["client"] = "mutated"
	command.Session.Transition.After.Roles[0] = "mutated"
	command.Response.SessionActions[0].Kind = SessionActionSetVariable

	again := service.PrepareClientCommand(connection, nil, ClientCommandResetConnection, "", ClientCommandOptions{})
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Session.Transition.After.Roles[0] != "reader" {
		t.Fatalf("session transition leaked mutation: %#v", again.Session.Transition.After.Roles)
	}
	if again.Response.SessionActions[0].Kind != SessionActionResetConnection {
		t.Fatalf("response action leaked mutation: %#v", again.Response.SessionActions)
	}
}
