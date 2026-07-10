package qsbridge

import "testing"

func TestPlanningServicePrepareClientSessionActionsPreviewsWithoutApplying(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Session.CurrentSchema = "quanta"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilitySessionActions)
	registry.Put(connection.Session)

	exchange := service.PrepareClientSessionActions(connection, registry, []SessionAction{
		{Kind: SessionActionUseSchema, Value: "analytics"},
	}, ClientSessionActionOptions{})
	if !exchange.Supported() || exchange.Applied {
		t.Fatalf("exchange = %#v, want supported preview only", exchange)
	}
	if exchange.Transition.Before.CurrentSchema != "quanta" || exchange.Transition.After.CurrentSchema != "analytics" {
		t.Fatalf("transition = %#v, want schema preview", exchange.Transition)
	}
	stored, ok := registry.Get("session-1")
	if !ok || stored.CurrentSchema != "quanta" {
		t.Fatalf("stored = %#v ok=%v, want registry unchanged", stored, ok)
	}
}

func TestPlanningServicePrepareClientSessionActionsAppliesToRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Session.CurrentSchema = "quanta"
	connection.Session.Variables = map[string]string{"autocommit": "1"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilitySessionActions)
	registry.Put(connection.Session)

	exchange := service.PrepareClientSessionActions(connection, registry, []SessionAction{
		{Kind: SessionActionUseSchema, Value: "analytics"},
		{Kind: SessionActionSetVariable, Name: "autocommit", Value: "0"},
	}, ClientSessionActionOptions{Apply: true})
	if !exchange.Supported() || !exchange.Applied {
		t.Fatalf("exchange = %#v, want applied transition", exchange)
	}
	stored, ok := registry.Get("session-1")
	if !ok || stored.CurrentSchema != "analytics" || stored.Variables["autocommit"] != "0" {
		t.Fatalf("stored = %#v ok=%v, want applied session", stored, ok)
	}
}

func TestPlanningServicePrepareClientSessionActionsRejectsUnsupportedProtocol(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Session.CurrentSchema = "quanta"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	registry.Put(connection.Session)

	exchange := service.PrepareClientSessionActions(connection, registry, []SessionAction{
		{Kind: SessionActionUseSchema, Value: "analytics"},
	}, ClientSessionActionOptions{Apply: true})
	if exchange.Supported() || exchange.Applied {
		t.Fatalf("exchange = %#v, want protocol rejection", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	stored, ok := registry.Get("session-1")
	if !ok || stored.CurrentSchema != "quanta" {
		t.Fatalf("stored = %#v ok=%v, want registry unchanged", stored, ok)
	}
}

func TestPlanningServicePrepareClientSessionActionsReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilitySessionActions)

	exchange := service.PrepareClientSessionActions(connection, nil, []SessionAction{
		{Kind: SessionActionUseSchema, Value: "analytics"},
	}, ClientSessionActionOptions{Apply: true})
	if exchange.Supported() || exchange.Applied {
		t.Fatalf("exchange = %#v, want missing registry failure", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
}

func TestPlanningServicePrepareClientResultSessionActionsMergesResultDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilitySessionActions)
	result := ExecutionResult{
		SessionActions: []SessionAction{{Kind: SessionActionSetTimeZone, Value: "UTC"}},
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "bad result metadata"),
		},
	}

	exchange := service.PrepareClientResultSessionActions(connection, NewMemorySessionRegistry(), result, ClientSessionActionOptions{})
	if exchange.Supported() {
		t.Fatalf("expected result diagnostics to make exchange unsupported")
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
}

func TestPlanningServicePrepareClientSessionActionsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Session.Roles = []RoleName{"reader"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilitySessionActions)
	actions := []SessionAction{{Kind: SessionActionSetSQLMode, Value: "ANSI_QUOTES"}}

	exchange := service.PrepareClientSessionActions(connection, nil, actions, ClientSessionActionOptions{})
	exchange.Connection.Session.Roles[0] = "mutated"
	exchange.Transition.Actions[0].Value = "mutated"
	exchange.Transition.After.SQLModes[0] = "mutated"
	if connection.Session.Roles[0] != "reader" {
		t.Fatalf("connection roles leaked mutation: %#v", connection.Session.Roles)
	}
	if actions[0].Value != "ANSI_QUOTES" {
		t.Fatalf("actions leaked mutation: %#v", actions)
	}
}
