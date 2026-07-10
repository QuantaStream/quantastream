package qsbridge

import "testing"

func TestPlanningServicePrepareClientBeginTransactionBuildsStatementResponse(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	exchange := service.PrepareClientBeginTransaction(connection, nil, ClientTransactionOptions{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported begin transaction metadata", exchange)
	}
	if exchange.Action.Kind != SessionActionBeginTransaction {
		t.Fatalf("action = %#v, want begin transaction", exchange.Action)
	}
	if exchange.Response.Status != "Transaction started" {
		t.Fatalf("status = %q, want transaction started", exchange.Response.Status)
	}
	if !protocolStatusFlagsContain(exchange.Response.Flags, ProtocolStatusSessionStateChanged) {
		t.Fatalf("flags = %#v, want session state change", exchange.Response.Flags)
	}
	if exchange.Session.Applied {
		t.Fatalf("session should not apply without ApplySession")
	}
}

func TestPlanningServicePrepareClientTransactionActionAppliesSessionRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	registry.Put(connection.Session)

	exchange := service.PrepareClientCommitTransaction(connection, registry, ClientTransactionOptions{ApplySession: true})
	if !exchange.Supported() || !exchange.Session.Applied {
		t.Fatalf("exchange = %#v, want applied commit transaction metadata", exchange)
	}
	stored, ok := registry.Get("session-1")
	if !ok || stored.ID != "session-1" {
		t.Fatalf("stored = %#v ok=%v, want applied session metadata", stored, ok)
	}
	if len(exchange.Session.Transition.Actions) != 1 || exchange.Session.Transition.Actions[0].Kind != SessionActionCommitTransaction {
		t.Fatalf("session transition = %#v, want commit action", exchange.Session.Transition)
	}
}

func TestPlanningServicePrepareClientTransactionActionRejectsUnsupportedProtocol(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	registry.Put(connection.Session)

	exchange := service.PrepareClientRollbackTransaction(connection, registry, ClientTransactionOptions{ApplySession: true})
	if exchange.Supported() || exchange.Session.Applied {
		t.Fatalf("exchange = %#v, want protocol rejection", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	stored, ok := registry.Get("session-1")
	if !ok || stored.ID != "session-1" {
		t.Fatalf("stored = %#v ok=%v, want registry unchanged", stored, ok)
	}
}

func TestPlanningServicePrepareClientTransactionActionRejectsNonTransactionAction(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	exchange := service.PrepareClientTransactionAction(connection, nil, SessionAction{Kind: SessionActionSetVariable, Name: "autocommit", Value: "0"}, ClientTransactionOptions{})
	if exchange.Supported() {
		t.Fatalf("expected non-transaction action to be unsupported")
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
}

func TestPlanningServicePrepareClientTransactionActionCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	exchange := service.PrepareClientBeginTransaction(connection, nil, ClientTransactionOptions{})
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Response.SessionActions[0].Kind = SessionActionRollbackTransaction

	again := service.PrepareClientBeginTransaction(connection, nil, ClientTransactionOptions{})
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Response.SessionActions[0].Kind != SessionActionBeginTransaction {
		t.Fatalf("response actions leaked mutation: %#v", again.Response.SessionActions)
	}
}
