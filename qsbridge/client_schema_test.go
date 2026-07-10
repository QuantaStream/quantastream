package qsbridge

import "testing"

func TestPlanningServicePrepareClientUseSchemaBuildsStatementResponse(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	exchange := service.PrepareClientUseSchema(connection, nil, "analytics", ClientSchemaSelectionOptions{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported schema selection metadata", exchange)
	}
	if exchange.Response.Status != "Database changed" {
		t.Fatalf("status = %q, want database changed", exchange.Response.Status)
	}
	if !protocolStatusFlagsContain(exchange.Response.Flags, ProtocolStatusSessionStateChanged) {
		t.Fatalf("flags = %#v, want session state change", exchange.Response.Flags)
	}
	if exchange.Session.Applied {
		t.Fatalf("session should not apply without ApplySession")
	}
	if exchange.Session.Transition.Before.CurrentSchema != "quanta" || exchange.Session.Transition.After.CurrentSchema != "analytics" {
		t.Fatalf("transition = %#v, want schema preview", exchange.Session.Transition)
	}
}

func TestPlanningServicePrepareClientUseSchemaAppliesSessionRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Session.CurrentSchema = "quanta"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	registry.Put(connection.Session)

	exchange := service.PrepareClientUseSchema(connection, registry, "analytics", ClientSchemaSelectionOptions{ApplySession: true})
	if !exchange.Supported() || !exchange.Session.Applied {
		t.Fatalf("exchange = %#v, want applied schema selection", exchange)
	}
	stored, ok := registry.Get("session-1")
	if !ok || stored.CurrentSchema != "analytics" {
		t.Fatalf("stored = %#v ok=%v, want analytics schema", stored, ok)
	}
}

func TestPlanningServicePrepareClientUseSchemaValidatesCatalogWhenConfigured(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	catalog := MemoryCatalog{Tables: []TableDefinition{{Schema: "analytics", Name: "events"}}}

	exchange := service.PrepareClientUseSchema(connection, nil, "ANALYTICS", ClientSchemaSelectionOptions{Catalog: catalog})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want catalog-validated schema selection", exchange)
	}
	if exchange.Session.Transition.After.CurrentSchema != "ANALYTICS" {
		t.Fatalf("transition = %#v, want requested schema preserved", exchange.Session.Transition)
	}
}

func TestPlanningServicePrepareClientUseSchemaRejectsUnknownCatalogSchema(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Session.CurrentSchema = "quanta"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	registry.Put(connection.Session)
	catalog := MemoryCatalog{Tables: []TableDefinition{{Schema: "analytics", Name: "events"}}}

	exchange := service.PrepareClientUseSchema(connection, registry, "missing", ClientSchemaSelectionOptions{ApplySession: true, Catalog: catalog})
	if exchange.Supported() || exchange.Session.Applied {
		t.Fatalf("exchange = %#v, want unknown schema rejection", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticCatalogSchemaNotFound) {
		t.Fatalf("diagnostics = %#v, want catalog schema diagnostic", exchange.Diagnostics)
	}
	protocolError, ok := exchange.FirstProtocolError()
	if !ok || protocolError.VendorCode != mysqlErrorUnknownDatabase || protocolError.SQLState != SQLStateInvalidCatalogName {
		t.Fatalf("protocol error = %#v ok=%v, want unknown database error", protocolError, ok)
	}
	stored, ok := registry.Get("session-1")
	if !ok || stored.CurrentSchema != "quanta" {
		t.Fatalf("stored = %#v ok=%v, want registry unchanged", stored, ok)
	}
}

func TestPlanningServicePrepareClientUseSchemaRejectsEmptySchema(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	exchange := service.PrepareClientUseSchema(connection, nil, "", ClientSchemaSelectionOptions{})
	if exchange.Supported() {
		t.Fatalf("expected empty schema to be unsupported")
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
}

func TestPlanningServicePrepareClientUseSchemaRejectsUnsupportedProtocol(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Session.CurrentSchema = "quanta"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	registry.Put(connection.Session)

	exchange := service.PrepareClientUseSchema(connection, registry, "analytics", ClientSchemaSelectionOptions{ApplySession: true})
	if exchange.Supported() || exchange.Session.Applied {
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

func TestPlanningServicePrepareClientUseSchemaCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	exchange := service.PrepareClientUseSchema(connection, nil, "analytics", ClientSchemaSelectionOptions{})
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Response.SessionActions[0].Value = "mutated"

	again := service.PrepareClientUseSchema(connection, nil, "analytics", ClientSchemaSelectionOptions{})
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Response.SessionActions[0].Value != "analytics" {
		t.Fatalf("response action leaked mutation: %#v", again.Response.SessionActions)
	}
}
