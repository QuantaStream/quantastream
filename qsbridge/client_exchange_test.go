package qsbridge

import "testing"

func TestPlanningServicePrepareClientExchangeComposesHandoffAndPreview(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityMultiStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.PrepareConnectionClientExchange(
		connection,
		ConnectionPlanOptions{CatalogVersion: "catalog-v1"},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)}},
		"select 1",
		"select 2",
	)
	if !exchange.Supported() {
		t.Fatalf("diagnostics = %#v, want supported exchange", exchange.Diagnostics)
	}
	if parser.count != 2 {
		t.Fatalf("parser count = %d, want one prepare per statement", parser.count)
	}
	if len(exchange.Handoff.Statements) != 2 || len(exchange.Preview.Statements) != 2 {
		t.Fatalf("handoff/preview counts = %d/%d, want 2/2", len(exchange.Handoff.Statements), len(exchange.Preview.Statements))
	}
	if exchange.Handoff.Statements[1].Plan.Prepared.CatalogVersion != "catalog-v1" {
		t.Fatalf("prepared catalog version = %q, want catalog-v1", exchange.Handoff.Statements[1].Plan.Prepared.CatalogVersion)
	}
	if !exchange.Preview.Statements[0].HasSchema || exchange.Preview.Statements[0].Schema.Columns[0].Name != "order_id" {
		t.Fatalf("preview = %#v, want query schema metadata", exchange.Preview.Statements[0])
	}
}

func TestPlanningServicePrepareClientExchangeShortCircuitsUnsupportedBundle(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	exchange := service.PrepareConnectionClientExchange(
		clientStatementConnection(),
		ConnectionPlanOptions{},
		ClientHandoffOptions{},
		"select 1",
		"select 2",
	)
	if exchange.Supported() {
		t.Fatalf("expected exchange to be unsupported")
	}
	if parser.count != 0 {
		t.Fatalf("parser count = %d, want unsupported bundle to avoid planning", parser.count)
	}
	if len(exchange.Handoff.Statements) != 0 || len(exchange.Preview.Statements) != 0 {
		t.Fatalf("handoff/preview = %#v/%#v, want no statement metadata", exchange.Handoff.Statements, exchange.Preview.Statements)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
}

func TestPlanningServicePrepareClientExchangeCarriesProtocolErrors(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Routing = NativeOnlyRoutingPolicy()

	exchange := service.PrepareConnectionClientExchange(
		clientStatementConnection(),
		ConnectionPlanOptions{},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueString, "bad")}},
		"select 1",
	)
	if exchange.Supported() {
		t.Fatalf("expected exchange to be unsupported")
	}
	if len(exchange.ProtocolErrors()) == 0 {
		t.Fatalf("expected protocol errors")
	}
	if exchange.Outcomes()[0].Kind != ExecutionHandoffRejected {
		t.Fatalf("outcomes = %#v, want route rejection", exchange.Outcomes())
	}
}

func TestClientExchangeCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.PrepareConnectionClientExchange(
		connection,
		ConnectionPlanOptions{},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)}},
		"select 1",
	)
	exchange.Request.Statements[0].SQL = "mutated"
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Preview.Statements[0].Schema.Columns[0].Name = "mutated"

	again := service.PrepareConnectionClientExchange(
		connection,
		ConnectionPlanOptions{},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)}},
		"select 1",
	)
	if again.Request.Statements[0].SQL != "select 1" {
		t.Fatalf("request leaked mutation: %#v", again.Request.Statements)
	}
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Preview.Statements[0].Schema.Columns[0].Name == "mutated" {
		t.Fatalf("preview leaked mutation")
	}
}
