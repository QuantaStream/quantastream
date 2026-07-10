package qsbridge

import "testing"

func TestPlanningServicePrepareClientStatementBundlePlansEachStatement(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "fallback",
		Session:       SessionContext{User: "planner-default"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityMultiStatements)
	plan := ConnectionPlanOptions{CatalogVersion: "catalog-v1"}

	bundle := NewClientStatementBundle(connection, plan, "select 1", "select 2")
	planned := service.PrepareClientStatementBundle(bundle)
	if !planned.Supported() {
		t.Fatalf("diagnostics = %#v, want supported bundle plan", planned.Diagnostics)
	}
	if parser.count != 2 {
		t.Fatalf("parser count = %d, want one prepare per statement", parser.count)
	}
	if len(planned.Statements) != 2 {
		t.Fatalf("statements = %#v, want two planned statements", planned.Statements)
	}
	if planned.Statements[0].Statement.Ordinal != 0 || planned.Statements[1].Statement.Ordinal != 1 {
		t.Fatalf("ordinals = %d/%d, want adapter order", planned.Statements[0].Statement.Ordinal, planned.Statements[1].Statement.Ordinal)
	}
	if planned.Statements[0].Request.SQL != "select 1" || planned.Statements[1].Request.SQL != "select 2" {
		t.Fatalf("requests = %q/%q, want ordered SQL", planned.Statements[0].Request.SQL, planned.Statements[1].Request.SQL)
	}
	if planned.Statements[0].Prepared.DefaultSchema != "quanta" || planned.Statements[0].Prepared.CatalogVersion != "catalog-v1" {
		t.Fatalf("prepared = %#v, want connection planning metadata", planned.Statements[0].Prepared)
	}
}

func TestPlanningServicePrepareClientStatementBundleSkipsUnsupportedBundle(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1", "select 2")
	planned := service.PrepareClientStatementBundle(bundle)
	if planned.Supported() {
		t.Fatalf("expected unsupported multi-statement bundle")
	}
	if parser.count != 0 {
		t.Fatalf("parser count = %d, want unsupported bundle to avoid planning", parser.count)
	}
	if len(planned.Statements) != 0 {
		t.Fatalf("statements = %#v, want none", planned.Statements)
	}
	if !containsDiagnosticCode(planned.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", planned.Diagnostics)
	}
}

func TestPlanningServicePrepareClientStatementBundleMergesPreparedDiagnostics(t *testing.T) {
	parser := &countingParserBridge{
		statement: serviceSelectStatement(),
		diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseParse, "bad statement"),
		},
	}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select broken")
	planned := service.PrepareClientStatementBundle(bundle)
	if planned.Supported() {
		t.Fatalf("expected prepared diagnostics to block native planning")
	}
	if len(planned.Statements) != 1 {
		t.Fatalf("statements = %#v, want blocking statement metadata preserved", planned.Statements)
	}
	if !containsDiagnosticCode(planned.Diagnostics.Codes(), DiagnosticParserBoundary) {
		t.Fatalf("diagnostics = %#v, want parser boundary", planned.Diagnostics)
	}
	if _, ok := planned.FirstProtocolError(); !ok {
		t.Fatalf("expected protocol error for blocking prepared diagnostic")
	}
}

func TestClientPlanBundleCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")

	planned := service.PrepareClientStatementBundle(bundle)
	planned.Connection.Attributes["client"] = "mutated"
	plans := planned.PreparedPlans()
	plans[0].Session.User = "mutated"
	plans[0].Parameters[0].Type = DataTypeString

	again := service.PrepareClientStatementBundle(bundle)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Statements[0].Prepared.Session.User == "mutated" {
		t.Fatalf("prepared session leaked mutation")
	}
	if again.Statements[0].Prepared.Parameters[0].Type != DataTypeInt {
		t.Fatalf("prepared parameter metadata leaked mutation: %#v", again.Statements[0].Prepared.Parameters)
	}
}
