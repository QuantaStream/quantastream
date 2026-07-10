package qsbridge

import "testing"

func TestPlanningServicePrepareClientPlanInspectionReturnsExplainProfile(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityExplain, ProtocolCapabilityProfile)

	exchange := service.PrepareClientPlanInspection(connection, ConnectionPlanOptions{}, "select o_orderkey from orders where o_orderkey = ?", ExecutionOptions{
		RequestID:      "inspect-1",
		TraceExplain:   true,
		IncludeProfile: true,
	})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported inspection exchange", exchange)
	}
	if parser.count != 1 {
		t.Fatalf("parser count = %d, want one prepare", parser.count)
	}
	if exchange.Statement.Ordinal != 0 || exchange.Request.SQL == "" {
		t.Fatalf("statement/request = %#v/%#v, want adapter statement metadata", exchange.Statement, exchange.Request)
	}
	if exchange.Profile.RequestID != "inspect-1" || !exchange.Profile.TraceExplain || !exchange.Profile.IncludeProfile {
		t.Fatalf("profile = %#v, want requested explain/profile flags", exchange.Profile)
	}
	if exchange.Profile.LogicalPlan == "" || exchange.Profile.PhysicalPlan == "" {
		t.Fatalf("profile plans = %q/%q, want explain text", exchange.Profile.LogicalPlan, exchange.Profile.PhysicalPlan)
	}
	if len(exchange.Inspection.Query.ResultColumns) != 1 || len(exchange.Inspection.Logical.Nodes) == 0 {
		t.Fatalf("inspection = %#v, want query and logical metadata", exchange.Inspection)
	}
}

func TestPlanningServicePrepareClientPlanInspectionRejectsUnsupportedProtocol(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.PrepareClientPlanInspection(connection, ConnectionPlanOptions{}, "select o_orderkey from orders where o_orderkey = ?", ExecutionOptions{
		TraceExplain:   true,
		IncludeProfile: true,
	})
	if exchange.Supported() {
		t.Fatalf("expected unsupported protocol inspection exchange")
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	if exchange.Prepared.SQL == "" || len(exchange.Inspection.Logical.Nodes) == 0 {
		t.Fatalf("exchange = %#v, want planning metadata even when protocol rejects explain/profile", exchange)
	}
}

func TestPlanningServicePrepareClientPlanInspectionReportsPlanningDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser: stubParserBridge{
			statement: serviceSelectStatement(),
			diagnostics: DiagnosticSet{
				ErrorDiagnostic(DiagnosticParserBoundary, PhaseParse, "bad statement"),
			},
		},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityExplain)

	exchange := service.PrepareClientPlanInspection(connection, ConnectionPlanOptions{}, "select broken", ExecutionOptions{TraceExplain: true})
	if exchange.Supported() {
		t.Fatalf("expected planning diagnostics to block inspection exchange")
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticParserBoundary) {
		t.Fatalf("diagnostics = %#v, want parser boundary", exchange.Diagnostics)
	}
	if _, ok := exchange.FirstProtocolError(); !ok {
		t.Fatalf("expected protocol error")
	}
}

func TestPlanningServicePrepareClientPlanInspectionCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityExplain, ProtocolCapabilityProfile)

	exchange := service.PrepareClientPlanInspection(connection, ConnectionPlanOptions{}, "select o_orderkey from orders where o_orderkey = ?", ExecutionOptions{
		TraceExplain:   true,
		IncludeProfile: true,
	})
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Inspection.Query.ResultColumns[0].Name = "mutated"
	exchange.Inspection.Logical.Nodes[0].Summary = "mutated"
	exchange.Profile.Diagnostics = DiagnosticSet{{Fields: []FieldRef{{Name: "mutated"}}}}

	again := service.PrepareClientPlanInspection(connection, ConnectionPlanOptions{}, "select o_orderkey from orders where o_orderkey = ?", ExecutionOptions{
		TraceExplain:   true,
		IncludeProfile: true,
	})
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Inspection.Query.ResultColumns[0].Name == "mutated" || again.Inspection.Logical.Nodes[0].Summary == "mutated" {
		t.Fatalf("inspection leaked mutation: %#v", again.Inspection)
	}
	if len(again.Profile.Diagnostics) > 0 && again.Profile.Diagnostics[0].Fields[0].Name == "mutated" {
		t.Fatalf("profile diagnostics leaked mutation: %#v", again.Profile.Diagnostics)
	}
}
