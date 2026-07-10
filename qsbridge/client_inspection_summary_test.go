package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientPlanInspectionReturnsSummaryRow(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityExplain, ProtocolCapabilityProfile)
	inspection := service.PrepareClientPlanInspection(connection, ConnectionPlanOptions{}, "select o_orderkey from orders where o_orderkey = ?", ExecutionOptions{
		RequestID:      "inspect-1",
		TraceExplain:   true,
		IncludeProfile: true,
	})

	exchange := service.SummarizeClientPlanInspection(inspection)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported inspection summary", exchange)
	}
	row := exchange.Rows[0]
	if row.RequestID != "inspect-1" || row.Kind != QueryKindSelect || !row.Supported || !row.Explain || !row.Profile {
		t.Fatalf("row = %#v, want explain/profile metadata", row)
	}
	if row.LogicalNodes == 0 || row.PhysicalNodes == 0 || row.Parameters != 1 || row.ResultColumns != 1 || row.SQLLength == 0 {
		t.Fatalf("row = %#v, want inspection counts", row)
	}
	if exchange.Result.RequestID != "inspect-1" || exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 15 {
		t.Fatalf("result/schema = %#v/%#v, want inspection summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[1].Value != "inspect-1" || resultRow[2].Value != string(QueryKindSelect) || resultRow[4].Value != true {
		t.Fatalf("result row = %#v, want inspection summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPlanInspectionReportsProtocolDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	inspection := service.PrepareClientPlanInspection(connection, ConnectionPlanOptions{}, "select o_orderkey from orders where o_orderkey = ?", ExecutionOptions{
		TraceExplain:   true,
		IncludeProfile: true,
	})

	exchange := service.SummarizeClientPlanInspection(inspection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, protocol diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported inspection row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientPlanInspectionFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}
	inspection := service.PrepareClientPlanInspection(connection, ConnectionPlanOptions{}, "select 1", ExecutionOptions{TraceExplain: true})

	exchange := service.SummarizeClientPlanInspection(inspection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless summary", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientPlanInspectionCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityExplain, ProtocolCapabilityProfile)
	inspection := service.PrepareClientPlanInspection(connection, ConnectionPlanOptions{}, "select o_orderkey from orders where o_orderkey = ?", ExecutionOptions{
		RequestID:    "inspect-1",
		TraceExplain: true,
	})

	exchange := service.SummarizeClientPlanInspection(inspection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Inspection.Prepared.Parameters[0].Type = DataTypeString
	exchange.Inspection.Inspection.Logical.Nodes[0].Summary = "mutated"
	exchange.Inspection.Profile.Diagnostics = DiagnosticSet{{Fields: []FieldRef{{Name: "mutated"}}}}
	exchange.Rows[0].Kind = QueryKindInsert
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.SummarizeClientPlanInspection(inspection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Inspection.Prepared.Parameters[0].Type != DataTypeInt {
		t.Fatalf("prepared metadata leaked mutation: %#v", again.Inspection.Prepared.Parameters)
	}
	if again.Inspection.Inspection.Logical.Nodes[0].Summary == "mutated" {
		t.Fatalf("inspection metadata leaked mutation: %#v", again.Inspection.Inspection.Logical.Nodes)
	}
	if len(again.Inspection.Profile.Diagnostics) > 0 && again.Inspection.Profile.Diagnostics[0].Fields[0].Name == "mutated" {
		t.Fatalf("profile metadata leaked mutation: %#v", again.Inspection.Profile.Diagnostics)
	}
	if again.Rows[0].Kind != QueryKindSelect {
		t.Fatalf("row leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Ordinal" || again.ResultSchema.Columns[0].Name != "Ordinal" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != string(QueryKindSelect) {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
