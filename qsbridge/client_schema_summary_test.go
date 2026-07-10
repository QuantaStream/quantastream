package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientUseSchemaReturnsSelectionRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	selection := service.PrepareClientUseSchema(connection, nil, "analytics", ClientSchemaSelectionOptions{})

	exchange := service.SummarizeClientUseSchema(connection, selection)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported schema selection summary", exchange)
	}
	row := exchange.Rows[0]
	if row.RequestedSchema != "analytics" || row.PreviousSchema != "quanta" || row.NextSchema != "analytics" {
		t.Fatalf("row = %#v, want schema transition metadata", row)
	}
	if row.Applied || row.SessionActions != 1 || row.Status != "Database changed" || !row.Supported {
		t.Fatalf("row = %#v, want schema selection response metadata", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("result/schema = %#v/%#v, want schema selection summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "analytics" || resultRow[1].Value != "quanta" || resultRow[5].Value != "Database changed" {
		t.Fatalf("result row = %#v, want schema summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientUseSchemaReportsSelectionDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	selection := service.PrepareClientUseSchema(connection, nil, "", ClientSchemaSelectionOptions{})

	exchange := service.SummarizeClientUseSchema(connection, selection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, selection diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported schema selection row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientUseSchemaFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientUseSchema(connection, ClientSchemaSelectionExchange{Schema: "analytics"})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block schema summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientUseSchemaCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Session.CurrentSchema = "quanta"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	selection := service.PrepareClientUseSchema(connection, nil, "analytics", ClientSchemaSelectionOptions{})

	exchange := service.SummarizeClientUseSchema(connection, selection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Selection.Connection.Attributes["client"] = "mutated"
	exchange.Selection.Response.SessionActions[0].Value = "mutated"
	exchange.Rows[0].NextSchema = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.SummarizeClientUseSchema(connection, selection)
	if again.Connection.Attributes["client"] != "mysql" || again.Selection.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v/%#v", again.Connection.Attributes, again.Selection.Connection.Attributes)
	}
	if again.Selection.Response.SessionActions[0].Value != "analytics" || again.Rows[0].NextSchema != "analytics" {
		t.Fatalf("schema selection summary leaked mutation: %#v/%#v", again.Selection.Response, again.Rows)
	}
	if again.Result.Columns[0].Name != "Requested_schema" || again.ResultSchema.Columns[0].Name != "Requested_schema" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != "analytics" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
