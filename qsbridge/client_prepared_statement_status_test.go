package qsbridge

import "testing"

func TestPlanningServiceListClientPreparedStatementsReturnsRegistryRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	prepared := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		Handle:     PreparedStatementHandle{Name: "stmt_orders"},
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, registry)

	exchange := service.ListClientPreparedStatements(connection, registry)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported prepared statement inventory", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one prepared handle row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.StatementID != prepared.Description.Handle.ID || row.StatementName != "stmt_orders" {
		t.Fatalf("row = %#v, want registered handle identity", row)
	}
	if row.Schema != "quanta" || row.User != "moli" || !row.Supported {
		t.Fatalf("row = %#v, want schema/user/supported metadata", row)
	}
	if row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want read/select lifecycle metadata", row)
	}
	if row.Parameters != 1 || row.ResultColumns != 1 || row.SQL != "select o_orderkey from orders where o_orderkey = ?" {
		t.Fatalf("row = %#v, want prepared shape metadata", row)
	}
	if len(exchange.ResultSchema.Columns) != 16 || exchange.ResultSchema.Columns[0].Name != "Statement_id" || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want prepared statement status result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != int(prepared.Description.Handle.ID) || resultRow[1].Value != "stmt_orders" || resultRow[6].Value != string(PhysicalAccessRead) || resultRow[7].Value != string(ClientPlanLifecycleSelect) {
		t.Fatalf("result row = %#v, want prepared handle identity", resultRow)
	}
}

func TestPlanningServiceListClientPreparedStatementsReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.ListClientPreparedStatements(clientStatementConnection(ClientCapabilityPreparedStatements), nil)

	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing registry to block inventory", exchange)
	}
	if !containsDiagnosticCode(exchange.ExchangeDiagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.ExchangeDiagnostics.Codes())
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed complete result", exchange.Result)
	}
}

func TestPlanningServiceListClientPreparedStatementsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientPreparedStatements(connection, NewMemoryPreparedStatementRegistry())
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block inventory", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless inventory", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientPreparedStatementsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		Handle:     PreparedStatementHandle{Name: "stmt_orders"},
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, registry)

	exchange := service.ListClientPreparedStatements(connection, registry)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].SQL = "mutated"
	exchange.Rows[0].Diagnostics = append(exchange.Rows[0].Diagnostics, DiagnosticRouteRejected)
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.ListClientPreparedStatements(connection, registry)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].SQL != "select o_orderkey from orders where o_orderkey = ?" || containsDiagnosticCode(again.Rows[0].Diagnostics, DiagnosticRouteRejected) {
		t.Fatalf("row leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Statement_id" || again.ResultSchema.Columns[0].Name != "Statement_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != "stmt_orders" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
