package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientPreparedMetadataReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	prepared := service.PrepareWithRequest(connection.PlanRequest(
		"select o_orderkey from orders where o_orderkey = ?",
		ConnectionPlanOptions{},
	)).WithHandle(PreparedStatementHandle{ID: 7, Name: "stmt_orders"})

	exchange := service.SummarizeClientPreparedMetadata(connection, prepared)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported prepare metadata summary", exchange)
	}
	row := exchange.Row
	if row.RowCount != 2 || row.ParameterCount != 1 || row.ResultColumnCount != 1 {
		t.Fatalf("row = %#v, want parameter and result counts", row)
	}
	if row.ReadIntentCount != 2 || row.WriteIntentCount != 0 || row.SelectLifecycleCount != 2 || row.MutationLifecycleCount != 0 {
		t.Fatalf("row = %#v, want read/select lifecycle counts", row)
	}
	if row.NullableCount != 1 || row.SourceCount != 1 || row.FlaggedCount != 2 {
		t.Fatalf("row = %#v, want nullable/source/flag counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want prepared metadata summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 2 || resultRow[1].Value != 1 || resultRow[5].Value != 2 || resultRow[8].Value != 1 {
		t.Fatalf("result row = %#v, want prepared metadata summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPreparedMetadataReportsBlockingDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	prepared := PreparedPlan{
		SQL:       "select * from missing",
		Supported: false,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "missing table"),
		},
	}

	exchange := service.SummarizeClientPreparedMetadata(connection, prepared)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported prepare metadata summary", exchange)
	}
	if exchange.Row.RowCount != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("row/result/schema = %#v/%#v/%#v, want failed prepared metadata summary envelope", exchange.Row, exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientPreparedMetadataCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	prepared := service.PrepareWithRequest(connection.PlanRequest(
		"select o_orderkey from orders where o_orderkey = ?",
		ConnectionPlanOptions{},
	)).WithHandle(PreparedStatementHandle{ID: 7, Name: "stmt_orders"})

	exchange := service.SummarizeClientPreparedMetadata(connection, prepared)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Prepared.Parameters[0].Type = DataTypeString
	exchange.Row.RowCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientPreparedMetadata(connection, prepared)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Prepared.Parameters[0].Type != DataTypeInt {
		t.Fatalf("prepared metadata leaked mutation: %#v", again.Prepared.Parameters)
	}
	if again.Row.RowCount != 2 || again.Row.ParameterCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 2 {
		t.Fatalf("prepared metadata summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}
