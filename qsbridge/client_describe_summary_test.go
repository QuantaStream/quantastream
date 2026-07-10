package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientDescribeReturnsQueryRow(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	described := service.DescribeClientStatement(ClientDescribeStatementRequest{
		Connection: connection,
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	})

	exchange := service.SummarizeClientDescribe(described)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported describe summary", exchange)
	}
	row := exchange.Rows[0]
	if row.Source != "sql" || row.Kind != QueryKindSelect || !row.Supported || row.Parameters != 1 || row.ResultColumns != 1 {
		t.Fatalf("row = %#v, want query describe metadata", row)
	}
	if row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want read/select lifecycle metadata", row)
	}
	if !row.HasResultSchema || row.HasStatementResponse || row.SQLLength == 0 {
		t.Fatalf("row = %#v, want result schema describe flags", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 15 {
		t.Fatalf("result/schema = %#v/%#v, want describe summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "sql" || resultRow[3].Value != string(QueryKindSelect) || resultRow[4].Value != string(PhysicalAccessRead) || resultRow[8].Value != 1 {
		t.Fatalf("result row = %#v, want describe summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientDescribeReturnsPreparedRow(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	prepared := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		Handle:     PreparedStatementHandle{Name: "stmt_orders"},
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, registry)
	described := service.DescribeClientPreparedStatement(connection, registry, prepared.Description.Handle)

	exchange := service.SummarizeClientDescribe(described)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported prepared describe summary", exchange)
	}
	row := exchange.Rows[0]
	if row.Source != "prepared" || row.StatementID != prepared.Description.Handle.ID || row.StatementName != "stmt_orders" {
		t.Fatalf("row = %#v, want prepared handle metadata", row)
	}
}

func TestPlanningServiceSummarizeClientDescribeReportsDescribeDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	described := service.DescribeClientPreparedStatement(connection, nil, PreparedStatementHandle{ID: 1})

	exchange := service.SummarizeClientDescribe(described)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, describe diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported describe row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientDescribeFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}
	described := service.DescribeClientStatement(ClientDescribeStatementRequest{Connection: connection, SQL: "select 1"})

	exchange := service.SummarizeClientDescribe(described)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless summary", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientDescribeCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	described := service.DescribeClientStatement(ClientDescribeStatementRequest{
		Connection: connection,
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	})

	exchange := service.SummarizeClientDescribe(described)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Describe.Description.Parameters[0].Type = DataTypeString
	exchange.Describe.ResultSchema.Columns[0].Name = "mutated"
	exchange.Rows[0].Source = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.SummarizeClientDescribe(described)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Describe.Description.Parameters[0].Type != DataTypeInt {
		t.Fatalf("description metadata leaked mutation: %#v", again.Describe.Description.Parameters)
	}
	if again.Describe.ResultSchema.Columns[0].Name != "order_id" {
		t.Fatalf("schema metadata leaked mutation: %#v", again.Describe.ResultSchema.Columns)
	}
	if again.Rows[0].Source != "sql" {
		t.Fatalf("row leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Source" || again.ResultSchema.Columns[0].Name != "Source" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "sql" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
