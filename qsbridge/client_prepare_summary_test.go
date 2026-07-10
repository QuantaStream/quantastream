package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientPrepareReturnsPrepareRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	sql := "select o_orderkey from orders where o_orderkey = ?"
	prepare := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		Handle:     PreparedStatementHandle{Name: "stmt_orders"},
		SQL:        sql,
	}, registry)

	exchange := service.SummarizeClientPrepare(connection, prepare)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported prepare summary", exchange)
	}
	row := exchange.Rows[0]
	if row.StatementID == 0 || row.StatementName != "stmt_orders" || !row.Registered || !row.Supported {
		t.Fatalf("row = %#v, want registered prepare metadata", row)
	}
	if row.Parameters != 1 || row.ResultColumns != 1 || row.SQLLength != len(sql) {
		t.Fatalf("row = %#v, want prepare shape counts", row)
	}
	if row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want read SELECT lifecycle metadata", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 12 {
		t.Fatalf("result/schema = %#v/%#v, want prepare summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[1].Value != "stmt_orders" || resultRow[3].Value != string(PhysicalAccessRead) || resultRow[4].Value != string(ClientPlanLifecycleSelect) || resultRow[6].Value != true || resultRow[8].Value != 1 {
		t.Fatalf("result row = %#v, want prepare summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPrepareReturnsMutationLifecycleRoute(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	prepare := ClientPrepareExchange{
		Connection: connection,
		Request: ClientPrepareRequest{
			Handle: PreparedStatementHandle{ID: 7, Name: "stmt_update"},
			SQL:    "update orders set o_totalprice = ? where o_orderkey = ?",
		},
		Description: PreparedPlanDescription{
			Handle:       PreparedStatementHandle{ID: 7, Name: "stmt_update"},
			Kind:         QueryKindUpdate,
			AccessIntent: PhysicalAccessWrite,
			Parameters:   []ParameterRef{{Index: 1, Type: DataTypeFloat}, {Index: 2, Type: DataTypeInt}},
			Result:       ResultShape{Kind: ResultStatement},
			Supported:    true,
		},
		Registered: true,
	}

	exchange := service.SummarizeClientPrepare(connection, prepare)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported mutation prepare summary", exchange)
	}
	row := exchange.Rows[0]
	if row.Kind != QueryKindUpdate || row.AccessIntent != PhysicalAccessWrite || row.Lifecycle != ClientPlanLifecycleMutation || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want update write mutation lifecycle route", row)
	}
	if row.Parameters != 2 || !row.Registered || !row.Supported {
		t.Fatalf("row = %#v, want registered update with two parameters", row)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[2].Value != string(QueryKindUpdate) || resultRow[3].Value != string(PhysicalAccessWrite) || resultRow[4].Value != string(ClientPlanLifecycleMutation) {
		t.Fatalf("result row = %#v, want mutation routing cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPrepareReportsPrepareDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	prepare := ClientPrepareExchange{
		Connection: connection,
		Request:    ClientPrepareRequest{SQL: "select * from missing"},
		Description: PreparedPlanDescription{
			SQL: "select * from missing",
			Diagnostics: DiagnosticSet{
				ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "missing table"),
			},
		},
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "missing table"),
		},
	}

	exchange := service.SummarizeClientPrepare(connection, prepare)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, prepare diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported || exchange.Rows[0].Registered {
		t.Fatalf("rows = %#v, want unsupported prepare row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticCatalogTableNotFound) {
		t.Fatalf("diagnostics = %#v, want catalog table not found", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientPrepareFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientPrepare(connection, ClientPrepareExchange{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block prepare summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientPrepareCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	prepare := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		Handle:     PreparedStatementHandle{Name: "stmt_orders"},
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, NewMemoryPreparedStatementRegistry())

	exchange := service.SummarizeClientPrepare(connection, prepare)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Prepare.Connection.Attributes["client"] = "mutated"
	exchange.Prepare.Description.Parameters[0].Type = DataTypeString
	exchange.Rows[0].StatementName = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.SummarizeClientPrepare(connection, prepare)
	if again.Connection.Attributes["client"] != "mysql" || again.Prepare.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v/%#v", again.Connection.Attributes, again.Prepare.Connection.Attributes)
	}
	if again.Prepare.Description.Parameters[0].Type != DataTypeInt || again.Rows[0].StatementName != "stmt_orders" {
		t.Fatalf("prepare summary leaked mutation: %#v/%#v", again.Prepare.Description, again.Rows)
	}
	if again.Result.Columns[0].Name != "Statement_id" || again.ResultSchema.Columns[0].Name != "Statement_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != "stmt_orders" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
