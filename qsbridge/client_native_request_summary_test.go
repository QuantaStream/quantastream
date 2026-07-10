package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientNativeRequestReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	prepared, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "native-1", MaxRows: 10, Streaming: true, Cancelable: true},
		IndexedParameterValue(1, ValueInt, 99),
	)
	if !prepared.Supported || !request.SupportedForExecution() {
		t.Fatalf("prepared/request = %#v/%#v, want supported native request", prepared, request)
	}

	exchange := service.SummarizeClientNativeRequest(connection, request)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported native summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one native request row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.Kind != ClientNativeRequestSingle || row.RequestID != "native-1" || row.User != "moli" {
		t.Fatalf("row = %#v, want single native request identity", row)
	}
	if !row.Supported || row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 || row.ResultKind != ResultQuery || row.ResultColumns != 1 || row.ParameterCount != 1 || row.MaxRows != 10 || !row.Streaming || !row.Cancelable {
		t.Fatalf("row = %#v, want native request shape", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 21 {
		t.Fatalf("result/schema = %#v/%#v, want native summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(ClientNativeRequestSingle) || resultRow[4].Value != true || resultRow[5].Value != string(PhysicalAccessRead) || resultRow[6].Value != string(ClientPlanLifecycleSelect) || resultRow[7].Value != 7 || resultRow[17].Value != 1 {
		t.Fatalf("result row = %#v, want native request cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientNativeBatchRequestReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()

	_, request := service.PrepareBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "native-batch-1", BatchSize: 25},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
		ParameterValues(IndexedParameterValue(1, ValueInt, 8)),
	)
	exchange := service.SummarizeClientNativeBatchRequest(connection, request)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported native batch summary", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Kind != ClientNativeRequestBatch || exchange.Rows[0].AccessIntent != PhysicalAccessRead || exchange.Rows[0].Lifecycle != ClientPlanLifecycleSelect || exchange.Rows[0].LifecycleSteps != 7 || exchange.Rows[0].ParameterSetCount != 2 {
		t.Fatalf("rows = %#v, want batch native request with two parameter sets", exchange.Rows)
	}
	if exchange.Result.Chunks[0].Rows[0][18].Value != 2 {
		t.Fatalf("result row = %#v, want parameter set count", exchange.Result.Chunks[0].Rows[0])
	}
}

func TestPlanningServiceSummarizeClientNativeRequestReportsWriteIntent(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	request := PreparedPlan{
		Kind:      QueryKindUpdate,
		Supported: true,
		Result:    ResultShape{Kind: ResultStatement},
		Statement: StatementResult{AffectedRows: 1},
	}.ExecutionRequest(ExecutionOptions{RequestID: "native-write"})

	exchange := service.SummarizeClientNativeRequest(connection, request)
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one native write row", exchange.Rows)
	}
	if exchange.Rows[0].AccessIntent != PhysicalAccessWrite || exchange.Rows[0].Lifecycle != ClientPlanLifecycleMutation || exchange.Rows[0].LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want write intent and mutation lifecycle", exchange.Rows[0])
	}
	if exchange.Result.Chunks[0].Rows[0][5].Value != string(PhysicalAccessWrite) || exchange.Result.Chunks[0].Rows[0][6].Value != string(ClientPlanLifecycleMutation) {
		t.Fatalf("result row = %#v, want write intent and mutation lifecycle cells", exchange.Result.Chunks[0].Rows[0])
	}
}

func TestPlanningServiceSummarizeClientNativeRequestKeepsInvalidRequestAsData(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)
	connection := clientStatementConnection()

	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueString, "bad"),
	)
	exchange := service.SummarizeClientNativeRequest(connection, request)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want metadata exchange supported", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported native request row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticParameterTypeMismatch) {
		t.Fatalf("diagnostics = %#v, want parameter mismatch", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientNativeRequestCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	_, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "native-1"},
		IndexedParameterValue(1, ValueInt, 99),
	)
	exchange := service.SummarizeClientNativeRequest(connection, request)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].SQL = "mutated"
	exchange.Result.Chunks[0].Rows[0][20].Value = "mutated"

	again := service.SummarizeClientNativeRequest(connection, request)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].SQL != "select o_orderkey from orders where o_orderkey = ?" || again.Result.Chunks[0].Rows[0][20].Value != "select o_orderkey from orders where o_orderkey = ?" {
		t.Fatalf("native summary leaked mutation: %#v/%#v", again.Rows, again.Result.Chunks)
	}
}
