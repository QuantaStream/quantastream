package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientPreparedExecutionReturnsExecuteRows(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
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
	executed := service.ExecuteClientPreparedStatement(connection, registry, prepared.Description.Handle, ClientPreparedExecutionOptions{
		Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)},
	})

	exchange := service.SummarizeClientPreparedExecution(connection, executed)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported prepared execution summary", exchange)
	}
	row := exchange.Rows[0]
	if row.Operation != ClientPreparedExecutionSingle || row.StatementID != prepared.Description.Handle.ID || row.StatementName != "stmt_orders" {
		t.Fatalf("row = %#v, want prepared execute handle metadata", row)
	}
	if row.Handoff != ExecutionHandoffNative || row.ResponseKind != ClientResponseQuery || row.Bindings != 1 || row.ParameterSets != 1 || !row.Supported {
		t.Fatalf("row = %#v, want native prepared execute metadata", row)
	}
	if row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want read SELECT lifecycle metadata", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 14 {
		t.Fatalf("result/schema = %#v/%#v, want prepared execution summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(ClientPreparedExecutionSingle) || resultRow[4].Value != string(PhysicalAccessRead) || resultRow[5].Value != string(ClientPlanLifecycleSelect) || resultRow[7].Value != string(ExecutionHandoffNative) || resultRow[10].Value != 1 {
		t.Fatalf("result row = %#v, want execute summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPreparedExecutionReportsWriteLifecycleRoute(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	executed := ClientPreparedExecutionExchange{
		Handle: PreparedStatementHandle{ID: 7, Name: "stmt_update"},
		Prepared: PreparedPlan{
			Kind:      QueryKindUpdate,
			Supported: true,
			Result:    ResultShape{Kind: ResultStatement},
		},
		Response: ClientResponseItem{Kind: ClientResponseStatement},
	}

	exchange := service.SummarizeClientPreparedExecution(connection, executed)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported write execution summary", exchange)
	}
	row := exchange.Rows[0]
	if row.Kind != QueryKindUpdate || row.AccessIntent != PhysicalAccessWrite || row.Lifecycle != ClientPlanLifecycleMutation || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want update write mutation lifecycle route", row)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[3].Value != string(QueryKindUpdate) || resultRow[4].Value != string(PhysicalAccessWrite) || resultRow[5].Value != string(ClientPlanLifecycleMutation) {
		t.Fatalf("result row = %#v, want write routing cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPreparedBatchExecutionReturnsBatchRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityBatchExecution)
	prepared := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		Handle:     PreparedStatementHandle{Name: "stmt_orders"},
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, registry)
	executed := service.ExecuteClientPreparedBatchStatement(connection, registry, prepared.Description.Handle, ClientPreparedBatchExecutionOptions{
		ParameterSets: []ParameterValueSet{
			ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
			ParameterValues(IndexedParameterValue(1, ValueInt, 8)),
		},
	})

	exchange := service.SummarizeClientPreparedBatchExecution(connection, executed)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported prepared batch summary", exchange)
	}
	row := exchange.Rows[0]
	if row.Operation != ClientPreparedExecutionBatch || row.Handoff != ExecutionHandoffNative || row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.ParameterSets != 2 || !row.Supported {
		t.Fatalf("row = %#v, want native batch execution metadata", row)
	}
}

func TestPlanningServiceSummarizeClientPreparedExecutionReportsExecutionDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	executed := service.ExecuteClientPreparedStatement(connection, NewMemoryPreparedStatementRegistry(), PreparedStatementHandle{ID: 99}, ClientPreparedExecutionOptions{})

	exchange := service.SummarizeClientPreparedExecution(connection, executed)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, execution diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported execution row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientPreparedExecutionFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientPreparedExecution(connection, ClientPreparedExecutionExchange{Handle: PreparedStatementHandle{ID: 1}})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block prepared execution summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientPreparedExecutionCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	prepared := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		Handle:     PreparedStatementHandle{Name: "stmt_orders"},
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, registry)
	executed := service.ExecuteClientPreparedStatement(connection, registry, prepared.Description.Handle, ClientPreparedExecutionOptions{
		Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)},
	})

	exchange := service.SummarizeClientPreparedExecution(connection, executed)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].StatementName = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.SummarizeClientPreparedExecution(connection, executed)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].StatementName != "stmt_orders" {
		t.Fatalf("row leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Operation" || again.ResultSchema.Columns[0].Name != "Operation" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != "stmt_orders" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
