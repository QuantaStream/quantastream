package qsbridge

import "testing"

func TestPlanningServiceListClientPreparedLongDataReturnsRegistryRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	registry := NewMemoryPreparedLongDataRegistry()
	handle := PreparedStatementHandle{ID: 7, Name: "stmt_blob"}
	registry.Append(PreparedLongDataFragment{
		Handle:     handle,
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 12,
	})
	registry.Append(PreparedLongDataFragment{
		Handle:     handle,
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 5,
		Final:      true,
	})

	exchange := service.ListClientPreparedLongData(connection, registry)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported long-data inventory", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one long-data state row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.StatementID != 7 || row.StatementName != "stmt_blob" || row.Parameter == "" {
		t.Fatalf("row = %#v, want handle and parameter metadata", row)
	}
	if row.Chunks != 2 || row.TotalBytes != 17 || !row.Final {
		t.Fatalf("row = %#v, want accumulated long-data state", row)
	}
	if len(exchange.ResultSchema.Columns) != 7 || exchange.ResultSchema.Columns[0].Name != "Statement_id" || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want long-data status result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 7 || resultRow[4].Value != 2 || resultRow[5].Value != 17 {
		t.Fatalf("result row = %#v, want long-data counters", resultRow)
	}
}

func TestPlanningServiceListClientPreparedLongDataReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.ListClientPreparedLongData(clientStatementConnection(ClientCapabilityPreparedStatements), nil)

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

func TestPlanningServiceListClientPreparedLongDataFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientPreparedLongData(connection, NewMemoryPreparedLongDataRegistry())
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block inventory", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless inventory", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientPreparedLongDataCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	registry := NewMemoryPreparedLongDataRegistry()
	registry.Append(PreparedLongDataFragment{
		Handle:     PreparedStatementHandle{ID: 7, Name: "stmt_blob"},
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 12,
	})

	exchange := service.ListClientPreparedLongData(connection, registry)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].StatementName = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.ListClientPreparedLongData(connection, registry)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].StatementName != "stmt_blob" {
		t.Fatalf("row leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Statement_id" || again.ResultSchema.Columns[0].Name != "Statement_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != "stmt_blob" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
