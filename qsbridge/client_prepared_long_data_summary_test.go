package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientPreparedLongDataReturnsStoreRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	preparedRegistry := NewMemoryPreparedStatementRegistry()
	longDataRegistry := NewMemoryPreparedLongDataRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	description := preparedRegistry.Register(PreparedPlan{
		Handle:     PreparedStatementHandle{Name: "stmt_blob"},
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeString, Nullable: true}},
		Supported:  true,
	})
	stored := service.StoreClientPreparedLongData(connection, preparedRegistry, longDataRegistry, PreparedLongDataFragment{
		Handle:     description.Handle,
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 12,
		Final:      true,
	})

	exchange := service.SummarizeClientPreparedLongData(connection, stored)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported long-data summary", exchange)
	}
	row := exchange.Rows[0]
	if row.StatementID != description.Handle.ID || row.StatementName != description.Handle.Name || row.Parameter == "" {
		t.Fatalf("row = %#v, want handle and parameter metadata", row)
	}
	if row.ChunkBytes != 12 || !row.FinalFragment || !row.Stored || row.StateChunks != 1 || row.StateTotalBytes != 12 || !row.StateFinal || !row.Supported {
		t.Fatalf("row = %#v, want stored long-data counters", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 12 {
		t.Fatalf("result/schema = %#v/%#v, want long-data summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != int(description.Handle.ID) || resultRow[4].Value != 12 || resultRow[6].Value != true {
		t.Fatalf("result row = %#v, want long-data summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPreparedLongDataReportsValidationDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	stored := service.StoreClientPreparedLongData(connection, NewMemoryPreparedStatementRegistry(), NewMemoryPreparedLongDataRegistry(), PreparedLongDataFragment{
		Handle:     PreparedStatementHandle{ID: 99},
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 12,
	})

	exchange := service.SummarizeClientPreparedLongData(connection, stored)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, validation diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported || exchange.Rows[0].Stored {
		t.Fatalf("rows = %#v, want unsupported long-data row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientPreparedLongDataFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientPreparedLongData(connection, ClientPreparedLongDataExchange{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block long-data summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientPreparedLongDataCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	preparedRegistry := NewMemoryPreparedStatementRegistry()
	longDataRegistry := NewMemoryPreparedLongDataRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	description := preparedRegistry.Register(PreparedPlan{
		Handle:     PreparedStatementHandle{Name: "stmt_blob"},
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeString, Nullable: true}},
		Supported:  true,
	})
	stored := service.StoreClientPreparedLongData(connection, preparedRegistry, longDataRegistry, PreparedLongDataFragment{
		Handle:     description.Handle,
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 12,
	})

	exchange := service.SummarizeClientPreparedLongData(connection, stored)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.LongData.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].StatementName = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.SummarizeClientPreparedLongData(connection, stored)
	if again.Connection.Attributes["client"] != "mysql" || again.LongData.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v/%#v", again.Connection.Attributes, again.LongData.Connection.Attributes)
	}
	if again.Rows[0].StatementName != description.Handle.Name {
		t.Fatalf("row leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Statement_id" || again.ResultSchema.Columns[0].Name != "Statement_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != description.Handle.Name {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
