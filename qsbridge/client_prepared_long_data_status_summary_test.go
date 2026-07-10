package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientPreparedLongDataStatusReturnsRegistryCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	registry := NewMemoryPreparedLongDataRegistry()
	first := PreparedStatementHandle{ID: 7, Name: "stmt_blob"}
	second := PreparedStatementHandle{ID: 8}
	registry.Append(PreparedLongDataFragment{
		Handle:     first,
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 12,
	})
	registry.Append(PreparedLongDataFragment{
		Handle:     first,
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 5,
		Final:      true,
	})
	registry.Append(PreparedLongDataFragment{
		Handle:     second,
		Parameter:  IndexedParameterValue(2, ValueInt, nil),
		ChunkBytes: 20,
	})

	exchange := service.SummarizeClientPreparedLongDataStatus(connection, registry)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported long-data status summary", exchange)
	}
	row := exchange.Row
	if row.StateCount != 2 || row.NamedStatementCount != 1 || row.FinalStateCount != 1 {
		t.Fatalf("row = %#v, want state/name/final counts", row)
	}
	if row.StringKindCount != 1 || row.TotalChunks != 3 || row.TotalBytes != 37 || row.LargestStateBytes != 20 {
		t.Fatalf("row = %#v, want kind/chunk/byte totals", row)
	}
	if row.DistinctStatementCount != 2 {
		t.Fatalf("row = %#v, want two distinct prepared handles", row)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one long-data status summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 2 || resultRow[4].Value != 3 || resultRow[5].Value != 37 {
		t.Fatalf("result row = %#v, want long-data status summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPreparedLongDataStatusReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.SummarizeClientPreparedLongDataStatus(clientStatementConnection(ClientCapabilityPreparedStatements), nil)

	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing registry to block summary", exchange)
	}
	if !containsDiagnosticCode(exchange.ExchangeDiagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.ExchangeDiagnostics.Codes())
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed complete result", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientPreparedLongDataStatusCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	registry := NewMemoryPreparedLongDataRegistry()
	registry.Append(PreparedLongDataFragment{
		Handle:     PreparedStatementHandle{ID: 7, Name: "stmt_blob"},
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 12,
	})

	exchange := service.SummarizeClientPreparedLongDataStatus(connection, registry)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.StateCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientPreparedLongDataStatus(connection, registry)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.StateCount != 1 || again.Row.TotalChunks != 1 || again.Row.TotalBytes != 12 {
		t.Fatalf("row leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "State_count" || again.ResultSchema.Columns[0].Name != "State_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
