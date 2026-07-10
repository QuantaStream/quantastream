package qsbridge

import "testing"

func TestPlanningServiceListClientResultPayloadSummaryReturnsColumnShape(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	execution := ExecutionResult{
		RequestID: "payload-1",
		Status:    ExecutionComplete,
		Kind:      ResultQuery,
		Columns: []ResultColumn{
			{Name: "id", Type: DataTypeInt},
			{Name: "name", Type: DataTypeString},
		},
		Chunks: []ResultChunk{
			{
				Sequence: 1,
				Rows: []ResultRow{
					{metadataIntCell(1), metadataStringCell("Abe")},
					{metadataIntCell(2), {Kind: ValueNull, Value: nil}},
				},
			},
			{
				Sequence: 2,
				Rows: []ResultRow{
					{metadataIntCell(3)},
					nil,
				},
				Final: true,
			},
		},
	}

	exchange := service.ListClientResultPayloadSummary(connection, execution)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported payload summary", exchange)
	}
	if len(exchange.Rows) != 2 {
		t.Fatalf("rows = %#v, want two payload columns", exchange.Rows)
	}
	first := exchange.Rows[0]
	if first.Ordinal != 1 || first.ColumnName != "id" || first.LogicalType != DataTypeInt || first.Cells != 3 || first.MissingCells != 0 || first.NullCells != 0 {
		t.Fatalf("first row = %#v, want id payload shape", first)
	}
	second := exchange.Rows[1]
	if second.Ordinal != 2 || second.ColumnName != "name" || second.LogicalType != DataTypeString || second.Cells != 2 || second.MissingCells != 1 || second.NullCells != 1 {
		t.Fatalf("second row = %#v, want name payload shape", second)
	}
	if joinValueKinds(second.ValueKinds) != "null,string" {
		t.Fatalf("value kinds = %#v, want null,string", second.ValueKinds)
	}
	if exchange.Result.RowsReturned != 2 || len(exchange.ResultSchema.Columns) != 9 {
		t.Fatalf("result/schema = %#v/%#v, want payload summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[1]
	if resultRow[2].Value != "name" || resultRow[5].Value != 2 || resultRow[6].Value != 1 || resultRow[8].Value != "null,string" {
		t.Fatalf("result row = %#v, want second payload cells", resultRow)
	}
}

func TestPlanningServiceListClientResultPayloadSummaryIncludesExtraCells(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	execution := ExecutionResult{
		RequestID: "payload-1",
		Status:    ExecutionComplete,
		Kind:      ResultQuery,
		Columns:   []ResultColumn{{Name: "id", Type: DataTypeInt}},
		Chunks: []ResultChunk{{
			Rows: []ResultRow{
				{metadataIntCell(1), metadataStringCell("extra")},
			},
			Final: true,
		}},
	}

	exchange := service.ListClientResultPayloadSummary(connection, execution)
	if len(exchange.Rows) != 2 {
		t.Fatalf("rows = %#v, want declared and extra payload ordinals", exchange.Rows)
	}
	extra := exchange.Rows[1]
	if extra.Ordinal != 2 || extra.ColumnName != "" || extra.LogicalType != DataTypeUnknown || extra.Cells != 1 {
		t.Fatalf("extra row = %#v, want unnamed extra payload shape", extra)
	}
}

func TestPlanningServiceListClientResultPayloadSummaryCarriesDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	execution := ExecutionResult{
		RequestID: "payload-1",
		Status:    ExecutionFailed,
		Columns:   []ResultColumn{{Name: "id", Type: DataTypeInt}},
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseExecute, "payload failed"),
		},
	}

	exchange := service.ListClientResultPayloadSummary(connection, execution)
	if exchange.Supported() {
		t.Fatalf("expected execution diagnostics to block payload summary")
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].ColumnName != "id" {
		t.Fatalf("rows = %#v, want metadata row even when diagnostics block result", exchange.Rows)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed summary envelope", exchange.Result)
	}
}

func TestPlanningServiceListClientResultPayloadSummaryCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	execution := ExecutionResult{
		RequestID: "payload-1",
		Status:    ExecutionComplete,
		Kind:      ResultQuery,
		Columns:   []ResultColumn{{Name: "id", Type: DataTypeInt}},
		Chunks: []ResultChunk{{
			Rows:  []ResultRow{{metadataIntCell(1)}},
			Final: true,
		}},
	}

	exchange := service.ListClientResultPayloadSummary(connection, execution)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Execution.RequestID = "mutated"
	exchange.Rows[0].ColumnName = "mutated"
	exchange.Rows[0].ValueKinds[0] = ValueString
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.ListClientResultPayloadSummary(connection, execution)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Execution.RequestID != "payload-1" || again.Rows[0].ColumnName != "id" || joinValueKinds(again.Rows[0].ValueKinds) != "int" {
		t.Fatalf("payload summary leaked mutation: execution=%#v rows=%#v", again.Execution, again.Rows)
	}
	if again.Result.Columns[0].Name != "Request_id" || again.ResultSchema.Columns[0].Name != "Request_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != "id" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
