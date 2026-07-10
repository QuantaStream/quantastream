package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientCharsetMetadataReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	characterSets := []ClientCharacterSet{
		{Name: "latin1", DefaultCollation: "latin1_swedish_ci", MaxLen: 1},
		{Name: "utf8mb4", DefaultCollation: "utf8mb4_0900_ai_ci", MaxLen: 4},
	}
	collations := []ClientCollation{
		{Name: "latin1_swedish_ci", CharacterSet: "latin1", ID: 8, Default: true, Compiled: true, SortLen: 1},
		{Name: "utf8mb4_bin", CharacterSet: "utf8mb4", ID: 46, Compiled: true, SortLen: 1},
		{Name: "utf8mb4_0900_ai_ci", CharacterSet: "utf8mb4", ID: 255, Default: true, Compiled: true, SortLen: 0},
	}

	exchange := service.SummarizeClientCharsetMetadata(connection, characterSets, collations, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported charset summary", exchange)
	}
	row := exchange.Row
	if row.CharacterSetCount != 2 || row.CollationCount != 3 || row.DefaultCollationCount != 2 {
		t.Fatalf("row = %#v, want charset/collation/default counts", row)
	}
	if row.CompiledCollationCount != 3 || row.MultiByteCharsetCount != 1 || row.ZeroSortLenCount != 1 {
		t.Fatalf("row = %#v, want compiled/multibyte/sortlen counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 6 {
		t.Fatalf("result/schema = %#v/%#v, want one-row charset summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 2 || resultRow[1].Value != 3 || resultRow[5].Value != 1 {
		t.Fatalf("result row = %#v, want charset summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientCharsetMetadataFiltersPattern(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	characterSets := []ClientCharacterSet{
		{Name: "latin1", DefaultCollation: "latin1_swedish_ci", MaxLen: 1},
		{Name: "utf8mb4", DefaultCollation: "utf8mb4_0900_ai_ci", MaxLen: 4},
	}
	collations := []ClientCollation{
		{Name: "latin1_swedish_ci", CharacterSet: "latin1", ID: 8, Default: true, Compiled: true, SortLen: 1},
		{Name: "utf8mb4_bin", CharacterSet: "utf8mb4", ID: 46, Compiled: true, SortLen: 1},
		{Name: "utf8mb4_0900_ai_ci", CharacterSet: "utf8mb4", ID: 255, Default: true, Compiled: true, SortLen: 0},
	}

	exchange := service.SummarizeClientCharsetMetadata(connection, characterSets, collations, "utf8mb4")
	row := exchange.Row
	if row.CharacterSetCount != 1 || row.CollationCount != 2 || row.DefaultCollationCount != 1 {
		t.Fatalf("row = %#v, want filtered charset summary counts", row)
	}
	if row.CompiledCollationCount != 2 || row.MultiByteCharsetCount != 1 || row.ZeroSortLenCount != 1 {
		t.Fatalf("row = %#v, want filtered compiled/multibyte/sortlen counts", row)
	}
}

func TestPlanningServiceSummarizeClientCharsetMetadataReturnsFailedEnvelopeForDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.SummarizeClientCharsetMetadata(connection, []ClientCharacterSet{{Name: "utf8mb4", MaxLen: 4}}, nil, "")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if exchange.Row.CharacterSetCount != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 6 {
		t.Fatalf("row/result/schema = %#v/%#v/%#v, want failed charset summary envelope", exchange.Row, exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientCharsetMetadataCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	characterSets := []ClientCharacterSet{{Name: "utf8mb4", MaxLen: 4}}
	collations := []ClientCollation{{Name: "utf8mb4_0900_ai_ci", CharacterSet: "utf8mb4", Default: true, Compiled: true}}

	exchange := service.SummarizeClientCharsetMetadata(connection, characterSets, collations, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.CharacterSetCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientCharsetMetadata(connection, characterSets, collations, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.CharacterSetCount != 1 || again.Row.CollationCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("charset summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}
