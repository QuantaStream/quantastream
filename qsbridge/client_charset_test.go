package qsbridge

import "testing"

func TestPlanningServiceListClientCharacterSetsFiltersAndBuildsResult(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	charsets := []ClientCharacterSet{
		{Name: "latin1", Description: "cp1252 West European", DefaultCollation: "latin1_swedish_ci", MaxLen: 1},
		{Name: "utf8mb4", Description: "UTF-8 Unicode", DefaultCollation: "utf8mb4_0900_ai_ci", MaxLen: 4},
	}

	exchange := service.ListClientCharacterSets(connection, charsets, "utf8%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported character set metadata", exchange)
	}
	if exchange.Pattern != "utf8%" || len(exchange.CharacterSets) != 1 || exchange.CharacterSets[0].Name != "utf8mb4" {
		t.Fatalf("exchange = %#v, want filtered utf8 character set", exchange)
	}
	if len(exchange.ResultSchema.Columns) != 4 || exchange.ResultSchema.Columns[0].Name != "Charset" || exchange.ResultSchema.Columns[3].Name != "Maxlen" {
		t.Fatalf("schema = %#v, want character set columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][2].Value != "utf8mb4_0900_ai_ci" || exchange.Result.Chunks[0].Rows[0][3].Value != 4 {
		t.Fatalf("result rows = %#v, want utf8mb4 character set row", exchange.Result.Chunks)
	}
}

func TestPlanningServiceListClientCollationsFiltersByNameOrCharset(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	collations := []ClientCollation{
		{Name: "latin1_swedish_ci", CharacterSet: "latin1", ID: 8, Default: true, Compiled: true, SortLen: 1},
		{Name: "utf8mb4_bin", CharacterSet: "utf8mb4", ID: 46, Compiled: true, SortLen: 1},
		{Name: "utf8mb4_0900_ai_ci", CharacterSet: "utf8mb4", ID: 255, Default: true, Compiled: true, SortLen: 0},
	}

	exchange := service.ListClientCollations(connection, collations, "utf8mb4")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported collation metadata", exchange)
	}
	if len(exchange.Collations) != 2 || exchange.Collations[0].Name != "utf8mb4_0900_ai_ci" || exchange.Collations[1].Name != "utf8mb4_bin" {
		t.Fatalf("collations = %#v, want sorted utf8mb4 collations", exchange.Collations)
	}
	if len(exchange.ResultSchema.Columns) != 6 || exchange.ResultSchema.Columns[0].Name != "Collation" || exchange.ResultSchema.Columns[2].Name != "Id" {
		t.Fatalf("schema = %#v, want collation columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 2 || exchange.Result.Chunks[0].Rows[0][2].Value != 255 || exchange.Result.Chunks[0].Rows[0][3].Value != true {
		t.Fatalf("result rows = %#v, want collation metadata rows", exchange.Result.Chunks)
	}
}

func TestPlanningServiceListClientCharsetMetadataCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	charsets := []ClientCharacterSet{{Name: "utf8mb4", DefaultCollation: "utf8mb4_0900_ai_ci", MaxLen: 4}}
	collations := []ClientCollation{{Name: "utf8mb4_0900_ai_ci", CharacterSet: "utf8mb4", ID: 255, Default: true}}

	charsetExchange := service.ListClientCharacterSets(connection, charsets, "")
	charsetExchange.Connection.Attributes["client"] = "mutated"
	charsetExchange.CharacterSets[0].Name = "mutated"
	charsetExchange.Result.Columns[0].Name = "mutated"
	charsetExchange.ResultSchema.Columns[0].Name = "mutated"
	charsetExchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	collationExchange := service.ListClientCollations(connection, collations, "")
	collationExchange.Collations[0].Name = "mutated"
	collationExchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	charsetAgain := service.ListClientCharacterSets(connection, charsets, "")
	if charsetAgain.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", charsetAgain.Connection.Attributes)
	}
	if charsetAgain.CharacterSets[0].Name != "utf8mb4" {
		t.Fatalf("character sets leaked mutation: %#v", charsetAgain.CharacterSets)
	}
	if charsetAgain.Result.Columns[0].Name != "Charset" || charsetAgain.ResultSchema.Columns[0].Name != "Charset" {
		t.Fatalf("character set result metadata leaked mutation: %#v/%#v", charsetAgain.Result.Columns, charsetAgain.ResultSchema.Columns)
	}
	if charsetAgain.Result.Chunks[0].Rows[0][0].Value != "utf8mb4" {
		t.Fatalf("character set rows leaked mutation: %#v", charsetAgain.Result.Chunks)
	}

	collationAgain := service.ListClientCollations(connection, collations, "")
	if collationAgain.Collations[0].Name != "utf8mb4_0900_ai_ci" {
		t.Fatalf("collations leaked mutation: %#v", collationAgain.Collations)
	}
	if collationAgain.Result.Chunks[0].Rows[0][0].Value != "utf8mb4_0900_ai_ci" {
		t.Fatalf("collation rows leaked mutation: %#v", collationAgain.Result.Chunks)
	}
}

func TestPlanningServiceListClientCharsetMetadataReturnsFailedEnvelopeForDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "blocked"),
	}

	charsets := service.ListClientCharacterSets(connection, []ClientCharacterSet{{Name: "utf8mb4"}}, "")
	if charsets.Supported() || charsets.Result.Status != ExecutionFailed || !charsets.Result.Complete || len(charsets.CharacterSets) != 0 {
		t.Fatalf("character sets = %#v, want failed rowless envelope", charsets)
	}
	collations := service.ListClientCollations(connection, []ClientCollation{{Name: "utf8mb4_0900_ai_ci"}}, "")
	if collations.Supported() || collations.Result.Status != ExecutionFailed || !collations.Result.Complete || len(collations.Collations) != 0 {
		t.Fatalf("collations = %#v, want failed rowless envelope", collations)
	}
}
