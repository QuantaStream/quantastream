package qsbridge

import "testing"

func TestPlanningServicePreviewClientParseReturnsUnboundShape(t *testing.T) {
	parser := &countingParserBridge{statement: UnboundStatement{
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables:      []UnboundTable{{Name: "orders"}},
			Projection:  []UnboundProjection{{Expr: UnboundField("", "o_orderkey")}},
			Predicates:  []UnboundPredicate{{Expr: UnboundBinary(BinaryOpEqual, UnboundField("", "o_orderstatus"), UnboundLiteral(ValueString, "F"))}},
			GroupBy:     []UnboundExpr{UnboundField("", "o_orderstatus")},
			Aggregates:  []UnboundAggregate{{Function: "count", CountAll: true}},
			OrderBy:     []UnboundSort{{Expr: UnboundField("", "o_orderstatus")}},
			Memberships: []UnboundMembership{{LeftField: "o_custkey", RightField: "c_custkey"}},
		},
	}}
	service := NewPlanningService(Planner{Parser: parser}, nil)
	connection := clientStatementConnection()
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select count(*) from orders")

	exchange := service.PreviewClientParse(bundle)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported parse preview", exchange)
	}
	if parser.count != 1 {
		t.Fatalf("parser calls = %d, want one parse", parser.count)
	}
	if len(exchange.Statements) != 1 || exchange.Statements[0].Unbound.Kind != QueryKindSelect {
		t.Fatalf("statements = %#v, want one select statement", exchange.Statements)
	}
	row := exchange.Result.Chunks[0].Rows[0]
	if row[0].Value != 0 || row[1].Value != string(QueryKindSelect) || row[2].Value != 1 || row[5].Value != 1 || row[8].Value != 1 {
		t.Fatalf("row = %#v, want select shape counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 14 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want parse preview row", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServicePreviewClientParseReportsParserDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser: stubParserBridge{
			diagnostics: DiagnosticSet{
				ErrorDiagnostic(DiagnosticParserBoundary, PhaseParse, "could not parse"),
			},
		},
	}, nil)
	connection := clientStatementConnection()
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select")

	exchange := service.PreviewClientParse(bundle)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want parser diagnostics to block preview", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticParserBoundary) {
		t.Fatalf("diagnostics = %#v, want parser boundary diagnostic", exchange.Diagnostics)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed parse preview", exchange.Result)
	}
}

func TestPlanningServicePreviewClientParseRejectsNilParser(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")

	exchange := service.PreviewClientParse(bundle)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want nil parser rejection", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInternalInvariant) {
		t.Fatalf("diagnostics = %#v, want nil parser invariant", exchange.Diagnostics)
	}
}

func TestPlanningServicePreviewClientParseCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{Parser: stubParserBridge{statement: UnboundStatement{
		Kind:   QueryKindSelect,
		Select: UnboundSelect{Tables: []UnboundTable{{Name: "orders"}}},
	}}}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select * from orders")

	exchange := service.PreviewClientParse(bundle)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Statements[0].Unbound.Select.Tables[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.PreviewClientParse(bundle)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Statements[0].Unbound.Select.Tables[0].Name != "orders" {
		t.Fatalf("statement leaked mutation: %#v", again.Statements[0].Unbound)
	}
	if again.Result.Columns[0].Name != "Ordinal" || again.ResultSchema.Columns[0].Name != "Ordinal" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != string(QueryKindSelect) {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
