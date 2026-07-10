package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientParsePreviewReturnsUnboundShapeCounts(t *testing.T) {
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

	exchange := service.SummarizeClientParsePreview(bundle)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported parse preview summary", exchange)
	}
	if parser.count != 1 {
		t.Fatalf("parser calls = %d, want one parse", parser.count)
	}
	row := exchange.Row
	if row.StatementCount != 1 || row.SelectCount != 1 || row.TableCount != 1 || row.ProjectionCount != 1 {
		t.Fatalf("row = %#v, want statement and table/projection counts", row)
	}
	if row.MembershipCount != 1 || row.PredicateCount != 1 || row.GroupByCount != 1 || row.AggregateCount != 1 || row.OrderByCount != 1 {
		t.Fatalf("row = %#v, want select-shape counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 17 {
		t.Fatalf("result/schema = %#v/%#v, want parse preview summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 1 || resultRow[1].Value != 1 || resultRow[9].Value != 1 {
		t.Fatalf("result row = %#v, want parse preview summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientParsePreviewReportsParserDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser: stubParserBridge{
			diagnostics: DiagnosticSet{
				ErrorDiagnostic(DiagnosticParserBoundary, PhaseParse, "could not parse"),
			},
		},
	}, nil)
	connection := clientStatementConnection()
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select")

	exchange := service.SummarizeClientParsePreview(bundle)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want parser diagnostics to block preview summary", exchange)
	}
	if exchange.Row.StatementCount != 1 || exchange.Row.DiagnosticCount != 1 {
		t.Fatalf("row = %#v, want failed statement counted", exchange.Row)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 17 {
		t.Fatalf("result/schema = %#v/%#v, want failed parse preview summary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientParsePreviewCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{Parser: stubParserBridge{statement: UnboundStatement{
		Kind:   QueryKindSelect,
		Select: UnboundSelect{Tables: []UnboundTable{{Name: "orders"}}},
	}}}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select * from orders")

	exchange := service.SummarizeClientParsePreview(bundle)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Statements[0].Unbound.Select.Tables[0].Name = "mutated"
	exchange.Row.StatementCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientParsePreview(bundle)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Statements[0].Unbound.Select.Tables[0].Name != "orders" {
		t.Fatalf("statement leaked mutation: %#v", again.Statements[0].Unbound)
	}
	if again.Row.StatementCount != 1 || again.Row.TableCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("parse preview summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}
