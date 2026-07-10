package qsbridge

import "testing"

func TestPlanningServiceListClientAccessRequirementsReturnsPreparedRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	orders := TableInstance{ID: "orders:o", Schema: "quanta", Table: "orders", Alias: "o"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Type: DataTypeInt}
	totalPrice := FieldRef{Table: orders, Name: "o_totalprice", Type: DataTypeFloat}
	prepared := PreparedPlan{
		SQL:            "select o_orderkey from orders where o_totalprice > ?",
		DefaultSchema:  "quanta",
		CatalogVersion: "v1",
		Session:        SessionContext{User: "moli"},
		Query: QueryIR{
			Sources: []TableInstance{orders},
			Projection: []ProjectionColumn{{
				Alias: "o_orderkey",
				Expr:  Field(orderKey),
			}},
			Predicates: []Predicate{{
				Expr: Binary(BinaryOpGreater, Field(totalPrice), Parameter(1, DataTypeFloat)),
			}},
		},
		Supported: true,
	}

	exchange := service.ListClientAccessRequirements(connection, prepared)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported access requirement metadata", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one table access requirement", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.Ordinal != 1 || row.Privilege != AccessSelect || row.Table != "orders" || row.Alias != "o" {
		t.Fatalf("row = %#v, want select requirement for orders alias o", row)
	}
	if len(row.Fields) != 2 {
		t.Fatalf("fields = %#v, want predicate and projection fields", row.Fields)
	}
	if len(exchange.ResultSchema.Columns) != 6 || exchange.ResultSchema.Columns[0].Name != "Ordinal" || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want access requirement result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[1].Value != string(AccessSelect) || resultRow[3].Value != "orders" {
		t.Fatalf("result row = %#v, want select orders requirement", resultRow)
	}
}

func TestPlanningServiceListClientAccessRequirementsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientAccessRequirements(connection, PreparedPlan{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientAccessRequirementsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	orders := TableInstance{ID: "orders:o", Schema: "quanta", Table: "orders", Alias: "o"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Type: DataTypeInt}
	prepared := PreparedPlan{
		Query: QueryIR{
			Sources:    []TableInstance{orders},
			Projection: []ProjectionColumn{{Alias: "o_orderkey", Expr: Field(orderKey)}},
		},
		Supported: true,
	}

	exchange := service.ListClientAccessRequirements(connection, prepared)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Prepared.Query.Sources[0].Table = "mutated"
	exchange.Rows[0].Fields[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][3].Value = "mutated"

	again := service.ListClientAccessRequirements(connection, prepared)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Prepared.Query.Sources[0].Table != "orders" {
		t.Fatalf("prepared plan leaked mutation: %#v", again.Prepared.Query.Sources)
	}
	if again.Rows[0].Fields[0].Name != "o_orderkey" {
		t.Fatalf("row leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Ordinal" || again.ResultSchema.Columns[0].Name != "Ordinal" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][3].Value != "orders" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
