package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientAccessRequirementsReturnsPreparedCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	orders := TableInstance{ID: "orders:o", Schema: "quanta", Table: "orders", Alias: "o"}
	customers := TableInstance{ID: "customers:c", Schema: "quanta", Table: "customers", Alias: "c"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Type: DataTypeInt}
	totalPrice := FieldRef{Table: orders, Name: "o_totalprice", Type: DataTypeFloat}
	customerName := FieldRef{Table: customers, Name: "c_name", Type: DataTypeString}
	prepared := PreparedPlan{
		Query: QueryIR{
			Sources: []TableInstance{orders, customers},
			Projection: []ProjectionColumn{
				{Alias: "o_orderkey", Expr: Field(orderKey)},
				{Alias: "c_name", Expr: Field(customerName)},
			},
			Predicates: []Predicate{{
				Expr: Binary(BinaryOpGreater, Field(totalPrice), Literal(ValueFloat, 100.0)),
			}},
		},
		Supported: true,
	}

	exchange := service.SummarizeClientAccessRequirements(connection, prepared)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported access requirement summary", exchange)
	}
	row := exchange.Row
	if row.RequirementCount != 2 || row.SelectCount != 2 || row.TableCount != 2 || row.FieldCount != 3 {
		t.Fatalf("row = %#v, want two select table requirements and three fields", row)
	}
	if row.HasMutation || row.InsertCount != 0 || row.UpdateCount != 0 || row.DeleteCount != 0 {
		t.Fatalf("row = %#v, did not expect mutation counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 2 || resultRow[1].Value != 2 || resultRow[6].Value != 3 {
		t.Fatalf("result row = %#v, want access summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientAccessRequirementsReportsMutationCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	customers := TableInstance{ID: "customers", Schema: "quanta", Table: "customers"}
	city := FieldRef{Table: customers, Name: "city", Type: DataTypeString}
	age := FieldRef{Table: customers, Name: "age", Type: DataTypeInt}
	prepared := PreparedPlan{
		Query: QueryIR{
			Mutation: MutationShape{
				Kind:   MutationUpdate,
				Target: customers,
				Columns: []FieldRef{
					city,
					age,
				},
			},
		},
		Supported: true,
	}

	exchange := service.SummarizeClientAccessRequirements(connection, prepared)
	if exchange.Row.RequirementCount != 1 || exchange.Row.UpdateCount != 1 || !exchange.Row.HasMutation {
		t.Fatalf("row = %#v, want one update mutation requirement", exchange.Row)
	}
	if exchange.Row.TableCount != 1 || exchange.Row.FieldCount != 2 {
		t.Fatalf("row = %#v, want one table and two mutation fields", exchange.Row)
	}
}

func TestPlanningServiceSummarizeClientAccessRequirementsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientAccessRequirements(connection, PreparedPlan{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || exchange.Result.RowsReturned != 0 {
		t.Fatalf("result = %#v, want failed rowless summary", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientAccessRequirementsCopiesMutableState(t *testing.T) {
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

	exchange := service.SummarizeClientAccessRequirements(connection, prepared)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Prepared.Query.Sources[0].Table = "mutated"
	exchange.Row.RequirementCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientAccessRequirements(connection, prepared)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Prepared.Query.Sources[0].Table != "orders" {
		t.Fatalf("prepared plan leaked mutation: %#v", again.Prepared.Query.Sources)
	}
	if again.Row.RequirementCount != 1 {
		t.Fatalf("row leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Requirement_count" || again.ResultSchema.Columns[0].Name != "Requirement_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
