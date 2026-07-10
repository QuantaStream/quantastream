package qsbridge

import "testing"

func TestPlanningServicePrepareClientMutationSummaryBuildsInsertRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	customers := TableInstance{ID: "customers", Table: "customers_qa", Alias: "c"}
	custID := FieldRef{Table: customers, Name: "cust_id", Type: DataTypeString}
	age := FieldRef{Table: customers, Name: "age", Type: DataTypeInt}
	prepared := PreparedPlan{
		Kind:       QueryKindInsert,
		Supported:  true,
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeString}, {Index: 2, Type: DataTypeInt}},
		Query: QueryIR{
			Kind: QueryKindInsert,
			Mutation: MutationShape{
				Kind:    MutationInsert,
				Target:  customers,
				Columns: []FieldRef{custID, age},
				Rows:    []MutationRow{{}, {}},
			},
		},
	}

	exchange := service.PrepareClientMutationSummary(connection, prepared)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported mutation summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one mutation summary row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.Kind != MutationInsert || row.Target.Table != "customers_qa" || row.ColumnCount != 2 || row.RowCount != 2 || row.ParameterCount != 2 {
		t.Fatalf("row = %#v, want insert shape counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 11 || exchange.ResultSchema.Columns[0].Name != "Kind" {
		t.Fatalf("schema = %#v, want mutation summary schema", exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(MutationInsert) || resultRow[1].Value != "customers_qa.as c" || resultRow[7].Value != "c.cust_id,c.age" {
		t.Fatalf("result row = %#v, want insert metadata", resultRow)
	}
}

func TestPlanningServicePrepareClientMutationSummaryBuildsUpdateRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	customers := TableInstance{ID: "customers", Table: "customers_qa"}
	age := FieldRef{Table: customers, Name: "age", Type: DataTypeInt}
	prepared := PreparedPlan{
		Kind:      QueryKindUpdate,
		Supported: true,
		Query: QueryIR{
			Kind: QueryKindUpdate,
			Mutation: MutationShape{
				Kind:        MutationUpdate,
				Target:      customers,
				Assignments: []MutationAssignment{{Field: age}},
				Predicates:  []Predicate{{Scope: PredicateScopeWhere}},
			},
		},
	}

	exchange := service.PrepareClientMutationSummary(connection, prepared)
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one update summary row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.Kind != MutationUpdate || row.AssignmentCount != 1 || row.PredicateCount != 1 || len(row.PredicateScopes) != 1 {
		t.Fatalf("row = %#v, want update shape counts", row)
	}
	if exchange.Result.Chunks[0].Rows[0][8].Value != string(PredicateScopeWhere) {
		t.Fatalf("result row = %#v, want predicate scope", exchange.Result.Chunks[0].Rows[0])
	}
}

func TestPlanningServicePrepareClientMutationSummaryReturnsNoRowsForSelect(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.PrepareClientMutationSummary(clientStatementConnection(), PreparedPlan{
		Kind:      QueryKindSelect,
		Supported: true,
		Query:     QueryIR{Kind: QueryKindSelect},
	})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported empty mutation summary", exchange)
	}
	if len(exchange.Rows) != 0 || exchange.Result.RowsReturned != 0 {
		t.Fatalf("exchange = %#v, want no mutation rows for select", exchange)
	}
}

func TestPlanningServicePrepareClientMutationSummaryReturnsFailedEnvelopeForBlockingDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticNativeBlocker, PhaseClassify, "unsupported")}
	exchange := service.PrepareClientMutationSummary(connection, PreparedPlan{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection diagnostics", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed mutation summary envelope", exchange.Result)
	}
}
