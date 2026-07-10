package qsbridge

import "testing"

func TestPlanningServiceListClientFunctionUsagesReturnsPreparedRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	name := Field(FieldRef{Table: customer, Name: "c_name", Type: DataTypeString})
	nationKey := Field(FieldRef{Table: customer, Name: "c_nationkey", Type: DataTypeInt})
	acctbal := Field(FieldRef{Table: customer, Name: "c_acctbal", Type: DataTypeFloat})
	prepared := PreparedPlan{
		Query: QueryIR{
			Projection: []ProjectionColumn{
				{
					Expr: FunctionCall(FunctionDefinition{
						Name:          "lower",
						Kind:          FunctionScalar,
						Origin:        FunctionOriginMySQLCompatible,
						ReturnType:    DataTypeString,
						Deterministic: true,
					}, name),
					Alias: "name_lower",
					Type:  DataTypeString,
				},
			},
			Aggregates: []Aggregate{{
				Function:  "topn",
				Origin:    FunctionOriginQuantaCustom,
				Placement: FunctionPlacementAggregate,
				Type:      DataTypeString,
				Input:     Field(FieldRef{Table: customer, Name: "c_nationkey", Type: DataTypeInt}),
			}},
			Predicates: []Predicate{
				{
					Placement: PredicateResidualScan,
					Expr: FunctionCall(FunctionDefinition{
						Name:       "sample_stratified",
						Kind:       FunctionScalar,
						Origin:     FunctionOriginLegacyCustom,
						Placement:  FunctionPlacementPredicate,
						ReturnType: DataTypeBool,
					}, nationKey),
				},
			},
			OrderBy: []SortSpec{
				{
					Expr: FunctionCall(FunctionDefinition{
						Name:          "abs",
						Kind:          FunctionScalar,
						Origin:        FunctionOriginMySQLCompatible,
						ReturnType:    DataTypeFloat,
						Deterministic: true,
					}, acctbal),
				},
			},
		},
		Supported: true,
	}

	exchange := service.ListClientFunctionUsages(connection, prepared)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported function usage metadata", exchange)
	}
	if len(exchange.Rows) != 4 {
		t.Fatalf("rows = %#v, want four function usages", exchange.Rows)
	}
	assertClientFunctionUsage(t, exchange.Rows[0], 1, "lower", FunctionOriginMySQLCompatible, FunctionPlacementExpression, FunctionUsageProjection, DataTypeString, true)
	assertClientFunctionUsage(t, exchange.Rows[1], 2, "sample_stratified", FunctionOriginLegacyCustom, FunctionPlacementPredicate, FunctionUsagePredicate, DataTypeBool, false)
	assertClientFunctionUsage(t, exchange.Rows[2], 3, "topn", FunctionOriginQuantaCustom, FunctionPlacementAggregate, FunctionUsageAggregate, DataTypeString, false)
	assertClientFunctionUsage(t, exchange.Rows[3], 4, "abs", FunctionOriginMySQLCompatible, FunctionPlacementExpression, FunctionUsageOrderBy, DataTypeFloat, true)
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 4 || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("result/schema = %#v/%#v, want function usage rows", exchange.Result, exchange.ResultSchema)
	}
	row := exchange.Result.Chunks[0].Rows[1]
	if row[0].Value != 2 || row[1].Value != "sample_stratified" || row[2].Value != string(FunctionOriginLegacyCustom) || row[3].Value != string(FunctionPlacementPredicate) || row[4].Value != string(FunctionUsagePredicate) || row[5].Value != string(DataTypeBool) || row[6].Value != false {
		t.Fatalf("result row = %#v, want sampling predicate usage", row)
	}
}

func TestPlanningServiceListClientFunctionUsagesCarriesConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientFunctionUsages(connection, PreparedPlan{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported function usage metadata", exchange)
	}
	if !containsDiagnosticCode(exchange.ExchangeDiagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.ExchangeDiagnostics.Codes())
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("result/schema = %#v/%#v, want failed function usage envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientFunctionUsagesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	prepared := PreparedPlan{
		Query: QueryIR{
			Projection: []ProjectionColumn{
				{
					Expr: FunctionCall(FunctionDefinition{
						Name:       "lower",
						Kind:       FunctionScalar,
						ReturnType: DataTypeString,
					}),
				},
			},
		},
		Supported: true,
	}

	exchange := service.ListClientFunctionUsages(connection, prepared)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Prepared.Query.Projection[0].Alias = "mutated"
	exchange.Rows[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.ListClientFunctionUsages(connection, prepared)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Prepared.Query.Projection[0].Alias != "" || again.Rows[0].Name != "lower" || again.Result.Chunks[0].Rows[0][1].Value != "lower" {
		t.Fatalf("metadata leaked mutation: %#v/%#v/%#v", again.Prepared.Query.Projection, again.Rows, again.Result.Chunks)
	}
}

func assertClientFunctionUsage(t *testing.T, row ClientFunctionUsageRow, ordinal int, name string, origin FunctionOrigin, placement FunctionPlacement, context FunctionUsageContext, returnType DataType, deterministic bool) {
	t.Helper()
	if row.Ordinal != ordinal || row.Name != name || row.Origin != origin || row.Placement != placement || row.Context != context || row.ReturnType != returnType || row.Deterministic != deterministic {
		t.Fatalf("row = %#v, want ordinal=%d name=%s origin=%s placement=%s context=%s return=%s deterministic=%v", row, ordinal, name, origin, placement, context, returnType, deterministic)
	}
}
