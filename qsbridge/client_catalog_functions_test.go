package qsbridge

import "testing"

func TestPlanningServiceListClientCatalogFunctionsReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	catalog := MemoryCatalog{Functions: []FunctionDefinition{
		{Name: "sample_stratified", Kind: FunctionScalar, Origin: FunctionOriginLegacyCustom, Placement: FunctionPlacementPredicate, Arguments: []DataType{DataTypeString, DataTypeFloat}, Native: true},
		{Name: "sum", Kind: FunctionAggregate, Origin: FunctionOriginMySQLCompatible, Arguments: []DataType{DataTypeFloat}, ReturnType: DataTypeFloat, Native: true},
		{Name: "substr", Kind: FunctionScalar, Origin: FunctionOriginMySQLCompatible, Arguments: []DataType{DataTypeString, DataTypeInt, DataTypeInt}, ReturnType: DataTypeString, Aliases: []string{"substring", "mid"}, Native: true, Deterministic: true},
		{Name: "topn", Kind: FunctionAggregate, Origin: FunctionOriginQuantaCustom, Arguments: []DataType{DataTypeString}, ReturnType: DataTypeString, Native: true},
	}}

	exchange := service.ListClientCatalogFunctions(connection, catalog, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported function metadata", exchange)
	}
	if len(exchange.Functions) != 4 || exchange.Functions[0].Name != "sample_stratified" || exchange.Functions[1].Name != "substr" || exchange.Functions[2].Name != "sum" || exchange.Functions[3].Name != "topn" {
		t.Fatalf("functions = %#v, want sorted function metadata", exchange.Functions)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 4 || len(exchange.ResultSchema.Columns) != 9 {
		t.Fatalf("result/schema = %#v/%#v, want function metadata rows", exchange.Result, exchange.ResultSchema)
	}
	row := exchange.Result.Chunks[0].Rows[1]
	if row[0].Value != "substr" || row[1].Value != "scalar" || row[2].Value != "string" || row[3].Value != "string,int,int" || row[4].Value != "substring,mid" || row[5].Value != string(FunctionOriginMySQLCompatible) || row[6].Value != string(FunctionPlacementExpression) || row[7].Value != true || row[8].Value != true {
		t.Fatalf("row = %#v, want substr function metadata", row)
	}
	sampling := exchange.Result.Chunks[0].Rows[0]
	if sampling[0].Value != "sample_stratified" || sampling[5].Value != string(FunctionOriginLegacyCustom) || sampling[6].Value != string(FunctionPlacementPredicate) {
		t.Fatalf("sampling row = %#v, want predicate-only legacy custom function metadata", sampling)
	}
	custom := exchange.Result.Chunks[0].Rows[3]
	if custom[0].Value != "topn" || custom[5].Value != string(FunctionOriginQuantaCustom) || custom[6].Value != string(FunctionPlacementAggregate) || custom[8].Value != false {
		t.Fatalf("custom row = %#v, want Quanta custom function metadata", custom)
	}
}

func TestPlanningServiceListClientCatalogFunctionsFiltersByNameOrAlias(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	catalog := MemoryCatalog{Functions: []FunctionDefinition{
		{Name: "lower", Kind: FunctionScalar, Aliases: []string{"lcase"}, ReturnType: DataTypeString},
		{Name: "upper", Kind: FunctionScalar, Aliases: []string{"ucase"}, ReturnType: DataTypeString},
	}}

	exchange := service.ListClientCatalogFunctions(connection, catalog, "lc%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported filtered function metadata", exchange)
	}
	if len(exchange.Functions) != 1 || exchange.Functions[0].Name != "lower" {
		t.Fatalf("functions = %#v, want alias-matched lower function", exchange.Functions)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][0].Value != "lower" {
		t.Fatalf("result rows = %#v, want lower row", exchange.Result.Chunks)
	}
}

func TestPlanningServiceListClientCatalogFunctionsReportsUnsupportedCatalog(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientCatalogFunctions(connection, nil, "")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported catalog", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 9 {
		t.Fatalf("result/schema = %#v/%#v, want failed function envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientCatalogFunctionsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	catalog := MemoryCatalog{Functions: []FunctionDefinition{{
		Name:    "lower",
		Aliases: []string{"lcase"},
	}}}

	exchange := service.ListClientCatalogFunctions(connection, catalog, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Functions[0].Aliases[0] = "mutated"
	exchange.Result.Chunks[0].Rows[0][4].Value = "mutated"

	again := service.ListClientCatalogFunctions(connection, catalog, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Functions[0].Aliases[0] != "lcase" || again.Result.Chunks[0].Rows[0][4].Value != "lcase" {
		t.Fatalf("function metadata leaked mutation: %#v/%#v", again.Functions, again.Result.Chunks)
	}
}
