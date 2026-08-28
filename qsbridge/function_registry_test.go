package qsbridge

import "testing"

func TestBuiltinFunctionDefinitionResolvesAliases(t *testing.T) {
	function, ok := BuiltinScalarFunctionDefinition("SUBSTRING")
	if !ok {
		t.Fatalf("expected substring alias to resolve")
	}
	if function.Name != "substr" {
		t.Fatalf("Name = %q, want substr", function.Name)
	}
	if function.ReturnType != DataTypeString {
		t.Fatalf("ReturnType = %q, want string", function.ReturnType)
	}

	function, ok = BuiltinScalarFunctionDefinition("HOUR")
	if !ok {
		t.Fatalf("expected hour alias to resolve")
	}
	if function.Name != "hourofday" {
		t.Fatalf("Name = %q, want hourofday", function.Name)
	}
	if function.ReturnType != DataTypeInt {
		t.Fatalf("ReturnType = %q, want int", function.ReturnType)
	}
}

func TestBuiltinFunctionDefinitionsAreDefensiveCopies(t *testing.T) {
	functions := BuiltinFunctionDefinitions()
	if len(functions) == 0 {
		t.Fatalf("expected built-in functions")
	}
	functions[0].Name = "mutated"
	functions[0].Contexts = append(functions[0].Contexts, FunctionContextTableSelector)

	again := BuiltinFunctionDefinitions()
	if again[0].Name == "mutated" {
		t.Fatalf("built-in function definitions leaked caller mutation")
	}
	if len(again[0].Contexts) != len(builtinFunctionDefinitions[0].Contexts) {
		t.Fatalf("built-in context slice leaked caller mutation")
	}

	substr, ok := BuiltinScalarFunctionDefinition("substr")
	if !ok {
		t.Fatalf("expected substr function")
	}
	substr.Aliases[0] = "mutated"
	againSubstr, ok := BuiltinScalarFunctionDefinition("substring")
	if !ok {
		t.Fatalf("expected substring alias after caller mutation")
	}
	if againSubstr.Aliases[0] == "mutated" {
		t.Fatalf("built-in alias slice leaked caller mutation")
	}
}

func TestBuiltinAggregateFunctionMetadata(t *testing.T) {
	if !IsBuiltinAggregateFunction("topn") {
		t.Fatalf("expected topn aggregate")
	}
	if got, want := BuiltinAggregateReturnType("avg"), DataTypeFloat; got != want {
		t.Fatalf("avg return type = %q, want %q", got, want)
	}
	if got, want := BuiltinAggregateReturnType("topn"), DataTypeString; got != want {
		t.Fatalf("topn return type = %q, want %q", got, want)
	}
	if IsBuiltinScalarFunction("topn") {
		t.Fatalf("topn should not be scalar")
	}
}

func TestBuiltinFunctionContextBinding(t *testing.T) {
	if !IsBuiltinSQLScalarFunction("substring") {
		t.Fatalf("expected substring to bind as SQL scalar")
	}
	if IsBuiltinSQLScalarFunction("now") {
		t.Fatalf("now should not bind as SQL scalar until runtime supports it")
	}
	if !IsBuiltinCatalogScalarFunction("current_timestamp", CatalogExpressionPurposeColumnDefault) {
		t.Fatalf("expected current_timestamp to bind as catalog default function")
	}
	if IsBuiltinCatalogScalarFunction("current_timestamp", CatalogExpressionPurposeTableSelector) {
		t.Fatalf("current_timestamp should not bind as streaming selector function")
	}
	if !IsBuiltinCatalogScalarFunction("hash.sha256", CatalogExpressionPurposeTableSelector) {
		t.Fatalf("expected deterministic hash function to bind in table selectors")
	}
}

func TestBuiltinSQLFunctionDefinitionsExcludeCatalogOnlyFunctions(t *testing.T) {
	for _, function := range BuiltinSQLFunctionDefinitions() {
		if function.Matches("now") {
			t.Fatalf("SQL function definitions included catalog-only now(): %#v", function)
		}
	}
}
