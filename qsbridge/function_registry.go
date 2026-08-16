package qsbridge

// BuiltinFunctionDefinitions returns the shared built-in function metadata used
// by parser, binder, runtime, catalog defaults, and future streaming selectors.
func BuiltinFunctionDefinitions() []FunctionDefinition {
	return cloneFunctionDefinitions(builtinFunctionDefinitions)
}

// BuiltinFunctionDefinition resolves one built-in function by canonical name or alias.
func BuiltinFunctionDefinition(name string) (FunctionDefinition, bool) {
	for _, function := range builtinFunctionDefinitions {
		if function.Matches(name) {
			return cloneFunctionDefinition(function), true
		}
	}
	return FunctionDefinition{}, false
}

// BuiltinFunctionDefinitionsForContext returns built-ins that may bind in context.
func BuiltinFunctionDefinitionsForContext(context FunctionBindingContext) []FunctionDefinition {
	matched := make([]FunctionDefinition, 0, len(builtinFunctionDefinitions))
	for _, function := range builtinFunctionDefinitions {
		if function.SupportsContext(context) {
			matched = append(matched, cloneFunctionDefinition(function))
		}
	}
	return matched
}

// BuiltinSQLFunctionDefinitions returns functions that may bind in SQL text.
func BuiltinSQLFunctionDefinitions() []FunctionDefinition {
	matched := make([]FunctionDefinition, 0, len(builtinFunctionDefinitions))
	for _, function := range builtinFunctionDefinitions {
		if function.SupportsContext(FunctionContextSQLExpression) || function.SupportsContext(FunctionContextSQLAggregate) {
			matched = append(matched, cloneFunctionDefinition(function))
		}
	}
	return matched
}

// BuiltinFunctionDefinitionForContext resolves one built-in function allowed in context.
func BuiltinFunctionDefinitionForContext(name string, context FunctionBindingContext) (FunctionDefinition, bool) {
	function, ok := BuiltinFunctionDefinition(name)
	if !ok || !function.SupportsContext(context) {
		return FunctionDefinition{}, false
	}
	return function, true
}

// BuiltinScalarFunctionDefinition resolves one scalar built-in by canonical name or alias.
func BuiltinScalarFunctionDefinition(name string) (FunctionDefinition, bool) {
	function, ok := BuiltinFunctionDefinition(name)
	if !ok || function.Kind != FunctionScalar {
		return FunctionDefinition{}, false
	}
	return function, true
}

// BuiltinScalarFunctionDefinitionForContext resolves one scalar built-in allowed in context.
func BuiltinScalarFunctionDefinitionForContext(name string, context FunctionBindingContext) (FunctionDefinition, bool) {
	function, ok := BuiltinFunctionDefinitionForContext(name, context)
	if !ok || function.Kind != FunctionScalar {
		return FunctionDefinition{}, false
	}
	return function, true
}

// BuiltinAggregateFunctionDefinition resolves one aggregate built-in by canonical name or alias.
func BuiltinAggregateFunctionDefinition(name string) (FunctionDefinition, bool) {
	function, ok := BuiltinFunctionDefinition(name)
	if !ok || function.Kind != FunctionAggregate {
		return FunctionDefinition{}, false
	}
	return function, true
}

// BuiltinAggregateFunctionDefinitionForContext resolves one aggregate built-in allowed in context.
func BuiltinAggregateFunctionDefinitionForContext(name string, context FunctionBindingContext) (FunctionDefinition, bool) {
	function, ok := BuiltinFunctionDefinitionForContext(name, context)
	if !ok || function.Kind != FunctionAggregate {
		return FunctionDefinition{}, false
	}
	return function, true
}

// IsBuiltinScalarFunction reports whether name is a registered scalar function.
func IsBuiltinScalarFunction(name string) bool {
	_, ok := BuiltinScalarFunctionDefinition(name)
	return ok
}

// IsBuiltinSQLScalarFunction reports whether name is a registered SQL scalar function.
func IsBuiltinSQLScalarFunction(name string) bool {
	_, ok := BuiltinScalarFunctionDefinitionForContext(name, FunctionContextSQLExpression)
	return ok
}

// IsBuiltinCatalogScalarFunction reports whether name is a registered catalog expression function.
func IsBuiltinCatalogScalarFunction(name string, purpose CatalogExpressionPurpose) bool {
	context := FunctionContextCatalogDefault
	if purpose == CatalogExpressionPurposeTableSelector {
		context = FunctionContextTableSelector
	}
	_, ok := BuiltinScalarFunctionDefinitionForContext(name, context)
	return ok
}

// IsBuiltinAggregateFunction reports whether name is a registered aggregate function.
func IsBuiltinAggregateFunction(name string) bool {
	_, ok := BuiltinAggregateFunctionDefinition(name)
	return ok
}

// IsBuiltinSQLAggregateFunction reports whether name is a registered SQL aggregate function.
func IsBuiltinSQLAggregateFunction(name string) bool {
	_, ok := BuiltinAggregateFunctionDefinitionForContext(name, FunctionContextSQLAggregate)
	return ok
}

// BuiltinAggregateReturnType returns the registered aggregate return type.
func BuiltinAggregateReturnType(name string) DataType {
	function, ok := BuiltinAggregateFunctionDefinition(name)
	if !ok {
		return DataTypeUnknown
	}
	return function.ReturnType
}

func builtinScalarFunction(name string, returnType DataType, contexts []FunctionBindingContext, aliases ...string) FunctionDefinition {
	return FunctionDefinition{
		Name:          name,
		Aliases:       append([]string(nil), aliases...),
		Kind:          FunctionScalar,
		Origin:        FunctionOriginMySQLCompatible,
		Placement:     FunctionPlacementExpression,
		Contexts:      append([]FunctionBindingContext(nil), contexts...),
		ReturnType:    returnType,
		Native:        true,
		Deterministic: true,
	}
}

func builtinVolatileScalarFunction(name string, returnType DataType, contexts []FunctionBindingContext, aliases ...string) FunctionDefinition {
	function := builtinScalarFunction(name, returnType, contexts, aliases...)
	function.Deterministic = false
	return function
}

func builtinAggregateFunction(name string, returnType DataType) FunctionDefinition {
	return FunctionDefinition{
		Name:          name,
		Kind:          FunctionAggregate,
		Origin:        FunctionOriginMySQLCompatible,
		Placement:     FunctionPlacementAggregate,
		Contexts:      []FunctionBindingContext{FunctionContextSQLAggregate},
		ReturnType:    returnType,
		Native:        true,
		Deterministic: true,
	}
}

func builtinTopNAggregateFunction() FunctionDefinition {
	return FunctionDefinition{
		Name:          "topn",
		Kind:          FunctionAggregate,
		Origin:        FunctionOriginQuantaCustom,
		Placement:     FunctionPlacementAggregate,
		Contexts:      []FunctionBindingContext{FunctionContextSQLAggregate},
		ReturnType:    DataTypeString,
		Native:        true,
		Deterministic: true,
	}
}

var sqlExpressionFunctionContexts = []FunctionBindingContext{FunctionContextSQLExpression}
var catalogDefaultFunctionContexts = []FunctionBindingContext{FunctionContextCatalogDefault}
var sqlAndCatalogExpressionFunctionContexts = []FunctionBindingContext{
	FunctionContextSQLExpression,
	FunctionContextCatalogDefault,
	FunctionContextTableSelector,
}

var builtinFunctionDefinitions = []FunctionDefinition{
	builtinAggregateFunction("count", DataTypeInt),
	builtinAggregateFunction("sum", DataTypeFloat),
	builtinAggregateFunction("min", DataTypeFloat),
	builtinAggregateFunction("max", DataTypeFloat),
	builtinAggregateFunction("avg", DataTypeFloat),
	builtinTopNAggregateFunction(),
	builtinScalarFunction("todate", DataTypeTime, sqlExpressionFunctionContexts),
	builtinScalarFunction("tostring", DataTypeString, sqlExpressionFunctionContexts),
	builtinScalarFunction("toint", DataTypeInt, sqlExpressionFunctionContexts),
	builtinScalarFunction("tonumber", DataTypeFloat, sqlExpressionFunctionContexts),
	builtinVolatileScalarFunction("database", DataTypeString, sqlExpressionFunctionContexts, "schema"),
	builtinVolatileScalarFunction("version", DataTypeString, sqlExpressionFunctionContexts),
	builtinVolatileScalarFunction("user", DataTypeString, sqlExpressionFunctionContexts),
	builtinVolatileScalarFunction("current_user", DataTypeString, sqlExpressionFunctionContexts),
	builtinVolatileScalarFunction("connection_id", DataTypeInt, sqlExpressionFunctionContexts),
	builtinVolatileScalarFunction("qs_session_variable", DataTypeUnknown, sqlExpressionFunctionContexts),
	builtinScalarFunction("lower", DataTypeString, sqlAndCatalogExpressionFunctionContexts, "lcase"),
	builtinScalarFunction("upper", DataTypeString, sqlAndCatalogExpressionFunctionContexts, "ucase"),
	builtinScalarFunction("length", DataTypeInt, sqlAndCatalogExpressionFunctionContexts, "char_length"),
	builtinScalarFunction("substr", DataTypeString, sqlExpressionFunctionContexts, "substring", "mid"),
	builtinScalarFunction("concat", DataTypeString, sqlExpressionFunctionContexts),
	builtinScalarFunction("trim", DataTypeString, sqlExpressionFunctionContexts),
	builtinScalarFunction("ltrim", DataTypeString, sqlExpressionFunctionContexts),
	builtinScalarFunction("rtrim", DataTypeString, sqlExpressionFunctionContexts),
	builtinScalarFunction("replace", DataTypeString, sqlExpressionFunctionContexts),
	builtinScalarFunction("left", DataTypeString, sqlExpressionFunctionContexts),
	builtinScalarFunction("right", DataTypeString, sqlExpressionFunctionContexts),
	builtinScalarFunction("coalesce", DataTypeUnknown, sqlExpressionFunctionContexts),
	builtinScalarFunction("ifnull", DataTypeUnknown, sqlExpressionFunctionContexts),
	builtinScalarFunction("nullif", DataTypeUnknown, sqlExpressionFunctionContexts),
	builtinScalarFunction("abs", DataTypeFloat, sqlExpressionFunctionContexts),
	builtinScalarFunction("round", DataTypeFloat, sqlExpressionFunctionContexts),
	builtinScalarFunction("timediff", DataTypeInt, sqlExpressionFunctionContexts),
	builtinScalarFunction("year", DataTypeInt, sqlExpressionFunctionContexts, "yy"),
	builtinScalarFunction("mm", DataTypeInt, sqlExpressionFunctionContexts, "monthofyear", "month"),
	builtinScalarFunction("yymm", DataTypeInt, sqlExpressionFunctionContexts),
	builtinScalarFunction("day", DataTypeInt, sqlExpressionFunctionContexts, "dayofmonth"),
	builtinScalarFunction("dayofweek", DataTypeInt, sqlExpressionFunctionContexts),
	builtinScalarFunction("hourofday", DataTypeInt, sqlExpressionFunctionContexts),
	builtinScalarFunction("hourofweek", DataTypeInt, sqlExpressionFunctionContexts),
	builtinScalarFunction("seconds", DataTypeInt, sqlExpressionFunctionContexts),
	builtinScalarFunction("hash.sha256", DataTypeString, sqlAndCatalogExpressionFunctionContexts),
	builtinVolatileScalarFunction("now", DataTypeTime, catalogDefaultFunctionContexts, "current_timestamp"),
	builtinScalarFunction("unixmillis", DataTypeInt, []FunctionBindingContext{FunctionContextCatalogDefault}, "unix_millis"),
}
