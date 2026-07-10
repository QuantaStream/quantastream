package qsbridge

import "strings"

// ClientCatalogFunctionsExchange is client-facing function metadata.
type ClientCatalogFunctionsExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Functions    []FunctionDefinition
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientCatalogFunctions returns registered SQL function metadata for adapters.
func (s PlanningService) ListClientCatalogFunctions(connection ConnectionContext, catalog CatalogFunctionMetadata, pattern string) ClientCatalogFunctionsExchange {
	_ = s
	exchange := ClientCatalogFunctionsExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.catalogFunctionsResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if catalog == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, catalogMetadataUnsupportedDiagnostics())
		exchange.Result = exchange.catalogFunctionsResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	functions, diagnostics := catalog.ListFunctions()
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Functions = filterCatalogFunctions(functions, pattern)
	}
	exchange.Result = exchange.catalogFunctionsResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether function metadata can be returned.
func (e ClientCatalogFunctionsExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts function metadata diagnostics into protocol-facing errors.
func (e ClientCatalogFunctionsExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking function metadata error, if any.
func (e ClientCatalogFunctionsExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCatalogFunctionsExchange) catalogFunctionsResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     catalogFunctionsResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.catalogFunctionRows(),
		Final: true,
	})
}

func catalogFunctionsResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Name", Type: DataTypeString},
		{Name: "Kind", Type: DataTypeString},
		{Name: "Return_type", Type: DataTypeString},
		{Name: "Arguments", Type: DataTypeString, Nullable: true},
		{Name: "Aliases", Type: DataTypeString, Nullable: true},
		{Name: "Origin", Type: DataTypeString, Nullable: true},
		{Name: "Placement", Type: DataTypeString, Nullable: true},
		{Name: "Native", Type: DataTypeBool},
		{Name: "Deterministic", Type: DataTypeBool},
	}
}

func (e ClientCatalogFunctionsExchange) catalogFunctionRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Functions))
	for _, function := range e.Functions {
		rows = append(rows, ResultRow{
			metadataStringCell(function.Name),
			metadataStringCell(string(function.Kind)),
			metadataStringCell(string(function.ReturnType)),
			metadataStringCell(joinDataTypes(function.Arguments)),
			metadataStringCell(strings.Join(function.Aliases, ",")),
			metadataStringCell(string(function.Origin)),
			metadataStringCell(string(function.EffectivePlacement())),
			metadataBoolCell(function.Native),
			metadataBoolCell(function.Deterministic),
		})
	}
	return rows
}

func filterCatalogFunctions(functions []FunctionDefinition, pattern string) []FunctionDefinition {
	cloned := cloneFunctionDefinitions(functions)
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloned
	}
	filtered := make([]FunctionDefinition, 0, len(cloned))
	for _, function := range cloned {
		if catalogFieldPatternMatch(pattern, function.Name) {
			filtered = append(filtered, function)
			continue
		}
		for _, alias := range function.Aliases {
			if catalogFieldPatternMatch(pattern, alias) {
				filtered = append(filtered, function)
				break
			}
		}
	}
	return filtered
}

func joinDataTypes(types []DataType) string {
	if len(types) == 0 {
		return ""
	}
	values := make([]string, 0, len(types))
	for _, dataType := range types {
		values = append(values, string(dataType))
	}
	return strings.Join(values, ",")
}
