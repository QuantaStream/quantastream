package qsbridge

// ClientSessionVariablesSummaryExchange is adapter-facing metadata for session-variable aggregates.
type ClientSessionVariablesSummaryExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Row          ClientSessionVariablesSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientSessionVariables returns aggregate SHOW VARIABLES-style metadata for the current session.
func (s PlanningService) SummarizeClientSessionVariables(connection ConnectionContext, pattern string) ClientSessionVariablesSummaryExchange {
	_ = s
	exchange := ClientSessionVariablesSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeClientSessionVariables(filterClientSessionVariables(sessionVariables(connection.Session), pattern))
	}
	exchange.Result = exchange.sessionVariablesSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether session variable summary metadata can be returned.
func (e ClientSessionVariablesSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts session variable summary diagnostics into protocol-facing errors.
func (e ClientSessionVariablesSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking session variable summary error, if any.
func (e ClientSessionVariablesSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientSessionVariablesSummaryExchange) sessionVariablesSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     sessionVariablesSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{sessionVariablesSummaryResultRow(e.Row)},
		Final: true,
	})
}

func sessionVariablesSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Variable_count", Type: DataTypeInt},
		{Name: "Built_in_count", Type: DataTypeInt},
		{Name: "Adapter_variable_count", Type: DataTypeInt},
		{Name: "Empty_value_count", Type: DataTypeInt},
		{Name: "Numeric_value_count", Type: DataTypeInt},
		{Name: "Selected_schema_count", Type: DataTypeInt},
		{Name: "SQL_mode_count", Type: DataTypeInt},
		{Name: "Time_zone_count", Type: DataTypeInt},
	}
}

func sessionVariablesSummaryResultRow(row ClientSessionVariablesSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.VariableCount),
		metadataIntCell(row.BuiltInCount),
		metadataIntCell(row.AdapterVariableCount),
		metadataIntCell(row.EmptyValueCount),
		metadataIntCell(row.NumericValueCount),
		metadataIntCell(row.SelectedSchemaCount),
		metadataIntCell(row.SQLModeCount),
		metadataIntCell(row.TimeZoneCount),
	}
}
