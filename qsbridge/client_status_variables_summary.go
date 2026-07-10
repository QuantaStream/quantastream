package qsbridge

// ClientStatusVariablesSummaryExchange is adapter-facing metadata for status-variable aggregates.
type ClientStatusVariablesSummaryExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Row          ClientStatusVariablesSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientStatusVariables returns aggregate SHOW STATUS-style metadata supplied by the adapter.
func (s PlanningService) SummarizeClientStatusVariables(connection ConnectionContext, variables []ClientStatusVariable, pattern string) ClientStatusVariablesSummaryExchange {
	_ = s
	exchange := ClientStatusVariablesSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeClientStatusVariables(filterClientStatusVariables(variables, pattern))
	}
	exchange.Result = exchange.statusVariablesSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether status variable summary metadata can be returned.
func (e ClientStatusVariablesSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts status variable summary diagnostics into protocol-facing errors.
func (e ClientStatusVariablesSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking status variable summary error, if any.
func (e ClientStatusVariablesSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientStatusVariablesSummaryExchange) statusVariablesSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     statusVariablesSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{statusVariablesSummaryResultRow(e.Row)},
		Final: true,
	})
}

func statusVariablesSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Variable_count", Type: DataTypeInt},
		{Name: "Empty_value_count", Type: DataTypeInt},
		{Name: "Numeric_value_count", Type: DataTypeInt},
		{Name: "Command_status_count", Type: DataTypeInt},
		{Name: "Thread_status_count", Type: DataTypeInt},
		{Name: "Connection_status_count", Type: DataTypeInt},
	}
}

func statusVariablesSummaryResultRow(row ClientStatusVariablesSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.VariableCount),
		metadataIntCell(row.EmptyValueCount),
		metadataIntCell(row.NumericValueCount),
		metadataIntCell(row.CommandStatusCount),
		metadataIntCell(row.ThreadStatusCount),
		metadataIntCell(row.ConnectionStatusCount),
	}
}
