package qsbridge

// ClientStatisticsSummaryExchange is adapter-facing metadata for aggregate protocol statistics.
type ClientStatisticsSummaryExchange struct {
	Connection   ConnectionContext
	Variables    []ClientStatusVariable
	Summary      string
	Row          ClientStatisticsSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientStatistics returns aggregate metadata for adapter-supplied protocol statistics.
func (s PlanningService) SummarizeClientStatistics(connection ConnectionContext, variables []ClientStatusVariable) ClientStatisticsSummaryExchange {
	_ = s
	exchange := ClientStatisticsSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Variables = filterClientStatusVariables(variables, "")
		exchange.Summary = clientStatisticsSummary(exchange.Variables)
		exchange.Row = summarizeClientStatistics(exchange.Variables, exchange.Summary)
	}
	exchange.Result = exchange.statisticsSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether statistics summary metadata can be returned.
func (e ClientStatisticsSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts statistics summary diagnostics into protocol-facing errors.
func (e ClientStatisticsSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking statistics summary error, if any.
func (e ClientStatisticsSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientStatisticsSummaryExchange) statisticsSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     statisticsSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{statisticsSummaryResultRow(e.Row)},
		Final: true,
	})
}

func statisticsSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Variable_count", Type: DataTypeInt},
		{Name: "Summary_length", Type: DataTypeInt},
		{Name: "Empty_value_count", Type: DataTypeInt},
		{Name: "Numeric_value_count", Type: DataTypeInt},
		{Name: "Command_variable_count", Type: DataTypeInt},
		{Name: "Thread_variable_count", Type: DataTypeInt},
		{Name: "Connection_count", Type: DataTypeInt},
	}
}

func statisticsSummaryResultRow(row ClientStatisticsSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.VariableCount),
		metadataIntCell(row.SummaryLength),
		metadataIntCell(row.EmptyValueCount),
		metadataIntCell(row.NumericValueCount),
		metadataIntCell(row.CommandVariableCount),
		metadataIntCell(row.ThreadVariableCount),
		metadataIntCell(row.ConnectionCount),
	}
}
