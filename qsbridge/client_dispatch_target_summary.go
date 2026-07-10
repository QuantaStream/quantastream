package qsbridge

// ClientDispatchTargetSummaryExchange is aggregate dispatch target metadata.
type ClientDispatchTargetSummaryExchange struct {
	Connection   ConnectionContext
	Rows         []DispatchTargetSummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientDispatchTargets returns aggregate dispatch target metadata.
func (s PlanningService) SummarizeClientDispatchTargets(connection ConnectionContext) ClientDispatchTargetSummaryExchange {
	_ = s
	exchange := ClientDispatchTargetSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []DispatchTargetSummary{DefaultDispatchTargetSummary()}
	}
	exchange.Result = exchange.dispatchTargetSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether dispatch target summary metadata can be returned.
func (e ClientDispatchTargetSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts dispatch target summary diagnostics into protocol-facing errors.
func (e ClientDispatchTargetSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking dispatch target summary error, if any.
func (e ClientDispatchTargetSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientDispatchTargetSummaryExchange) dispatchTargetSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     dispatchTargetSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.dispatchTargetSummaryRows(),
		Final: true,
	})
}

func dispatchTargetSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Targets", Type: DataTypeInt},
		{Name: "Runtime_owned", Type: DataTypeInt},
		{Name: "Requires_executor", Type: DataTypeInt},
		{Name: "Configurable", Type: DataTypeInt},
		{Name: "Terminal", Type: DataTypeInt},
	}
}

func (e ClientDispatchTargetSummaryExchange) dispatchTargetSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.TargetCount),
			metadataIntCell(row.RuntimeOwnedCount),
			metadataIntCell(row.RequiresExecutorCount),
			metadataIntCell(row.ConfigurableCount),
			metadataIntCell(row.TerminalCount),
		})
	}
	return rows
}
