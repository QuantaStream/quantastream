package qsbridge

// ClientExecutorStatusSummaryExchange is aggregate executor boundary metadata.
type ClientExecutorStatusSummaryExchange struct {
	Connection          ConnectionContext
	Status              ClientExecutorStatusExchange
	Rows                []ClientExecutorStatusSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientExecutorStatus returns aggregate executor boundary status.
func (s PlanningService) SummarizeClientExecutorStatus(status ClientExecutorStatusExchange) ClientExecutorStatusSummaryExchange {
	_ = s
	exchange := ClientExecutorStatusSummaryExchange{
		Connection:          cloneConnectionContext(status.Connection),
		Status:              cloneClientExecutorStatusExchange(status),
		ExchangeDiagnostics: cloneDiagnosticSet(status.ExchangeDiagnostics),
	}
	if status.Connection.Supported() {
		exchange.Rows = []ClientExecutorStatusSummaryRow{summarizeExecutorStatusRows(status.Rows)}
	}
	exchange.Result = exchange.executorStatusSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(status.Connection.Protocol)
	return exchange
}

// Supported reports whether executor status summary metadata can be returned.
func (e ClientExecutorStatusSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientExecutorStatusSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking executor status summary error, if any.
func (e ClientExecutorStatusSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientExecutorStatusSummaryExchange) executorStatusSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     executorStatusSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.executorStatusSummaryRows(),
		Final: true,
	})
}

func executorStatusSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Executors", Type: DataTypeInt},
		{Name: "Configured", Type: DataTypeInt},
		{Name: "Missing", Type: DataTypeInt},
		{Name: "Single_request", Type: DataTypeInt},
		{Name: "Batch_request", Type: DataTypeInt},
		{Name: "All_configured", Type: DataTypeBool},
		{Name: "All_single_request", Type: DataTypeBool},
		{Name: "All_batch_request", Type: DataTypeBool},
	}
}

func (e ClientExecutorStatusSummaryExchange) executorStatusSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.ExecutorCount),
			metadataIntCell(row.ConfiguredCount),
			metadataIntCell(row.MissingCount),
			metadataIntCell(row.SingleRequestCount),
			metadataIntCell(row.BatchRequestCount),
			metadataBoolCell(row.AllConfigured),
			metadataBoolCell(row.AllSingleRequest),
			metadataBoolCell(row.AllBatchRequest),
		})
	}
	return rows
}

func cloneClientExecutorStatusExchange(status ClientExecutorStatusExchange) ClientExecutorStatusExchange {
	status.Connection = cloneConnectionContext(status.Connection)
	status.Diagnostics = cloneDiagnosticSet(status.Diagnostics)
	status.ExchangeDiagnostics = cloneDiagnosticSet(status.ExchangeDiagnostics)
	status.Rows = append([]ClientExecutorStatusRow(nil), status.Rows...)
	status.Result = cloneExecutionResult(status.Result)
	status.ResultSchema = cloneProtocolResultSchema(status.ResultSchema)
	return status
}
