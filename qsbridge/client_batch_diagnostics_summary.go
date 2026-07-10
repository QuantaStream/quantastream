package qsbridge

// ClientBatchDiagnosticsSummaryExchange is adapter-facing metadata for aggregate batch diagnostics.
type ClientBatchDiagnosticsSummaryExchange struct {
	Connection          ConnectionContext
	Batch               BatchExecutionResult
	Row                 ClientBatchDiagnosticsSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientBatchDiagnostics returns aggregate diagnostic metadata for a batch result.
func (s PlanningService) SummarizeClientBatchDiagnostics(connection ConnectionContext, batch BatchExecutionResult) ClientBatchDiagnosticsSummaryExchange {
	_ = s
	exchange := ClientBatchDiagnosticsSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Batch:               cloneBatchExecutionResult(batch),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeBatchDiagnosticRows(batchDiagnosticRows(batch))
	}
	exchange.Result = exchange.batchDiagnosticsSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch diagnostic summary metadata can be returned.
func (e ClientBatchDiagnosticsSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientBatchDiagnosticsSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientBatchDiagnosticsSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientBatchDiagnosticsSummaryExchange) batchDiagnosticsSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchDiagnosticsSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{batchDiagnosticsSummaryResultRow(e.Row)},
		Final: true,
	})
}

func batchDiagnosticsSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Diagnostic_count", Type: DataTypeInt},
		{Name: "Batch_count", Type: DataTypeInt},
		{Name: "Item_count", Type: DataTypeInt},
		{Name: "Error_count", Type: DataTypeInt},
		{Name: "Warning_count", Type: DataTypeInt},
		{Name: "Info_count", Type: DataTypeInt},
		{Name: "Field_row_count", Type: DataTypeInt},
		{Name: "Spanned_row_count", Type: DataTypeInt},
	}
}

func batchDiagnosticsSummaryResultRow(row ClientBatchDiagnosticsSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.DiagnosticCount),
		metadataIntCell(row.BatchCount),
		metadataIntCell(row.ItemCount),
		metadataIntCell(row.ErrorCount),
		metadataIntCell(row.WarningCount),
		metadataIntCell(row.InfoCount),
		metadataIntCell(row.FieldRowCount),
		metadataIntCell(row.SpannedRowCount),
	}
}
