package qsbridge

// ClientBatchWarningsSummaryExchange is adapter-facing metadata for batch warning aggregates.
type ClientBatchWarningsSummaryExchange struct {
	Connection   ConnectionContext
	Batch        BatchExecutionResult
	Row          ClientBatchWarningsSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientBatchWarnings returns aggregate SHOW WARNINGS-style metadata for batch item results.
func (s PlanningService) SummarizeClientBatchWarnings(connection ConnectionContext, batch BatchExecutionResult) ClientBatchWarningsSummaryExchange {
	_ = s
	exchange := ClientBatchWarningsSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Batch:       cloneBatchExecutionResult(batch),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, batch.Diagnostics),
	}
	warningCount := batchWarningCount(batch)
	if connection.Supported() {
		exchange.Row = summarizeBatchWarningRows(batchWarningRows(batch), warningCount)
	}
	exchange.Result = exchange.batchWarningSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch warning summary metadata can be returned.
func (e ClientBatchWarningsSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch warning summary diagnostics into protocol-facing errors.
func (e ClientBatchWarningsSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch warning summary error, if any.
func (e ClientBatchWarningsSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientBatchWarningsSummaryExchange) batchWarningSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchWarningSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{batchWarningSummaryResultRow(e.Row)},
		Final: true,
	})
}

func batchWarningSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Warning_count", Type: DataTypeInt},
		{Name: "Detail_row_count", Type: DataTypeInt},
		{Name: "Warning_rows", Type: DataTypeInt},
		{Name: "Note_rows", Type: DataTypeInt},
		{Name: "Error_rows", Type: DataTypeInt},
		{Name: "Coded_rows", Type: DataTypeInt},
		{Name: "SQLState_rows", Type: DataTypeInt},
		{Name: "Items_with_details", Type: DataTypeInt},
		{Name: "Distinct_request_ids", Type: DataTypeInt},
	}
}

func batchWarningSummaryResultRow(row ClientBatchWarningsSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.WarningCount),
		metadataIntCell(row.DetailRowCount),
		metadataIntCell(row.WarningRows),
		metadataIntCell(row.NoteRows),
		metadataIntCell(row.ErrorRows),
		metadataIntCell(row.CodedRows),
		metadataIntCell(row.SQLStateRows),
		metadataIntCell(row.ItemsWithDetails),
		metadataIntCell(row.DistinctRequestIDs),
	}
}
