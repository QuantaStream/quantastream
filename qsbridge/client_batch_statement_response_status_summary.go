package qsbridge

// ClientBatchStatementResponseStatusSummaryExchange is adapter-facing batch OK/status summary metadata.
type ClientBatchStatementResponseStatusSummaryExchange struct {
	Connection   ConnectionContext
	Batch        BatchExecutionResult
	Row          ClientBatchStatementResponseStatusSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientBatchStatementResponseStatus returns aggregate OK/status metadata for batch item results.
func (s PlanningService) SummarizeClientBatchStatementResponseStatus(connection ConnectionContext, batch BatchExecutionResult) ClientBatchStatementResponseStatusSummaryExchange {
	_ = s
	exchange := ClientBatchStatementResponseStatusSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Batch:       cloneBatchExecutionResult(batch),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, batch.Diagnostics),
	}
	if connection.Supported() {
		var rows []ClientBatchStatementResponseStatusRow
		rows, exchange.Diagnostics = batchStatementResponseStatusRows(connection.Protocol, batch, exchange.Diagnostics)
		exchange.Row = summarizeBatchStatementResponseStatusRows(rows)
	}
	exchange.Result = exchange.batchStatementResponseStatusSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch statement response status summary metadata can be returned.
func (e ClientBatchStatementResponseStatusSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch statement response status summary diagnostics into protocol-facing errors.
func (e ClientBatchStatementResponseStatusSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch statement response status summary error, if any.
func (e ClientBatchStatementResponseStatusSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientBatchStatementResponseStatusSummaryExchange) batchStatementResponseStatusSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchStatementResponseStatusSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{batchStatementResponseStatusSummaryResultRow(e.Row)},
		Final: true,
	})
}

func batchStatementResponseStatusSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Item_count", Type: DataTypeInt},
		{Name: "Total_affected_rows", Type: DataTypeInt},
		{Name: "Items_with_affected_rows", Type: DataTypeInt},
		{Name: "Items_with_last_insert_id", Type: DataTypeInt},
		{Name: "Total_warnings", Type: DataTypeInt},
		{Name: "Items_with_warnings", Type: DataTypeInt},
		{Name: "Total_session_actions", Type: DataTypeInt},
		{Name: "Items_with_session_actions", Type: DataTypeInt},
		{Name: "Transaction_item_count", Type: DataTypeInt},
		{Name: "Items_with_diagnostics", Type: DataTypeInt},
		{Name: "Rows_affected_flag_count", Type: DataTypeInt},
		{Name: "Last_insert_id_flag_count", Type: DataTypeInt},
		{Name: "Warnings_flag_count", Type: DataTypeInt},
		{Name: "Session_state_changed_count", Type: DataTypeInt},
		{Name: "Transaction_flag_count", Type: DataTypeInt},
	}
}

func batchStatementResponseStatusSummaryResultRow(row ClientBatchStatementResponseStatusSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.ItemCount),
		metadataIntCell(int(row.TotalAffectedRows)),
		metadataIntCell(row.ItemsWithAffectedRows),
		metadataIntCell(row.ItemsWithLastInsertID),
		metadataIntCell(row.TotalWarnings),
		metadataIntCell(row.ItemsWithWarnings),
		metadataIntCell(row.TotalSessionActions),
		metadataIntCell(row.ItemsWithSessionActions),
		metadataIntCell(row.TransactionItemCount),
		metadataIntCell(row.ItemsWithDiagnostics),
		metadataIntCell(row.RowsAffectedFlagCount),
		metadataIntCell(row.LastInsertIDFlagCount),
		metadataIntCell(row.WarningsFlagCount),
		metadataIntCell(row.SessionStateChangedCount),
		metadataIntCell(row.TransactionFlagCount),
	}
}
