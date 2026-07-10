package qsbridge

// ClientBatchSessionStateSummaryExchange is adapter-facing metadata for batch session-state aggregates.
type ClientBatchSessionStateSummaryExchange struct {
	Connection   ConnectionContext
	Batch        BatchExecutionResult
	Row          ClientBatchSessionStateSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientBatchSessionStateChanges returns aggregate session-state metadata for a batch result.
func (s PlanningService) SummarizeClientBatchSessionStateChanges(connection ConnectionContext, batch BatchExecutionResult) ClientBatchSessionStateSummaryExchange {
	_ = s
	exchange := ClientBatchSessionStateSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Batch:       cloneBatchExecutionResult(batch),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, batch.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.batchSessionStateSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, validateBatchSessionActions(connection.Protocol, batch))
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Row = summarizeBatchSessionStateChanges(batch, batchSessionStateChanges(batch))
	}
	exchange.Result = exchange.batchSessionStateSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch session-state summary metadata can be returned.
func (e ClientBatchSessionStateSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch session-state summary diagnostics into protocol-facing errors.
func (e ClientBatchSessionStateSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch session-state summary error, if any.
func (e ClientBatchSessionStateSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientBatchSessionStateSummaryExchange) batchSessionStateSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchSessionStateSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{batchSessionStateSummaryResultRow(e.Row)},
		Final: true,
	})
}

func batchSessionStateSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Item_count", Type: DataTypeInt},
		{Name: "Changed_item_count", Type: DataTypeInt},
		{Name: "Change_count", Type: DataTypeInt},
		{Name: "Schema_change_count", Type: DataTypeInt},
		{Name: "Variable_change_count", Type: DataTypeInt},
		{Name: "Transaction_count", Type: DataTypeInt},
		{Name: "Reset_connection_count", Type: DataTypeInt},
		{Name: "Change_user_count", Type: DataTypeInt},
	}
}

func batchSessionStateSummaryResultRow(row ClientBatchSessionStateSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.ItemCount),
		metadataIntCell(row.ChangedItemCount),
		metadataIntCell(row.ChangeCount),
		metadataIntCell(row.SchemaChangeCount),
		metadataIntCell(row.VariableChangeCount),
		metadataIntCell(row.TransactionCount),
		metadataIntCell(row.ResetConnectionCount),
		metadataIntCell(row.ChangeUserCount),
	}
}
