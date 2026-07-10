package qsbridge

// ClientBatchExecutionProfileSummaryExchange is adapter-facing metadata for aggregate batch profile rows.
type ClientBatchExecutionProfileSummaryExchange struct {
	Connection   ConnectionContext
	Batch        BatchExecutionResult
	Row          ClientBatchProfileSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// PrepareClientBatchExecutionProfileSummary returns aggregate profile metadata for each batch item result.
func (s PlanningService) PrepareClientBatchExecutionProfileSummary(connection ConnectionContext, batch BatchExecutionResult) ClientBatchExecutionProfileSummaryExchange {
	_ = s
	exchange := ClientBatchExecutionProfileSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Batch:       cloneBatchExecutionResult(batch),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), batchProfileDiagnostics(batch)),
	}
	if connection.Supported() {
		exchange.Row = summarizeBatchExecutionProfileRows(batch, batchExecutionProfileRows(batch))
	}
	exchange.Result = exchange.batchExecutionProfileSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch profile summary metadata can be returned.
func (e ClientBatchExecutionProfileSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch profile summary diagnostics into protocol-facing errors.
func (e ClientBatchExecutionProfileSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch profile summary error, if any.
func (e ClientBatchExecutionProfileSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientBatchExecutionProfileSummaryExchange) batchExecutionProfileSummaryResult() ExecutionResult {
	result := ExecutionResult{
		RequestID:   e.Batch.RequestID,
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchExecutionProfileSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{batchExecutionProfileSummaryResultRow(e.Row)},
		Final: true,
	})
}

func batchExecutionProfileSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Item_count", Type: DataTypeInt},
		{Name: "Profiled_item_count", Type: DataTypeInt},
		{Name: "Row_count", Type: DataTypeInt},
		{Name: "Read_intent_item_count", Type: DataTypeInt},
		{Name: "Write_intent_item_count", Type: DataTypeInt},
		{Name: "Select_lifecycle_item_count", Type: DataTypeInt},
		{Name: "Mutation_lifecycle_item_count", Type: DataTypeInt},
		{Name: "Explain_count", Type: DataTypeInt},
		{Name: "Timing_count", Type: DataTypeInt},
		{Name: "Counter_count", Type: DataTypeInt},
		{Name: "Diagnostic_count", Type: DataTypeInt},
	}
}

func batchExecutionProfileSummaryResultRow(row ClientBatchProfileSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.ItemCount),
		metadataIntCell(row.ProfiledItems),
		metadataIntCell(row.RowCount),
		metadataIntCell(row.ReadIntentItems),
		metadataIntCell(row.WriteIntentItems),
		metadataIntCell(row.SelectLifecycleItems),
		metadataIntCell(row.MutationLifecycleItems),
		metadataIntCell(row.ExplainCount),
		metadataIntCell(row.TimingCount),
		metadataIntCell(row.CounterCount),
		metadataIntCell(row.DiagnosticCount),
	}
}
