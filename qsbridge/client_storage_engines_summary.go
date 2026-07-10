package qsbridge

// ClientStorageEnginesSummaryExchange is adapter-facing storage engine summary metadata.
type ClientStorageEnginesSummaryExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Row          ClientStorageEnginesSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientStorageEngines returns aggregate SHOW ENGINES-style metadata supplied by the adapter.
func (s PlanningService) SummarizeClientStorageEngines(connection ConnectionContext, engines []ClientStorageEngine, pattern string) ClientStorageEnginesSummaryExchange {
	_ = s
	exchange := ClientStorageEnginesSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeClientStorageEngines(filterClientStorageEngines(engines, pattern))
	}
	exchange.Result = exchange.storageEnginesSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether storage engine summary metadata can be returned.
func (e ClientStorageEnginesSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts storage engine summary diagnostics into protocol-facing errors.
func (e ClientStorageEnginesSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking storage engine summary error, if any.
func (e ClientStorageEnginesSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientStorageEnginesSummaryExchange) storageEnginesSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     storageEnginesSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{storageEnginesSummaryResultRow(e.Row)},
		Final: true,
	})
}

func storageEnginesSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Engine_count", Type: DataTypeInt},
		{Name: "Default_count", Type: DataTypeInt},
		{Name: "Available_count", Type: DataTypeInt},
		{Name: "Unavailable_count", Type: DataTypeInt},
		{Name: "Disabled_count", Type: DataTypeInt},
		{Name: "Transactions_count", Type: DataTypeInt},
		{Name: "XA_count", Type: DataTypeInt},
		{Name: "Savepoints_count", Type: DataTypeInt},
	}
}

func storageEnginesSummaryResultRow(row ClientStorageEnginesSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.EngineCount),
		metadataIntCell(row.DefaultCount),
		metadataIntCell(row.AvailableCount),
		metadataIntCell(row.UnavailableCount),
		metadataIntCell(row.DisabledCount),
		metadataIntCell(row.TransactionsCount),
		metadataIntCell(row.XACount),
		metadataIntCell(row.SavepointsCount),
	}
}
