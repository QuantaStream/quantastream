package qsbridge

// ClientSQLModesSummaryExchange is adapter-facing metadata for aggregate SQL compatibility modes.
type ClientSQLModesSummaryExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Row          ClientSQLModesSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientSQLModes returns aggregate adapter-supplied SQL mode metadata with session enablement.
func (s PlanningService) SummarizeClientSQLModes(connection ConnectionContext, modes []ClientSQLMode, pattern string) ClientSQLModesSummaryExchange {
	_ = s
	exchange := ClientSQLModesSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeClientSQLModes(filterClientSQLModes(markEnabledSQLModes(modes, connection.Session), pattern))
	}
	exchange.Result = exchange.sqlModesSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether SQL mode summary metadata can be returned.
func (e ClientSQLModesSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts SQL mode summary diagnostics into protocol-facing errors.
func (e ClientSQLModesSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking SQL mode summary error, if any.
func (e ClientSQLModesSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientSQLModesSummaryExchange) sqlModesSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     sqlModesSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{sqlModesSummaryResultRow(e.Row)},
		Final: true,
	})
}

func sqlModesSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Mode_count", Type: DataTypeInt},
		{Name: "Supported_count", Type: DataTypeInt},
		{Name: "Unsupported_count", Type: DataTypeInt},
		{Name: "Default_count", Type: DataTypeInt},
		{Name: "Enabled_count", Type: DataTypeInt},
		{Name: "Default_enabled_count", Type: DataTypeInt},
		{Name: "Supported_enabled_count", Type: DataTypeInt},
	}
}

func sqlModesSummaryResultRow(row ClientSQLModesSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.ModeCount),
		metadataIntCell(row.SupportedCount),
		metadataIntCell(row.UnsupportedCount),
		metadataIntCell(row.DefaultCount),
		metadataIntCell(row.EnabledCount),
		metadataIntCell(row.DefaultEnabledCount),
		metadataIntCell(row.SupportedEnabledCount),
	}
}
