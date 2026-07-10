package qsbridge

// ClientWarningsSummaryExchange is adapter-facing metadata for statement warning aggregates.
type ClientWarningsSummaryExchange struct {
	Connection   ConnectionContext
	Response     ProtocolStatementResponse
	Row          ClientWarningsSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientStatementWarnings returns aggregate SHOW WARNINGS-style metadata for a statement response.
func (s PlanningService) SummarizeClientStatementWarnings(connection ConnectionContext, response ProtocolStatementResponse) ClientWarningsSummaryExchange {
	_ = s
	exchange := ClientWarningsSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Response:    cloneProtocolStatementResponse(response),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeStatementNotices(response.Notices, response.Warnings)
	}
	exchange.Result = exchange.warningSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether warning summary metadata can be returned.
func (e ClientWarningsSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts warning summary diagnostics into protocol-facing errors.
func (e ClientWarningsSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking warning summary error, if any.
func (e ClientWarningsSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientWarningsSummaryExchange) warningSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     warningSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{warningSummaryResultRow(e.Row)},
		Final: true,
	})
}

func warningSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Warning_count", Type: DataTypeInt},
		{Name: "Notice_count", Type: DataTypeInt},
		{Name: "Warning_rows", Type: DataTypeInt},
		{Name: "Note_rows", Type: DataTypeInt},
		{Name: "Error_rows", Type: DataTypeInt},
		{Name: "Coded_rows", Type: DataTypeInt},
		{Name: "SQLState_rows", Type: DataTypeInt},
	}
}

func warningSummaryResultRow(row ClientWarningsSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.WarningCount),
		metadataIntCell(row.NoticeCount),
		metadataIntCell(row.WarningRows),
		metadataIntCell(row.NoteRows),
		metadataIntCell(row.ErrorRows),
		metadataIntCell(row.CodedRows),
		metadataIntCell(row.SQLStateRows),
	}
}
