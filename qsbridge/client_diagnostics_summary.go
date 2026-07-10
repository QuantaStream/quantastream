package qsbridge

// ClientDiagnosticsSummaryExchange is adapter-facing metadata for aggregate diagnostics.
type ClientDiagnosticsSummaryExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Row                 ClientDiagnosticsSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientDiagnostics returns aggregate SHOW ERRORS/diagnostics-style metadata.
func (s PlanningService) SummarizeClientDiagnostics(connection ConnectionContext, diagnostics DiagnosticSet) ClientDiagnosticsSummaryExchange {
	_ = s
	exchange := ClientDiagnosticsSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Diagnostics:         cloneDiagnosticSet(diagnostics),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeDiagnosticRows(diagnosticRows(diagnostics))
	}
	exchange.Result = exchange.diagnosticsSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether diagnostic summary metadata can be returned.
func (e ClientDiagnosticsSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientDiagnosticsSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientDiagnosticsSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientDiagnosticsSummaryExchange) diagnosticsSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     diagnosticsSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{diagnosticsSummaryResultRow(e.Row)},
		Final: true,
	})
}

func diagnosticsSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Diagnostic_count", Type: DataTypeInt},
		{Name: "Error_count", Type: DataTypeInt},
		{Name: "Warning_count", Type: DataTypeInt},
		{Name: "Info_count", Type: DataTypeInt},
		{Name: "Field_row_count", Type: DataTypeInt},
		{Name: "Spanned_row_count", Type: DataTypeInt},
	}
}

func diagnosticsSummaryResultRow(row ClientDiagnosticsSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.DiagnosticCount),
		metadataIntCell(row.ErrorCount),
		metadataIntCell(row.WarningCount),
		metadataIntCell(row.InfoCount),
		metadataIntCell(row.FieldRowCount),
		metadataIntCell(row.SpannedRowCount),
	}
}
