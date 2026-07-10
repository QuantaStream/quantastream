package qsbridge

// ClientDiagnosticRow describes one adapter-visible diagnostic or protocol error row.
type ClientDiagnosticRow struct {
	Severity   DiagnosticSeverity
	SQLState   SQLState
	VendorCode int
	Code       DiagnosticCode
	Phase      DiagnosticPhase
	Message    string
	Span       SourceSpan
	Fields     []FieldRef
}

// ClientDiagnosticsSummaryRow describes aggregate diagnostic metadata.
type ClientDiagnosticsSummaryRow struct {
	DiagnosticCount int
	ErrorCount      int
	WarningCount    int
	InfoCount       int
	FieldRowCount   int
	SpannedRowCount int
}

// ClientDiagnosticsExchange is adapter-facing diagnostic metadata.
type ClientDiagnosticsExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Rows                []ClientDiagnosticRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientDiagnostics returns SHOW ERRORS/diagnostics-style rows for supplied diagnostics.
func (s PlanningService) ListClientDiagnostics(connection ConnectionContext, diagnostics DiagnosticSet) ClientDiagnosticsExchange {
	_ = s
	exchange := ClientDiagnosticsExchange{
		Connection:          cloneConnectionContext(connection),
		Diagnostics:         cloneDiagnosticSet(diagnostics),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = diagnosticRows(diagnostics)
	}
	exchange.Result = exchange.diagnosticsResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether diagnostic metadata can be returned.
func (e ClientDiagnosticsExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientDiagnosticsExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientDiagnosticsExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientDiagnosticsExchange) diagnosticsResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     diagnosticsResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.diagnosticResultRows(),
		Final: true,
	})
}

func diagnosticsResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Level", Type: DataTypeString},
		{Name: "SQLState", Type: DataTypeString},
		{Name: "Vendor_code", Type: DataTypeInt},
		{Name: "Diagnostic_code", Type: DataTypeString},
		{Name: "Phase", Type: DataTypeString},
		{Name: "Message", Type: DataTypeString, Nullable: true},
		{Name: "Start_line", Type: DataTypeInt},
		{Name: "Start_col", Type: DataTypeInt},
		{Name: "End_line", Type: DataTypeInt},
		{Name: "End_col", Type: DataTypeInt},
		{Name: "Fields", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientDiagnosticsExchange) diagnosticResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Severity)),
			metadataStringCell(string(row.SQLState)),
			metadataIntCell(row.VendorCode),
			metadataStringCell(string(row.Code)),
			metadataStringCell(string(row.Phase)),
			metadataStringCell(row.Message),
			metadataIntCell(row.Span.StartLine),
			metadataIntCell(row.Span.StartCol),
			metadataIntCell(row.Span.EndLine),
			metadataIntCell(row.Span.EndCol),
			metadataStringCell(joinStringValues(qualifiedFieldNames(row.Fields))),
		})
	}
	return rows
}

func summarizeDiagnosticRows(rows []ClientDiagnosticRow) ClientDiagnosticsSummaryRow {
	summary := ClientDiagnosticsSummaryRow{DiagnosticCount: len(rows)}
	for _, row := range rows {
		switch row.Severity {
		case SeverityError:
			summary.ErrorCount++
		case SeverityWarning:
			summary.WarningCount++
		case SeverityInfo:
			summary.InfoCount++
		}
		if len(row.Fields) > 0 {
			summary.FieldRowCount++
		}
		if row.Span.StartLine != 0 || row.Span.StartCol != 0 || row.Span.EndLine != 0 || row.Span.EndCol != 0 {
			summary.SpannedRowCount++
		}
	}
	return summary
}
func diagnosticRows(diagnostics DiagnosticSet) []ClientDiagnosticRow {
	if len(diagnostics) == 0 {
		return nil
	}
	rows := make([]ClientDiagnosticRow, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		protocol := diagnostic.ProtocolError()
		rows = append(rows, ClientDiagnosticRow{
			Severity:   diagnostic.Severity,
			SQLState:   protocol.SQLState,
			VendorCode: protocol.VendorCode,
			Code:       diagnostic.Code,
			Phase:      diagnostic.Phase,
			Message:    diagnostic.Error(),
			Span:       diagnostic.Span,
			Fields:     append([]FieldRef(nil), diagnostic.Fields...),
		})
	}
	return rows
}
