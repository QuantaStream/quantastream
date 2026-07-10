package qsbridge

// ClientBatchDiagnosticScope identifies where a batch diagnostic was produced.
type ClientBatchDiagnosticScope string

const (
	// ClientBatchDiagnosticScopeBatch identifies diagnostics attached to the batch envelope.
	ClientBatchDiagnosticScopeBatch ClientBatchDiagnosticScope = "batch"
	// ClientBatchDiagnosticScopeItem identifies diagnostics attached to one batch item.
	ClientBatchDiagnosticScopeItem ClientBatchDiagnosticScope = "item"
)

// ClientBatchDiagnosticRow describes one adapter-visible batch diagnostic row.
type ClientBatchDiagnosticRow struct {
	Scope      ClientBatchDiagnosticScope
	Item       int
	RequestID  ExecutionRequestID
	Severity   DiagnosticSeverity
	SQLState   SQLState
	VendorCode int
	Code       DiagnosticCode
	Phase      DiagnosticPhase
	Message    string
	Span       SourceSpan
	Fields     []FieldRef
}

// ClientBatchDiagnosticsSummaryRow describes aggregate batch diagnostic metadata.
type ClientBatchDiagnosticsSummaryRow struct {
	DiagnosticCount int
	BatchCount      int
	ItemCount       int
	ErrorCount      int
	WarningCount    int
	InfoCount       int
	FieldRowCount   int
	SpannedRowCount int
}

// ClientBatchDiagnosticsExchange is adapter-facing batch diagnostic metadata.
type ClientBatchDiagnosticsExchange struct {
	Connection          ConnectionContext
	Batch               BatchExecutionResult
	Rows                []ClientBatchDiagnosticRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientBatchDiagnostics returns diagnostic rows for a batch result envelope and its items.
func (s PlanningService) ListClientBatchDiagnostics(connection ConnectionContext, batch BatchExecutionResult) ClientBatchDiagnosticsExchange {
	_ = s
	exchange := ClientBatchDiagnosticsExchange{
		Connection:          cloneConnectionContext(connection),
		Batch:               cloneBatchExecutionResult(batch),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = batchDiagnosticRows(batch)
	}
	exchange.Result = exchange.batchDiagnosticsResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch diagnostic metadata can be returned.
func (e ClientBatchDiagnosticsExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientBatchDiagnosticsExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientBatchDiagnosticsExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientBatchDiagnosticsExchange) batchDiagnosticsResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchDiagnosticsResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.batchDiagnosticResultRows(),
		Final: true,
	})
}

func batchDiagnosticsResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Scope", Type: DataTypeString},
		{Name: "Item", Type: DataTypeInt},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
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

func (e ClientBatchDiagnosticsExchange) batchDiagnosticResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Scope)),
			metadataIntCell(row.Item),
			metadataStringCell(string(row.RequestID)),
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

func batchDiagnosticRows(batch BatchExecutionResult) []ClientBatchDiagnosticRow {
	rows := make([]ClientBatchDiagnosticRow, 0, len(batch.Diagnostics)+len(batch.Items))
	itemDiagnostics := batchItemDiagnosticSignatures(batch)
	rows = append(rows, batchDiagnosticRowsForSet(ClientBatchDiagnosticScopeBatch, -1, batch.RequestID, batch.Diagnostics, itemDiagnostics)...)
	for itemIndex, item := range batch.Items {
		rows = append(rows, batchDiagnosticRowsForSet(ClientBatchDiagnosticScopeItem, itemIndex, item.RequestID, item.Diagnostics, nil)...)
	}
	return rows
}

func summarizeBatchDiagnosticRows(rows []ClientBatchDiagnosticRow) ClientBatchDiagnosticsSummaryRow {
	summary := ClientBatchDiagnosticsSummaryRow{DiagnosticCount: len(rows)}
	for _, row := range rows {
		switch row.Scope {
		case ClientBatchDiagnosticScopeBatch:
			summary.BatchCount++
		case ClientBatchDiagnosticScopeItem:
			summary.ItemCount++
		}
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

func batchDiagnosticRowsForSet(scope ClientBatchDiagnosticScope, item int, requestID ExecutionRequestID, diagnostics DiagnosticSet, skip map[string]struct{}) []ClientBatchDiagnosticRow {
	if len(diagnostics) == 0 {
		return nil
	}
	rows := make([]ClientBatchDiagnosticRow, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if _, ok := skip[diagnosticSignature(diagnostic)]; ok {
			continue
		}
		protocol := diagnostic.ProtocolError()
		rows = append(rows, ClientBatchDiagnosticRow{
			Scope:      scope,
			Item:       item,
			RequestID:  requestID,
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

func batchItemDiagnosticSignatures(batch BatchExecutionResult) map[string]struct{} {
	signatures := make(map[string]struct{})
	for _, item := range batch.Items {
		for _, diagnostic := range item.Diagnostics {
			signatures[diagnosticSignature(diagnostic)] = struct{}{}
		}
	}
	return signatures
}

func diagnosticSignature(diagnostic Diagnostic) string {
	return string(diagnostic.Severity) + "|" +
		string(diagnostic.Code) + "|" +
		string(diagnostic.Phase) + "|" +
		diagnostic.Message + "|" +
		joinStringValues(qualifiedFieldNames(diagnostic.Fields))
}
