package qsbridge

// ClientDispatchPreviewSummaryRow describes aggregate dispatch-preview metadata.
type ClientDispatchPreviewSummaryRow struct {
	User                 UserName
	Schema               string
	Protocol             ProtocolKind
	Supported            bool
	AllDispatchable      bool
	PreviewCount         int
	NativeTargetCount    int
	LegacyTargetCount    int
	NoTargetCount        int
	ConfiguredCount      int
	MissingExecutorCount int
	WillDispatchCount    int
	DiagnosticCodes      []DiagnosticCode
}

// ClientDispatchPreviewSummaryExchange is adapter-facing aggregate dispatch-preview metadata.
type ClientDispatchPreviewSummaryExchange struct {
	Connection          ConnectionContext
	Preview             ClientDispatchPreviewExchange
	Rows                []ClientDispatchPreviewSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientDispatchPreviews returns aggregate row metadata for dispatch previews.
func (s PlanningService) SummarizeClientDispatchPreviews(preview ClientDispatchPreviewExchange) ClientDispatchPreviewSummaryExchange {
	_ = s
	exchange := ClientDispatchPreviewSummaryExchange{
		Connection:          cloneConnectionContext(preview.Connection),
		Preview:             cloneClientDispatchPreviewExchange(preview),
		ExchangeDiagnostics: cloneDiagnosticSet(preview.ExchangeDiagnostics),
	}
	if preview.Connection.Supported() {
		exchange.Rows = []ClientDispatchPreviewSummaryRow{clientDispatchPreviewSummaryRow(preview)}
	}
	exchange.Result = exchange.clientDispatchPreviewSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(preview.Connection.Protocol)
	return exchange
}

// Supported reports whether dispatch-preview summary metadata can be returned.
func (e ClientDispatchPreviewSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientDispatchPreviewSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking dispatch-preview summary error, if any.
func (e ClientDispatchPreviewSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientDispatchPreviewSummaryExchange) clientDispatchPreviewSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     clientDispatchPreviewSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.clientDispatchPreviewSummaryRows(),
		Final: true,
	})
}

func clientDispatchPreviewSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "User", Type: DataTypeString, Nullable: true},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "Protocol", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "All_dispatchable", Type: DataTypeBool},
		{Name: "Previews", Type: DataTypeInt},
		{Name: "Native_targets", Type: DataTypeInt},
		{Name: "Legacy_targets", Type: DataTypeInt},
		{Name: "No_targets", Type: DataTypeInt},
		{Name: "Configured", Type: DataTypeInt},
		{Name: "Missing_executors", Type: DataTypeInt},
		{Name: "Will_dispatch", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientDispatchPreviewSummaryExchange) clientDispatchPreviewSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.User)),
			metadataStringCell(row.Schema),
			metadataStringCell(string(row.Protocol)),
			metadataBoolCell(row.Supported),
			metadataBoolCell(row.AllDispatchable),
			metadataIntCell(row.PreviewCount),
			metadataIntCell(row.NativeTargetCount),
			metadataIntCell(row.LegacyTargetCount),
			metadataIntCell(row.NoTargetCount),
			metadataIntCell(row.ConfiguredCount),
			metadataIntCell(row.MissingExecutorCount),
			metadataIntCell(row.WillDispatchCount),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func clientDispatchPreviewSummaryRow(preview ClientDispatchPreviewExchange) ClientDispatchPreviewSummaryRow {
	row := ClientDispatchPreviewSummaryRow{
		User:            preview.Connection.Session.User,
		Schema:          preview.Connection.Session.CurrentSchema,
		Protocol:        preview.Connection.Protocol.Kind,
		Supported:       preview.Supported(),
		AllDispatchable: len(preview.Rows) > 0,
		PreviewCount:    len(preview.Rows),
		DiagnosticCodes: preview.Diagnostics.Codes(),
	}
	for _, previewRow := range preview.Rows {
		switch previewRow.Target {
		case DispatchTargetNative:
			row.NativeTargetCount++
		case DispatchTargetLegacy:
			row.LegacyTargetCount++
		default:
			row.NoTargetCount++
		}
		if previewRow.ExecutorConfigured {
			row.ConfiguredCount++
		} else if previewRow.Target != DispatchTargetNone {
			row.MissingExecutorCount++
		}
		if previewRow.WillDispatch {
			row.WillDispatchCount++
		} else {
			row.AllDispatchable = false
		}
		row.DiagnosticCodes = append(row.DiagnosticCodes, previewRow.Diagnostics...)
	}
	return row
}

func cloneClientDispatchPreviewExchange(preview ClientDispatchPreviewExchange) ClientDispatchPreviewExchange {
	preview.Connection = cloneConnectionContext(preview.Connection)
	preview.Diagnostics = cloneDiagnosticSet(preview.Diagnostics)
	preview.ExchangeDiagnostics = cloneDiagnosticSet(preview.ExchangeDiagnostics)
	if len(preview.Rows) > 0 {
		rows := make([]ClientDispatchPreviewRow, 0, len(preview.Rows))
		for _, row := range preview.Rows {
			row.Diagnostics = append([]DiagnosticCode(nil), row.Diagnostics...)
			rows = append(rows, row)
		}
		preview.Rows = rows
	}
	preview.Result = cloneExecutionResult(preview.Result)
	preview.ResultSchema = cloneProtocolResultSchema(preview.ResultSchema)
	return preview
}
