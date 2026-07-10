package qsbridge

// ClientCursorStatusSummaryExchange is adapter-facing cursor inventory summary metadata.
type ClientCursorStatusSummaryExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Row                 ClientCursorStatusSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientCursors returns aggregate adapter-owned cursor metadata.
func (s PlanningService) SummarizeClientCursors(connection ConnectionContext, registry CursorRegistry) ClientCursorStatusSummaryExchange {
	_ = s
	exchange := ClientCursorStatusSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
		exchange.Result = exchange.cursorStatusSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if registry == nil {
		exchange.ExchangeDiagnostics = mergeDiagnosticSets(exchange.ExchangeDiagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor registry is not configured"),
		})
	} else {
		exchange.Row = summarizeCursorStatusRows(cursorStatusRows(registry.List()))
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.cursorStatusSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether cursor inventory summary metadata can be returned.
func (e ClientCursorStatusSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientCursorStatusSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientCursorStatusSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientCursorStatusSummaryExchange) cursorStatusSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     cursorStatusSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{cursorStatusSummaryResultRow(e.Row)},
		Final: true,
	})
}

func cursorStatusSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Cursor_count", Type: DataTypeInt},
		{Name: "Open_count", Type: DataTypeInt},
		{Name: "Exhausted_count", Type: DataTypeInt},
		{Name: "Closed_count", Type: DataTypeInt},
		{Name: "Forward_only_count", Type: DataTypeInt},
		{Name: "Scrollable_count", Type: DataTypeInt},
		{Name: "Total_batch_size", Type: DataTypeInt},
		{Name: "Total_max_rows", Type: DataTypeInt},
		{Name: "Total_position", Type: DataTypeInt},
		{Name: "Positioned_count", Type: DataTypeInt},
		{Name: "Configured_max_rows", Type: DataTypeInt},
	}
}

func cursorStatusSummaryResultRow(row ClientCursorStatusSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.CursorCount),
		metadataIntCell(row.OpenCount),
		metadataIntCell(row.ExhaustedCount),
		metadataIntCell(row.ClosedCount),
		metadataIntCell(row.ForwardOnlyCount),
		metadataIntCell(row.ScrollableCount),
		metadataIntCell(row.TotalBatchSize),
		metadataIntCell(row.TotalMaxRows),
		metadataIntCell(int(row.TotalPosition)),
		metadataIntCell(row.PositionedCount),
		metadataIntCell(row.ConfiguredMaxRows),
	}
}
