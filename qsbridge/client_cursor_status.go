package qsbridge

// ClientCursorStatusRow describes one adapter-visible cursor metadata row.
type ClientCursorStatusRow struct {
	ID        CursorID
	RequestID ExecutionRequestID
	Mode      CursorMode
	State     CursorState
	BatchSize int
	MaxRows   int
	Position  uint64
	Open      bool
}

// ClientCursorStatusSummaryRow describes aggregate cursor inventory metadata.
type ClientCursorStatusSummaryRow struct {
	CursorCount       int
	OpenCount         int
	ExhaustedCount    int
	ClosedCount       int
	ForwardOnlyCount  int
	ScrollableCount   int
	TotalBatchSize    int
	TotalMaxRows      int
	TotalPosition     uint64
	PositionedCount   int
	ConfiguredMaxRows int
}

// ClientCursorStatusExchange is adapter-facing cursor inventory metadata.
type ClientCursorStatusExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Rows                []ClientCursorStatusRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientCursors returns current adapter-owned cursor metadata as rows.
func (s PlanningService) ListClientCursors(connection ConnectionContext, registry CursorRegistry) ClientCursorStatusExchange {
	_ = s
	exchange := ClientCursorStatusExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
		exchange.Result = exchange.cursorStatusResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if registry == nil {
		exchange.ExchangeDiagnostics = mergeDiagnosticSets(exchange.ExchangeDiagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor registry is not configured"),
		})
	} else {
		exchange.Rows = cursorStatusRows(registry.List())
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.cursorStatusResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether cursor inventory metadata can be returned.
func (e ClientCursorStatusExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientCursorStatusExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientCursorStatusExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientCursorStatusExchange) cursorStatusResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     cursorStatusResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.cursorStatusResultRows(),
		Final: true,
	})
}

func cursorStatusResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Cursor_id", Type: DataTypeString},
		{Name: "Request_id", Type: DataTypeString},
		{Name: "Mode", Type: DataTypeString},
		{Name: "State", Type: DataTypeString},
		{Name: "Batch_size", Type: DataTypeInt},
		{Name: "Max_rows", Type: DataTypeInt},
		{Name: "Position", Type: DataTypeInt},
		{Name: "Open", Type: DataTypeBool},
	}
}

func (e ClientCursorStatusExchange) cursorStatusResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.ID)),
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(string(row.Mode)),
			metadataStringCell(string(row.State)),
			metadataIntCell(row.BatchSize),
			metadataIntCell(row.MaxRows),
			metadataIntCell(int(row.Position)),
			metadataBoolCell(row.Open),
		})
	}
	return rows
}

func summarizeCursorStatusRows(rows []ClientCursorStatusRow) ClientCursorStatusSummaryRow {
	summary := ClientCursorStatusSummaryRow{CursorCount: len(rows)}
	for _, row := range rows {
		switch row.State {
		case CursorStateOpen:
			summary.OpenCount++
		case CursorStateExhausted:
			summary.ExhaustedCount++
		case CursorStateClosed:
			summary.ClosedCount++
		}
		switch row.Mode {
		case CursorForwardOnly:
			summary.ForwardOnlyCount++
		case CursorScrollable:
			summary.ScrollableCount++
		}
		summary.TotalBatchSize += row.BatchSize
		summary.TotalMaxRows += row.MaxRows
		summary.TotalPosition += row.Position
		if row.Position > 0 {
			summary.PositionedCount++
		}
		if row.MaxRows > 0 {
			summary.ConfiguredMaxRows++
		}
	}
	return summary
}

func cursorStatusRows(cursors []CursorDescriptor) []ClientCursorStatusRow {
	if len(cursors) == 0 {
		return nil
	}
	rows := make([]ClientCursorStatusRow, 0, len(cursors))
	for _, cursor := range cursors {
		rows = append(rows, ClientCursorStatusRow{
			ID:        cursor.ID,
			RequestID: cursor.RequestID,
			Mode:      cursor.Mode,
			State:     cursor.State,
			BatchSize: cursor.BatchSize,
			MaxRows:   cursor.MaxRows,
			Position:  cursor.Position,
			Open:      cursor.Open(),
		})
	}
	return rows
}
