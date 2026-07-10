package qsbridge

// ClientCursorFetchSummaryRow describes one cursor fetch validation result.
type ClientCursorFetchSummaryRow struct {
	CursorID        CursorID
	RequestID       ExecutionRequestID
	Mode            CursorMode
	State           CursorState
	Position        uint64
	BatchSize       int
	MaxRows         int
	RequestedRows   uint64
	Final           bool
	Supported       bool
	DiagnosticCodes []DiagnosticCode
}

// ClientCursorFetchSummaryExchange is adapter-facing cursor fetch metadata.
type ClientCursorFetchSummaryExchange struct {
	Connection          ConnectionContext
	Fetch               ClientCursorFetchExchange
	Rows                []ClientCursorFetchSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientCursorFetch returns row metadata for one cursor fetch exchange.
func (s PlanningService) SummarizeClientCursorFetch(connection ConnectionContext, fetch ClientCursorFetchExchange) ClientCursorFetchSummaryExchange {
	_ = s
	exchange := ClientCursorFetchSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Fetch:               cloneClientCursorFetchExchange(fetch),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientCursorFetchSummaryRow{cursorFetchSummaryRow(fetch)}
	}
	exchange.Result = exchange.cursorFetchSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether cursor fetch summary metadata can be returned.
func (e ClientCursorFetchSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientCursorFetchSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientCursorFetchSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientCursorFetchSummaryExchange) cursorFetchSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     cursorFetchSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.cursorFetchSummaryRows(),
		Final: true,
	})
}

func cursorFetchSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Cursor_id", Type: DataTypeString, Nullable: true},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Mode", Type: DataTypeString, Nullable: true},
		{Name: "State", Type: DataTypeString, Nullable: true},
		{Name: "Position", Type: DataTypeInt},
		{Name: "Batch_size", Type: DataTypeInt},
		{Name: "Max_rows", Type: DataTypeInt},
		{Name: "Requested_rows", Type: DataTypeInt},
		{Name: "Final", Type: DataTypeBool},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientCursorFetchSummaryExchange) cursorFetchSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.CursorID)),
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(string(row.Mode)),
			metadataStringCell(string(row.State)),
			metadataIntCell(int(row.Position)),
			metadataIntCell(row.BatchSize),
			metadataIntCell(row.MaxRows),
			metadataIntCell(int(row.RequestedRows)),
			metadataBoolCell(row.Final),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func cursorFetchSummaryRow(fetch ClientCursorFetchExchange) ClientCursorFetchSummaryRow {
	return ClientCursorFetchSummaryRow{
		CursorID:        fetch.Cursor.ID,
		RequestID:       fetch.Cursor.RequestID,
		Mode:            fetch.Cursor.Mode,
		State:           fetch.Cursor.State,
		Position:        fetch.Cursor.Position,
		BatchSize:       fetch.Cursor.BatchSize,
		MaxRows:         fetch.Cursor.MaxRows,
		RequestedRows:   fetch.RequestedRows,
		Final:           fetch.Final,
		Supported:       fetch.Supported(),
		DiagnosticCodes: fetch.Diagnostics.Codes(),
	}
}

func cloneClientCursorFetchExchange(exchange ClientCursorFetchExchange) ClientCursorFetchExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Cursor = cloneCursorDescriptor(exchange.Cursor)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}
