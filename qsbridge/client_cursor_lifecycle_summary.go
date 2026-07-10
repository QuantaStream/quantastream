package qsbridge

// ClientCursorLifecycleOperation identifies a cursor lifecycle operation.
type ClientCursorLifecycleOperation string

const (
	// ClientCursorLifecycleOpen identifies cursor open metadata.
	ClientCursorLifecycleOpen ClientCursorLifecycleOperation = "open"
	// ClientCursorLifecycleAdvance identifies cursor advance metadata.
	ClientCursorLifecycleAdvance ClientCursorLifecycleOperation = "advance"
	// ClientCursorLifecycleClose identifies cursor close metadata.
	ClientCursorLifecycleClose ClientCursorLifecycleOperation = "close"
)

// ClientCursorLifecycleSummaryRow describes one cursor lifecycle exchange.
type ClientCursorLifecycleSummaryRow struct {
	Operation       ClientCursorLifecycleOperation
	CursorID        CursorID
	RequestID       ExecutionRequestID
	Mode            CursorMode
	State           CursorState
	Position        uint64
	BatchSize       int
	MaxRows         int
	Applied         bool
	Supported       bool
	DiagnosticCodes []DiagnosticCode
}

// ClientCursorLifecycleSummaryExchange is adapter-facing cursor lifecycle metadata.
type ClientCursorLifecycleSummaryExchange struct {
	Connection          ConnectionContext
	Rows                []ClientCursorLifecycleSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientCursorOpen returns row metadata for one cursor open exchange.
func (s PlanningService) SummarizeClientCursorOpen(connection ConnectionContext, opened ClientCursorOpenExchange) ClientCursorLifecycleSummaryExchange {
	_ = s
	return newClientCursorLifecycleSummary(connection, cursorLifecycleSummaryRow(
		ClientCursorLifecycleOpen,
		opened.Cursor,
		opened.Opened,
		opened.Supported(),
		opened.Diagnostics,
	))
}

// SummarizeClientCursorAdvance returns row metadata for one cursor advance exchange.
func (s PlanningService) SummarizeClientCursorAdvance(connection ConnectionContext, advanced ClientCursorAdvanceExchange) ClientCursorLifecycleSummaryExchange {
	_ = s
	return newClientCursorLifecycleSummary(connection, cursorLifecycleSummaryRow(
		ClientCursorLifecycleAdvance,
		advanced.Cursor,
		advanced.Advanced,
		advanced.Supported(),
		advanced.Diagnostics,
	))
}

// SummarizeClientCursorClose returns row metadata for one cursor close exchange.
func (s PlanningService) SummarizeClientCursorClose(connection ConnectionContext, closed ClientCursorCloseExchange) ClientCursorLifecycleSummaryExchange {
	_ = s
	return newClientCursorLifecycleSummary(connection, cursorLifecycleSummaryRow(
		ClientCursorLifecycleClose,
		closed.Cursor,
		closed.Closed,
		closed.Supported(),
		closed.Diagnostics,
	))
}

// Supported reports whether cursor lifecycle summary metadata can be returned.
func (e ClientCursorLifecycleSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientCursorLifecycleSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientCursorLifecycleSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func newClientCursorLifecycleSummary(connection ConnectionContext, row ClientCursorLifecycleSummaryRow) ClientCursorLifecycleSummaryExchange {
	exchange := ClientCursorLifecycleSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientCursorLifecycleSummaryRow{row}
	}
	exchange.Result = exchange.cursorLifecycleSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

func (e ClientCursorLifecycleSummaryExchange) cursorLifecycleSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     cursorLifecycleSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.cursorLifecycleSummaryRows(),
		Final: true,
	})
}

func cursorLifecycleSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Operation", Type: DataTypeString},
		{Name: "Cursor_id", Type: DataTypeString, Nullable: true},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Mode", Type: DataTypeString, Nullable: true},
		{Name: "State", Type: DataTypeString, Nullable: true},
		{Name: "Position", Type: DataTypeInt},
		{Name: "Batch_size", Type: DataTypeInt},
		{Name: "Max_rows", Type: DataTypeInt},
		{Name: "Applied", Type: DataTypeBool},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientCursorLifecycleSummaryExchange) cursorLifecycleSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Operation)),
			metadataStringCell(string(row.CursorID)),
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(string(row.Mode)),
			metadataStringCell(string(row.State)),
			metadataIntCell(int(row.Position)),
			metadataIntCell(row.BatchSize),
			metadataIntCell(row.MaxRows),
			metadataBoolCell(row.Applied),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func cursorLifecycleSummaryRow(operation ClientCursorLifecycleOperation, cursor CursorDescriptor, applied bool, supported bool, diagnostics DiagnosticSet) ClientCursorLifecycleSummaryRow {
	return ClientCursorLifecycleSummaryRow{
		Operation:       operation,
		CursorID:        cursor.ID,
		RequestID:       cursor.RequestID,
		Mode:            cursor.Mode,
		State:           cursor.State,
		Position:        cursor.Position,
		BatchSize:       cursor.BatchSize,
		MaxRows:         cursor.MaxRows,
		Applied:         applied,
		Supported:       supported,
		DiagnosticCodes: diagnostics.Codes(),
	}
}
