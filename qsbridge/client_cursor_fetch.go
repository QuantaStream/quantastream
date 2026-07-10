package qsbridge

// ClientCursorFetchExchange is the metadata response for a cursor fetch request.
type ClientCursorFetchExchange struct {
	Connection    ConnectionContext
	Cursor        CursorDescriptor
	RequestedRows uint64
	Final         bool
	Diagnostics   DiagnosticSet
}

// PrepareClientCursorFetch validates a forward-only cursor fetch request without advancing it.
func (s PlanningService) PrepareClientCursorFetch(connection ConnectionContext, registry CursorRegistry, id CursorID, requestedRows int) ClientCursorFetchExchange {
	_ = s
	exchange := ClientCursorFetchExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if registry == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor registry is not configured"),
		})
		return exchange
	}
	if id == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor fetch requires a cursor id"),
		})
		return exchange
	}
	cursor, ok := registry.Get(id)
	if !ok {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor handle not found"),
		})
		return exchange
	}
	exchange.Cursor = cloneCursorDescriptor(cursor)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, validateClientCursorFetch(cursor, requestedRows))
	if exchange.Diagnostics.BlocksNative() {
		return exchange
	}
	exchange.RequestedRows, exchange.Final = cursorFetchRows(cursor, requestedRows)
	return exchange
}

// Supported reports whether cursor fetch metadata can proceed.
func (e ClientCursorFetchExchange) Supported() bool {
	return e.Connection.Supported() && e.Cursor.Open() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts cursor fetch diagnostics into protocol-facing errors.
func (e ClientCursorFetchExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking cursor fetch error, if any.
func (e ClientCursorFetchExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func validateClientCursorFetch(cursor CursorDescriptor, requestedRows int) DiagnosticSet {
	diagnostics := make(DiagnosticSet, 0)
	if cursor.State != CursorStateOpen {
		diagnostics = append(diagnostics, ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor fetch requires an open cursor"))
	}
	if cursor.Mode != CursorForwardOnly {
		diagnostics = append(diagnostics, ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor fetch requires a forward-only cursor"))
	}
	if requestedRows <= 0 && cursor.BatchSize <= 0 {
		diagnostics = append(diagnostics, ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor fetch requires a positive row count or batch size"))
	}
	return diagnostics
}

func cursorFetchRows(cursor CursorDescriptor, requestedRows int) (uint64, bool) {
	rows := requestedRows
	if rows <= 0 {
		rows = cursor.BatchSize
	}
	if rows <= 0 {
		return 0, false
	}
	want := uint64(rows)
	if cursor.MaxRows <= 0 {
		return want, false
	}
	maxRows := uint64(cursor.MaxRows)
	if cursor.Position >= maxRows {
		return 0, true
	}
	remaining := maxRows - cursor.Position
	if want >= remaining {
		return remaining, true
	}
	return want, false
}
