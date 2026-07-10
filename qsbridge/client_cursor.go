package qsbridge

// ClientCursorOpenExchange is the metadata response for opening a result cursor.
type ClientCursorOpenExchange struct {
	Connection  ConnectionContext
	Cursor      CursorDescriptor
	Opened      bool
	Diagnostics DiagnosticSet
}

// ClientCursorAdvanceExchange is the metadata response for advancing cursor position.
type ClientCursorAdvanceExchange struct {
	Connection  ConnectionContext
	Cursor      CursorDescriptor
	Advanced    bool
	Diagnostics DiagnosticSet
}

// ClientCursorCloseExchange is the metadata response for closing cursor metadata.
type ClientCursorCloseExchange struct {
	Connection  ConnectionContext
	Cursor      CursorDescriptor
	Closed      bool
	Diagnostics DiagnosticSet
}

// OpenClientResultCursor stores adapter-owned cursor metadata for a result envelope.
func (s PlanningService) OpenClientResultCursor(connection ConnectionContext, registry CursorRegistry, result ExecutionResult) ClientCursorOpenExchange {
	_ = s
	exchange := ClientCursorOpenExchange{
		Connection:  cloneConnectionContext(connection),
		Cursor:      cloneCursorDescriptor(result.Cursor),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, result.Diagnostics),
	}
	if !connection.Supported() || exchange.Diagnostics.BlocksNative() {
		return exchange
	}
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, validateClientCursorOpen(connection.Protocol, result.Cursor))
	if exchange.Diagnostics.BlocksNative() {
		return exchange
	}
	if registry == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor registry is not configured"),
		})
		return exchange
	}
	cursor, ok := registry.Open(result.Cursor)
	if !ok {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor could not be opened"),
		})
		return exchange
	}
	exchange.Cursor = cursor
	exchange.Opened = true
	return exchange
}

// AdvanceClientCursor records adapter-owned cursor progress after result delivery.
func (s PlanningService) AdvanceClientCursor(connection ConnectionContext, registry CursorRegistry, id CursorID, rows uint64, final bool) ClientCursorAdvanceExchange {
	_ = s
	exchange := ClientCursorAdvanceExchange{
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
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor advance requires a cursor id"),
		})
		return exchange
	}
	cursor, ok := registry.Advance(id, rows, final)
	if !ok {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor handle not found or closed"),
		})
		return exchange
	}
	exchange.Cursor = cursor
	exchange.Advanced = true
	return exchange
}

// CloseClientCursor marks adapter-owned cursor metadata closed.
func (s PlanningService) CloseClientCursor(connection ConnectionContext, registry CursorRegistry, id CursorID) ClientCursorCloseExchange {
	_ = s
	exchange := ClientCursorCloseExchange{
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
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor close requires a cursor id"),
		})
		return exchange
	}
	cursor, ok := registry.Close(id)
	if !ok {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor handle not found"),
		})
		return exchange
	}
	exchange.Cursor = cursor
	exchange.Closed = true
	return exchange
}

// Supported reports whether cursor open metadata can proceed.
func (e ClientCursorOpenExchange) Supported() bool {
	return e.Connection.Supported() && e.Opened && !e.Diagnostics.BlocksNative()
}

// Supported reports whether cursor advance metadata can proceed.
func (e ClientCursorAdvanceExchange) Supported() bool {
	return e.Connection.Supported() && e.Advanced && !e.Diagnostics.BlocksNative()
}

// Supported reports whether cursor close metadata can proceed.
func (e ClientCursorCloseExchange) Supported() bool {
	return e.Connection.Supported() && e.Closed && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts cursor open diagnostics into protocol-facing errors.
func (e ClientCursorOpenExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// ProtocolErrors converts cursor advance diagnostics into protocol-facing errors.
func (e ClientCursorAdvanceExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// ProtocolErrors converts cursor close diagnostics into protocol-facing errors.
func (e ClientCursorCloseExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking cursor open error, if any.
func (e ClientCursorOpenExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

// FirstProtocolError returns the first blocking cursor advance error, if any.
func (e ClientCursorAdvanceExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

// FirstProtocolError returns the first blocking cursor close error, if any.
func (e ClientCursorCloseExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func validateClientCursorOpen(profile ProtocolProfile, cursor CursorDescriptor) DiagnosticSet {
	diagnostics := make(DiagnosticSet, 0)
	if cursor.ID == "" {
		diagnostics = append(diagnostics, ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor open requires a cursor id"))
	}
	if cursor.State != CursorStateOpen {
		diagnostics = append(diagnostics, ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor open requires an open cursor descriptor"))
	}
	switch cursor.Mode {
	case CursorForwardOnly:
		if !profile.Supports(ProtocolCapabilityForwardOnlyCursor) {
			diagnostics = append(diagnostics, protocolCapabilityDiagnostic("forward-only cursors are not supported by protocol profile"))
		}
	case CursorScrollable:
		diagnostics = append(diagnostics, ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "scrollable cursors are not part of the native execution scaffold"))
	case CursorNone:
		diagnostics = append(diagnostics, ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cursor open requires a cursor mode"))
	default:
		diagnostics = append(diagnostics, ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "unknown cursor mode: "+string(cursor.Mode)))
	}
	return diagnostics
}
