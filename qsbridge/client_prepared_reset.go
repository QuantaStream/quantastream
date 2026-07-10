package qsbridge

// ClientPreparedResetExchange is the metadata response for resetting a prepared handle.
type ClientPreparedResetExchange struct {
	Connection      ConnectionContext
	Handle          PreparedStatementHandle
	Prepared        PreparedPlan
	Reset           bool
	ClearedLongData bool
	Response        ClientResponseItem
	Diagnostics     DiagnosticSet
}

// ResetClientPreparedStatement validates a prepared handle and clears adapter-owned long-data state.
func (s PlanningService) ResetClientPreparedStatement(connection ConnectionContext, preparedRegistry PreparedStatementRegistry, longDataRegistry PreparedLongDataRegistry, handle PreparedStatementHandle) ClientPreparedResetExchange {
	_ = s
	result := ClientPreparedResetExchange{
		Connection:  cloneConnectionContext(connection),
		Handle:      handle,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}
	if handle.Empty() {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared statement reset requires a statement id or name"),
		})
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}
	if preparedRegistry == nil {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared statement registry is not configured"),
		})
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}
	prepared, ok := preparedRegistry.Get(handle)
	if !ok {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared statement handle not found"),
		})
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}

	response := preparedStatementResponse(connection.Protocol, "Prepared statement reset")
	result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, response.StatementResponse.Diagnostics)
	if response.Kind == ClientResponseError {
		result.Response = errorClientResponseItem(0, result.Diagnostics)
		return result
	}
	result.Prepared = clonePreparedPlan(prepared)
	if longDataRegistry != nil {
		result.ClearedLongData = longDataRegistry.ClearHandle(handle)
	}
	result.Reset = true
	result.Response = response
	result.Response.Outcome.Diagnostics = cloneDiagnosticSet(result.Diagnostics)
	return result
}

// Supported reports whether prepared reset metadata can proceed.
func (e ClientPreparedResetExchange) Supported() bool {
	return e.Connection.Supported() && e.Reset && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts prepared reset diagnostics into protocol-facing errors.
func (e ClientPreparedResetExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking prepared reset error, if any.
func (e ClientPreparedResetExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}
