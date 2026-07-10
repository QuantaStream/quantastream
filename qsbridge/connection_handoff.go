package qsbridge

// ConnectionProtocolExecutionHandoff combines connection metadata with a final execution handoff.
type ConnectionProtocolExecutionHandoff struct {
	Connection ConnectionContext
	Handoff    ProtocolRoutedAuthorizedExecutionRequest
}

// ConnectionProtocolBatchHandoff combines connection metadata with a final batch handoff.
type ConnectionProtocolBatchHandoff struct {
	Connection ConnectionContext
	Handoff    ProtocolRoutedAuthorizedBatchExecutionRequest
}

// PrepareConnectionProtocolExecutionHandoff prepares SQL from a connection and returns final metadata.
func (s PlanningService) PrepareConnectionProtocolExecutionHandoff(connection ConnectionContext, sql string, plan ConnectionPlanOptions, mode ProtocolExecutionMode, options ExecutionOptions, values ...ParameterValue) ConnectionProtocolExecutionHandoff {
	if !connection.Supported() {
		return ConnectionProtocolExecutionHandoff{Connection: cloneConnectionContext(connection)}
	}
	request := connection.PlanRequest(sql, plan)
	return ConnectionProtocolExecutionHandoff{
		Connection: cloneConnectionContext(connection),
		Handoff:    s.PrepareProtocolRoutedAuthorizedExecutionRequest(request, connection.Protocol, mode, options, values...),
	}
}

// PrepareConnectionProtocolBatchHandoff prepares SQL from a connection and returns final batch metadata.
func (s PlanningService) PrepareConnectionProtocolBatchHandoff(connection ConnectionContext, sql string, plan ConnectionPlanOptions, options ExecutionOptions, sets ...ParameterValueSet) ConnectionProtocolBatchHandoff {
	if !connection.Supported() {
		return ConnectionProtocolBatchHandoff{Connection: cloneConnectionContext(connection)}
	}
	request := connection.PlanRequest(sql, plan)
	return ConnectionProtocolBatchHandoff{
		Connection: cloneConnectionContext(connection),
		Handoff:    s.PrepareProtocolRoutedAuthorizedBatchExecutionRequest(request, connection.Protocol, options, sets...),
	}
}

// Supported reports whether the connection and final handoff both allow execution.
func (h ConnectionProtocolExecutionHandoff) Supported() bool {
	return h.Connection.Supported() && h.Handoff.Supported()
}

// Supported reports whether the connection and final batch handoff both allow execution.
func (h ConnectionProtocolBatchHandoff) Supported() bool {
	return h.Connection.Supported() && h.Handoff.Supported()
}

// HandoffKind returns the connection-aware final execution path or error class.
func (h ConnectionProtocolExecutionHandoff) HandoffKind() ExecutionHandoffKind {
	if !h.Connection.Supported() {
		return ExecutionHandoffDenied
	}
	return h.Handoff.HandoffKind()
}

// HandoffKind returns the connection-aware final batch execution path or error class.
func (h ConnectionProtocolBatchHandoff) HandoffKind() ExecutionHandoffKind {
	if !h.Connection.Supported() {
		return ExecutionHandoffDenied
	}
	return h.Handoff.HandoffKind()
}

// Diagnostics returns connection and handoff diagnostics in adapter-facing order.
func (h ConnectionProtocolExecutionHandoff) Diagnostics() DiagnosticSet {
	return mergeDiagnosticSets(h.Connection.Diagnostics, h.Handoff.Diagnostics())
}

// Diagnostics returns connection and batch handoff diagnostics in adapter-facing order.
func (h ConnectionProtocolBatchHandoff) Diagnostics() DiagnosticSet {
	return mergeDiagnosticSets(h.Connection.Diagnostics, h.Handoff.Diagnostics())
}

// Outcome returns the protocol-facing summary of this connection-aware handoff.
func (h ConnectionProtocolExecutionHandoff) Outcome() ExecutionHandoffOutcome {
	return ExecutionHandoffOutcome{
		Kind:        h.HandoffKind(),
		Supported:   h.Supported(),
		Diagnostics: h.Diagnostics(),
	}
}

// Outcome returns the protocol-facing summary of this connection-aware batch handoff.
func (h ConnectionProtocolBatchHandoff) Outcome() ExecutionHandoffOutcome {
	return ExecutionHandoffOutcome{
		Kind:        h.HandoffKind(),
		Supported:   h.Supported(),
		Diagnostics: h.Diagnostics(),
	}
}

// ProtocolErrors converts connection-aware diagnostics into protocol-facing errors.
func (h ConnectionProtocolExecutionHandoff) ProtocolErrors() []ProtocolError {
	return h.Diagnostics().ProtocolErrors()
}

// FirstProtocolError returns the first blocking connection-aware handoff error, if any.
func (h ConnectionProtocolExecutionHandoff) FirstProtocolError() (ProtocolError, bool) {
	return h.Diagnostics().FirstProtocolError()
}

// ProtocolErrors converts connection-aware batch diagnostics into protocol-facing errors.
func (h ConnectionProtocolBatchHandoff) ProtocolErrors() []ProtocolError {
	return h.Diagnostics().ProtocolErrors()
}

// FirstProtocolError returns the first blocking connection-aware batch handoff error, if any.
func (h ConnectionProtocolBatchHandoff) FirstProtocolError() (ProtocolError, bool) {
	return h.Diagnostics().FirstProtocolError()
}

// NativeRequest returns the native execution descriptor when connection-aware handoff selected native.
func (h ConnectionProtocolExecutionHandoff) NativeRequest() (ExecutionRequest, bool) {
	if !h.Connection.Supported() {
		return ExecutionRequest{}, false
	}
	return h.Handoff.NativeRequest()
}

// NativeRequest returns the native batch descriptor when connection-aware handoff selected native.
func (h ConnectionProtocolBatchHandoff) NativeRequest() (BatchExecutionRequest, bool) {
	if !h.Connection.Supported() {
		return BatchExecutionRequest{}, false
	}
	return h.Handoff.NativeRequest()
}

// LegacyFallbackRequest returns the fallback descriptor when connection-aware handoff selected legacy.
func (h ConnectionProtocolExecutionHandoff) LegacyFallbackRequest() (FallbackRequest, bool) {
	if !h.Connection.Supported() {
		return FallbackRequest{}, false
	}
	return h.Handoff.LegacyFallbackRequest()
}

// LegacyFallbackRequest returns the fallback descriptor when connection-aware batch handoff selected legacy.
func (h ConnectionProtocolBatchHandoff) LegacyFallbackRequest() (BatchFallbackRequest, bool) {
	if !h.Connection.Supported() {
		return BatchFallbackRequest{}, false
	}
	return h.Handoff.LegacyFallbackRequest()
}

func cloneConnectionContext(context ConnectionContext) ConnectionContext {
	return ConnectionContext{
		Session:      context.Session.Clone(),
		Protocol:     context.Protocol.Clone(),
		Capabilities: append(ClientCapabilities(nil), context.Capabilities...),
		Attributes:   cloneStringMap(context.Attributes),
		Diagnostics:  cloneDiagnosticSet(context.Diagnostics),
	}
}
