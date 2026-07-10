package qsbridge

// ClientExchange is the end-to-end adapter metadata for one client request.
//
// It composes adapter-provided statement text, planning handoff decisions, and
// protocol-facing result previews. It still does not execute statements, call
// legacy fallback, buffer rows, mutate sessions, or own network state.
type ClientExchange struct {
	Request     ClientStatementBundle
	Connection  ConnectionContext
	Handoff     ClientHandoffBundle
	Preview     ClientResultPreviewBundle
	Diagnostics DiagnosticSet
}

// PrepareClientExchange prepares handoff and result-preview metadata for a bundle.
func (s PlanningService) PrepareClientExchange(bundle ClientStatementBundle, options ClientHandoffOptions) ClientExchange {
	handoff := s.PrepareClientStatementHandoffBundle(bundle, options)
	preview := handoff.ResultPreviewBundle()
	return ClientExchange{
		Request:     cloneClientStatementBundle(bundle),
		Connection:  cloneConnectionContext(preview.Connection),
		Handoff:     handoff,
		Preview:     preview,
		Diagnostics: mergeDiagnosticSets(bundle.Diagnostics, handoff.Diagnostics, preview.Diagnostics),
	}
}

// PrepareConnectionClientExchange creates a statement bundle and prepares exchange metadata.
func (s PlanningService) PrepareConnectionClientExchange(connection ConnectionContext, plan ConnectionPlanOptions, options ClientHandoffOptions, statements ...string) ClientExchange {
	return s.PrepareClientExchange(NewClientStatementBundle(connection, plan, statements...), options)
}

// Supported reports whether the request can proceed through all metadata gates.
func (e ClientExchange) Supported() bool {
	return e.Request.Supported() && e.Handoff.Supported() && e.Preview.Supported()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

// Outcomes returns final handoff summaries in client statement order.
func (e ClientExchange) Outcomes() []ExecutionHandoffOutcome {
	return e.Handoff.Outcomes()
}

func cloneClientStatementBundle(bundle ClientStatementBundle) ClientStatementBundle {
	return ClientStatementBundle{
		Connection:  cloneConnectionContext(bundle.Connection),
		Statements:  append([]ClientStatementText(nil), bundle.Statements...),
		PlanOptions: cloneConnectionPlanOptions(bundle.PlanOptions),
		Diagnostics: cloneDiagnosticSet(bundle.Diagnostics),
	}
}
