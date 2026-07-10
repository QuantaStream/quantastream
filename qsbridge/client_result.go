package qsbridge

// ClientStatementResultPreview describes adapter-visible result metadata for one statement.
//
// It is derived from handoff metadata and empty result envelopes. It does not
// execute a native request, call legacy fallback, or buffer rows.
type ClientStatementResultPreview struct {
	Statement            ClientStatementText
	Outcome              ExecutionHandoffOutcome
	Result               ExecutionResult
	Schema               ProtocolResultSchema
	HasSchema            bool
	StatementResponse    ProtocolStatementResponse
	HasStatementResponse bool
}

// ClientResultPreviewBundle is ordered result metadata for a client request.
type ClientResultPreviewBundle struct {
	Connection  ConnectionContext
	Statements  []ClientStatementResultPreview
	Diagnostics DiagnosticSet
}

// ResultPreviewBundle creates ordered protocol-facing result metadata.
func (b ClientHandoffBundle) ResultPreviewBundle() ClientResultPreviewBundle {
	result := ClientResultPreviewBundle{
		Connection:  cloneConnectionContext(b.Connection),
		Diagnostics: cloneDiagnosticSet(b.Diagnostics),
	}
	if len(b.Statements) == 0 {
		return result
	}

	result.Statements = make([]ClientStatementResultPreview, 0, len(b.Statements))
	for _, statement := range b.Statements {
		preview := statement.resultPreview(b.Connection.Protocol)
		result.Statements = append(result.Statements, preview)
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, preview.Diagnostics())
	}
	return result
}

// Supported reports whether the preview metadata has no blocking diagnostics.
func (b ClientResultPreviewBundle) Supported() bool {
	return b.Connection.Supported() && !b.Diagnostics.BlocksNative()
}

// ProtocolErrors converts preview diagnostics into protocol-facing errors.
func (b ClientResultPreviewBundle) ProtocolErrors() []ProtocolError {
	return b.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking preview error, if any.
func (b ClientResultPreviewBundle) FirstProtocolError() (ProtocolError, bool) {
	return b.Diagnostics.FirstProtocolError()
}

// Diagnostics returns result and protocol-response diagnostics for one preview.
func (p ClientStatementResultPreview) Diagnostics() DiagnosticSet {
	diagnostics := mergeDiagnosticSets(p.Outcome.Diagnostics, p.Result.Diagnostics)
	if p.HasStatementResponse {
		diagnostics = mergeDiagnosticSets(diagnostics, p.StatementResponse.Diagnostics)
	}
	return diagnostics
}

func (h ClientStatementHandoff) resultPreview(profile ProtocolProfile) ClientStatementResultPreview {
	outcome := h.Handoff.Outcome()
	result := h.Handoff.Request.EmptyResult()
	if !h.Handoff.Supported() {
		result = result.WithDispatchDiagnostics(outcome.Diagnostics)
	}
	preview := ClientStatementResultPreview{
		Statement: h.Statement,
		Outcome:   outcome,
		Result:    cloneExecutionResult(result),
	}
	switch result.Kind {
	case ResultQuery:
		preview.Schema = result.ProtocolResultSchema(profile)
		preview.HasSchema = true
	case ResultStatement:
		preview.StatementResponse = result.ProtocolStatementResponse(profile)
		preview.HasStatementResponse = true
	}
	return preview
}
