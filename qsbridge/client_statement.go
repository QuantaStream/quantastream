package qsbridge

// ClientStatementText is one SQL text item submitted by a protocol adapter.
//
// Adapters or parser bridges decide how raw protocol input is split into
// statements. qsbridge only carries the ordered text and planning context.
type ClientStatementText struct {
	Ordinal int
	SQL     string
}

// ClientStatementBundle is a parser-neutral client request containing SQL text.
//
// It supports single-statement and adapter-split multi-statement requests
// without parsing, executing, or owning protocol buffering.
type ClientStatementBundle struct {
	Connection  ConnectionContext
	Statements  []ClientStatementText
	PlanOptions ConnectionPlanOptions
	Diagnostics DiagnosticSet
}

// NewClientStatementBundle creates a statement bundle for one connection.
func NewClientStatementBundle(connection ConnectionContext, plan ConnectionPlanOptions, statements ...string) ClientStatementBundle {
	bundle := ClientStatementBundle{
		Connection:  cloneConnectionContext(connection),
		PlanOptions: cloneConnectionPlanOptions(plan),
		Statements:  make([]ClientStatementText, 0, len(statements)),
	}
	for i, sql := range statements {
		bundle.Statements = append(bundle.Statements, ClientStatementText{
			Ordinal: i,
			SQL:     sql,
		})
	}
	bundle.Diagnostics = bundle.validate()
	return bundle
}

// Supported reports whether the bundle can be planned by an adapter.
func (b ClientStatementBundle) Supported() bool {
	return b.Connection.Supported() && !b.Diagnostics.BlocksNative()
}

// PlanRequests creates one planning request per statement in bundle order.
func (b ClientStatementBundle) PlanRequests() []PlanRequest {
	if !b.Supported() {
		return nil
	}
	requests := make([]PlanRequest, 0, len(b.Statements))
	for _, statement := range b.Statements {
		requests = append(requests, b.Connection.PlanRequest(statement.SQL, b.PlanOptions))
	}
	return requests
}

// ProtocolErrors converts bundle diagnostics into protocol-facing errors.
func (b ClientStatementBundle) ProtocolErrors() []ProtocolError {
	return b.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking bundle error, if any.
func (b ClientStatementBundle) FirstProtocolError() (ProtocolError, bool) {
	return b.Diagnostics.FirstProtocolError()
}

func (b ClientStatementBundle) validate() DiagnosticSet {
	diagnostics := cloneDiagnosticSet(b.Connection.Diagnostics)
	if len(b.Statements) == 0 {
		diagnostics = append(diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseParse,
			"client request requires at least one SQL statement",
		))
	}
	if len(b.Statements) > 1 && !b.Connection.Supports(ClientCapabilityMultiStatements) {
		diagnostics = append(diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseParse,
			"client did not advertise multi-statement support",
		))
	}
	for _, statement := range b.Statements {
		if statement.SQL == "" {
			diagnostics = append(diagnostics, ErrorDiagnostic(
				DiagnosticParserBoundary,
				PhaseParse,
				"client statement text is empty",
			))
		}
	}
	return diagnostics
}

func cloneConnectionPlanOptions(options ConnectionPlanOptions) ConnectionPlanOptions {
	return ConnectionPlanOptions{
		DefaultSchema:  options.DefaultSchema,
		CatalogVersion: options.CatalogVersion,
		Scope:          clonePhysicalScope(options.Scope),
		Optimization:   options.Optimization.Clone(),
	}
}
