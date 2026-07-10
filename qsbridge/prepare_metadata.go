package qsbridge

// PreparedStatementID identifies a prepared statement at a protocol boundary.
type PreparedStatementID uint64

// PreparedStatementHandle is adapter-owned identity for one prepared statement.
//
// qsbridge records this identity when supplied, but it does not allocate ids,
// own a statement registry, or manage close/deallocate behavior.
type PreparedStatementHandle struct {
	ID   PreparedStatementID
	Name string
}

// PreparedPlanDescription is the protocol-neutral response shape for prepare.
type PreparedPlanDescription struct {
	Handle        PreparedStatementHandle
	SQL           string
	Kind          QueryKind
	AccessIntent  PhysicalAccessIntent
	Parameters    []ParameterRef
	ResultColumns []ResultColumn
	Statement     StatementResult
	Result        ResultShape
	Supported     bool
	Diagnostics   DiagnosticSet
}

// PreparedStatementCloseRequest describes an adapter request to close a prepared statement.
type PreparedStatementCloseRequest struct {
	Handle      PreparedStatementHandle
	Diagnostics DiagnosticSet
}

// WithHandle returns a copy of p with adapter-owned prepared-statement identity.
func (p PreparedPlan) WithHandle(handle PreparedStatementHandle) PreparedPlan {
	p.Handle = handle
	return p
}

// Description returns protocol-neutral prepare response metadata.
func (p PreparedPlan) Description() PreparedPlanDescription {
	return PreparedPlanDescription{
		Handle:        p.Handle,
		SQL:           p.SQL,
		Kind:          p.Kind,
		AccessIntent:  p.AccessIntent(),
		Parameters:    append([]ParameterRef(nil), p.Parameters...),
		ResultColumns: append([]ResultColumn(nil), p.ResultColumns...),
		Statement:     cloneStatementResult(p.Statement),
		Result:        p.Result,
		Supported:     p.Supported && !p.Diagnostics.BlocksNative(),
		Diagnostics:   cloneDiagnosticSet(p.Diagnostics),
	}
}

// SupportedForPrepare reports whether the prepared description has no blocking diagnostics.
func (d PreparedPlanDescription) SupportedForPrepare() bool {
	return d.Supported && !d.Diagnostics.BlocksNative()
}

// CloseRequest returns metadata for adapter-owned prepared statement close/deallocate.
func (p PreparedPlan) CloseRequest() PreparedStatementCloseRequest {
	request := PreparedStatementCloseRequest{Handle: p.Handle}
	if request.Handle.Empty() {
		request.Diagnostics = append(request.Diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseExecute,
			"prepared statement close requires a statement id or name",
		))
	}
	return request
}

// Supported reports whether the close request carries usable adapter identity.
func (r PreparedStatementCloseRequest) Supported() bool {
	return !r.Handle.Empty() && !r.Diagnostics.BlocksNative()
}

// Empty reports whether the handle has no adapter-owned identity.
func (h PreparedStatementHandle) Empty() bool {
	return h.ID == 0 && h.Name == ""
}
