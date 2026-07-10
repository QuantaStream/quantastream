package qsbridge

// ClientDescribeStatementRequest asks qsbridge to describe SQL without executing it.
type ClientDescribeStatementRequest struct {
	Connection  ConnectionContext
	PlanOptions ConnectionPlanOptions
	SQL         string
}

// ClientDescribeExchange is adapter-facing statement metadata for SQL or a prepared handle.
//
// It carries parameter metadata, row schema metadata, and non-row statement
// response metadata without executing a plan, mutating a session, or allocating
// protocol packets.
type ClientDescribeExchange struct {
	Connection           ConnectionContext
	SQL                  string
	Handle               PreparedStatementHandle
	Description          PreparedPlanDescription
	ResultSchema         ProtocolResultSchema
	HasResultSchema      bool
	StatementResponse    ProtocolStatementResponse
	HasStatementResponse bool
	Diagnostics          DiagnosticSet
}

// DescribeClientStatement plans SQL and returns protocol-facing metadata.
func (s PlanningService) DescribeClientStatement(request ClientDescribeStatementRequest) ClientDescribeExchange {
	result := ClientDescribeExchange{
		Connection:  cloneConnectionContext(request.Connection),
		SQL:         request.SQL,
		Diagnostics: cloneDiagnosticSet(request.Connection.Diagnostics),
	}
	if !request.Connection.Supported() {
		return result
	}

	prepared := s.PrepareWithRequest(request.Connection.PlanRequest(request.SQL, request.PlanOptions))
	return result.withPreparedPlan(prepared, request.Connection.Protocol)
}

// DescribeClientPreparedStatement returns metadata for a registered prepared handle.
func (s PlanningService) DescribeClientPreparedStatement(connection ConnectionContext, registry PreparedStatementRegistry, handle PreparedStatementHandle) ClientDescribeExchange {
	_ = s
	result := ClientDescribeExchange{
		Connection:  cloneConnectionContext(connection),
		Handle:      handle,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return result
	}
	if registry == nil {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "prepared statement registry is not configured"),
		})
		return result
	}
	prepared, ok := registry.Get(handle)
	if !ok {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "prepared statement handle not found"),
		})
		return result
	}
	result.SQL = prepared.SQL
	return result.withPreparedPlan(prepared, connection.Protocol)
}

// Supported reports whether describe metadata is usable by an adapter.
func (e ClientDescribeExchange) Supported() bool {
	return e.Connection.Supported() && e.Description.SupportedForPrepare() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts describe diagnostics into protocol-facing errors.
func (e ClientDescribeExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking describe error, if any.
func (e ClientDescribeExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientDescribeExchange) withPreparedPlan(prepared PreparedPlan, profile ProtocolProfile) ClientDescribeExchange {
	e.Description = clonePreparedPlanDescription(prepared.Description())
	e.Diagnostics = mergeDiagnosticSets(e.Diagnostics, e.Description.Diagnostics)
	switch e.Description.Result.Kind {
	case ResultQuery:
		e.ResultSchema = NewProtocolResultSchema(profile, e.Description.ResultColumns)
		e.HasResultSchema = true
	case ResultStatement:
		e.StatementResponse = e.Description.Statement.ProtocolStatementResponse(profile)
		e.HasStatementResponse = true
		e.Diagnostics = mergeDiagnosticSets(e.Diagnostics, e.StatementResponse.Diagnostics)
	}
	return e
}
