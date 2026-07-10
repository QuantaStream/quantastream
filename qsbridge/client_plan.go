package qsbridge

// ClientStatementPlan is the prepared-plan metadata for one client statement.
//
// It preserves the adapter-provided statement ordinal, the derived planning
// request, and the prepared-plan snapshot without executing the statement.
type ClientStatementPlan struct {
	Statement ClientStatementText
	Request   PlanRequest
	Prepared  PreparedPlan
}

// ClientPlanBundle is an ordered planning result for a client statement bundle.
//
// It is a metadata-only envelope. Protocol adapters still own SQL splitting,
// result buffering, statement execution, and error-to-wire translation.
type ClientPlanBundle struct {
	Connection  ConnectionContext
	Statements  []ClientStatementPlan
	Diagnostics DiagnosticSet
}

// PrepareClientStatementBundle prepares each adapter-provided statement in order.
func (s PlanningService) PrepareClientStatementBundle(bundle ClientStatementBundle) ClientPlanBundle {
	result := ClientPlanBundle{
		Connection:  cloneConnectionContext(bundle.Connection),
		Diagnostics: cloneDiagnosticSet(bundle.Diagnostics),
	}
	if !bundle.Supported() {
		return result
	}

	requests := bundle.PlanRequests()
	result.Statements = make([]ClientStatementPlan, 0, len(requests))
	for i, request := range requests {
		prepared := s.PrepareWithRequest(request)
		result.Statements = append(result.Statements, ClientStatementPlan{
			Statement: bundle.Statements[i],
			Request:   request,
			Prepared:  prepared,
		})
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, prepared.Diagnostics)
	}
	return result
}

// Supported reports whether every statement in the bundle is native-plannable.
func (b ClientPlanBundle) Supported() bool {
	return b.Connection.Supported() && !b.Diagnostics.BlocksNative()
}

// PreparedPlans returns prepared-plan snapshots in client statement order.
func (b ClientPlanBundle) PreparedPlans() []PreparedPlan {
	if len(b.Statements) == 0 {
		return nil
	}
	plans := make([]PreparedPlan, 0, len(b.Statements))
	for _, statement := range b.Statements {
		plans = append(plans, clonePreparedPlan(statement.Prepared))
	}
	return plans
}

// ProtocolErrors converts bundle planning diagnostics into protocol-facing errors.
func (b ClientPlanBundle) ProtocolErrors() []ProtocolError {
	return b.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking bundle planning error, if any.
func (b ClientPlanBundle) FirstProtocolError() (ProtocolError, bool) {
	return b.Diagnostics.FirstProtocolError()
}
