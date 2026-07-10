package qsbridge

// ClientStatementExecutionOptions are execution preferences for one client statement.
//
// Ordinal matches the adapter-provided client statement ordinal. When Mode or
// Options are zero-valued, the bundle defaults are used.
type ClientStatementExecutionOptions struct {
	Ordinal int
	Mode    ProtocolExecutionMode
	Options ExecutionOptions
	Values  []ParameterValue
}

// ClientHandoffOptions are adapter-provided defaults and overrides for a bundle.
type ClientHandoffOptions struct {
	Mode       ProtocolExecutionMode
	Options    ExecutionOptions
	Values     []ParameterValue
	Statements []ClientStatementExecutionOptions
}

// ClientStatementHandoff is the final protocol-aware handoff for one client statement.
type ClientStatementHandoff struct {
	Statement ClientStatementText
	Plan      ClientStatementPlan
	Options   ClientStatementExecutionOptions
	Handoff   ProtocolRoutedAuthorizedExecutionRequest
}

// ClientHandoffBundle is the ordered final handoff metadata for a client request.
//
// The bundle gives protocol adapters one place to inspect native execution,
// legacy fallback, protocol rejection, route rejection, or access denial for
// every statement. It does not execute statements or buffer results.
type ClientHandoffBundle struct {
	Connection  ConnectionContext
	Statements  []ClientStatementHandoff
	Diagnostics DiagnosticSet
}

// PrepareClientStatementHandoffBundle prepares and routes each statement in order.
func (s PlanningService) PrepareClientStatementHandoffBundle(bundle ClientStatementBundle, options ClientHandoffOptions) ClientHandoffBundle {
	planned := s.PrepareClientStatementBundle(bundle)
	result := ClientHandoffBundle{
		Connection:  cloneConnectionContext(planned.Connection),
		Diagnostics: cloneDiagnosticSet(planned.Diagnostics),
	}
	if len(planned.Statements) == 0 {
		return result
	}

	result.Statements = make([]ClientStatementHandoff, 0, len(planned.Statements))
	for _, statement := range planned.Statements {
		statementOptions := options.optionsFor(statement.Statement.Ordinal)
		request := statement.Prepared.ExecutionRequest(statementOptions.Options, statementOptions.Values...)
		handoff := s.authorizeRouteAndNegotiate(
			statement.Prepared,
			request,
			planned.Connection.Protocol.NegotiateExecution(statementOptions.Mode, statementOptions.Options),
		)
		result.Statements = append(result.Statements, ClientStatementHandoff{
			Statement: statement.Statement,
			Plan:      cloneClientStatementPlan(statement),
			Options:   statementOptions.clone(),
			Handoff:   handoff,
		})
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, handoff.Diagnostics())
	}
	return result
}

// Supported reports whether every statement can proceed through its final handoff.
func (b ClientHandoffBundle) Supported() bool {
	if !b.Connection.Supported() {
		return false
	}
	if len(b.Statements) == 0 {
		return !b.Diagnostics.BlocksNative()
	}
	for _, statement := range b.Statements {
		if !statement.Handoff.Supported() {
			return false
		}
	}
	return true
}

// Outcomes returns protocol-facing handoff summaries in client statement order.
func (b ClientHandoffBundle) Outcomes() []ExecutionHandoffOutcome {
	if len(b.Statements) == 0 {
		return nil
	}
	outcomes := make([]ExecutionHandoffOutcome, 0, len(b.Statements))
	for _, statement := range b.Statements {
		outcomes = append(outcomes, statement.Handoff.Outcome())
	}
	return outcomes
}

// ProtocolErrors converts bundle handoff diagnostics into protocol-facing errors.
func (b ClientHandoffBundle) ProtocolErrors() []ProtocolError {
	return b.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking bundle handoff error, if any.
func (b ClientHandoffBundle) FirstProtocolError() (ProtocolError, bool) {
	return b.Diagnostics.FirstProtocolError()
}

func (o ClientHandoffOptions) optionsFor(ordinal int) ClientStatementExecutionOptions {
	result := ClientStatementExecutionOptions{
		Ordinal: ordinal,
		Mode:    o.Mode,
		Options: o.Options,
		Values:  append([]ParameterValue(nil), o.Values...),
	}
	for _, statement := range o.Statements {
		if statement.Ordinal != ordinal {
			continue
		}
		if statement.Mode != "" {
			result.Mode = statement.Mode
		}
		if !statement.Options.empty() {
			result.Options = statement.Options
		}
		if statement.Values != nil {
			result.Values = append([]ParameterValue(nil), statement.Values...)
		}
		return result
	}
	return result
}

func (o ClientStatementExecutionOptions) clone() ClientStatementExecutionOptions {
	o.Values = append([]ParameterValue(nil), o.Values...)
	return o
}

func (o ExecutionOptions) empty() bool {
	return o == ExecutionOptions{}
}

func cloneClientStatementPlan(plan ClientStatementPlan) ClientStatementPlan {
	return ClientStatementPlan{
		Statement: plan.Statement,
		Request:   plan.Request,
		Prepared:  clonePreparedPlan(plan.Prepared),
	}
}
