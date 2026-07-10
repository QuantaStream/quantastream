package qsbridge

// ClientChangeUserOptions controls adapter handling for a re-authenticated session.
type ClientChangeUserOptions struct {
	ApplySession     bool
	CapabilityPolicy ConnectionCapabilityPolicy
}

// ClientChangeUserExchange is adapter-facing metadata for a change-user command.
type ClientChangeUserExchange struct {
	Connection  ConnectionContext
	Request     ConnectionRequest
	Negotiation ConnectionCapabilityNegotiation
	Transition  SessionTransition
	Applied     bool
	Response    ProtocolStatementResponse
	Diagnostics DiagnosticSet
}

// PrepareClientChangeUser authenticates replacement user metadata for an existing connection.
func (s PlanningService) PrepareClientChangeUser(connection ConnectionContext, request ConnectionRequest, authenticator Authenticator, registry SessionRegistry, options ClientChangeUserOptions) ClientChangeUserExchange {
	_ = s
	request.SessionID = connection.Session.ID
	request.Authentication.SessionID = connection.Session.ID
	negotiation := request.NegotiateCapabilities(options.CapabilityPolicy)
	next := request.Authenticate(authenticator)
	next.Capabilities = append(ClientCapabilities(nil), negotiation.Accepted...)
	next.Diagnostics = mergeDiagnosticSets(next.Diagnostics, negotiation.Diagnostics)
	transition := SessionTransition{
		Before:  connection.Session.Clone(),
		After:   next.Session.Clone(),
		Actions: []SessionAction{{Kind: SessionActionChangeUser}},
	}
	exchange := ClientChangeUserExchange{
		Connection:  cloneConnectionContext(next),
		Request:     request.Clone(),
		Negotiation: cloneConnectionCapabilityNegotiation(negotiation),
		Transition:  cloneSessionTransition(transition),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, next.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if !next.Supported() {
		return exchange
	}
	response := commandStatementResponse(next.Protocol, "User changed", transition.Actions)
	exchange.Response = cloneProtocolStatementResponse(response)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, response.Diagnostics)
	if exchange.Diagnostics.BlocksNative() || !options.ApplySession {
		return exchange
	}
	if registry == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "session registry is not configured"),
		})
		return exchange
	}
	if !registry.Put(next.Session) {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "changed user session could not be registered"),
		})
		return exchange
	}
	exchange.Applied = true
	return exchange
}

// Supported reports whether change-user metadata can proceed.
func (e ClientChangeUserExchange) Supported() bool {
	return e.Connection.Supported() && e.Transition.Supported() && e.Response.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts change-user diagnostics into protocol-facing errors.
func (e ClientChangeUserExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking change-user error, if any.
func (e ClientChangeUserExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}
