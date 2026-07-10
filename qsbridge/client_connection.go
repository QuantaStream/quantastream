package qsbridge

// ClientConnectionOptions controls adapter handling for accepted connections.
type ClientConnectionOptions struct {
	RegisterSession  bool
	CapabilityPolicy ConnectionCapabilityPolicy
}

// ClientConnectionCloseOptions controls adapter handling for connection cleanup.
type ClientConnectionCloseOptions struct {
	RemoveSession bool
}

// ClientConnectionExchange is adapter-facing metadata for accepting a connection.
//
// It delegates authentication, returns the authenticated connection context,
// and can optionally register the session in an adapter-owned registry. It does
// not implement a wire handshake, TLS negotiation, password exchange, socket
// lifecycle, or persistent auth/session storage.
type ClientConnectionExchange struct {
	Request     ConnectionRequest
	Negotiation ConnectionCapabilityNegotiation
	Connection  ConnectionContext
	Registered  bool
	Diagnostics DiagnosticSet
}

// ClientConnectionCloseExchange is adapter-facing metadata for closing a connection.
type ClientConnectionCloseExchange struct {
	Connection      ConnectionContext
	CloseConnection bool
	RemovedSession  bool
	Response        ProtocolStatementResponse
	Diagnostics     DiagnosticSet
}

// PrepareClientConnection authenticates connection metadata and optionally stores the session.
func (s PlanningService) PrepareClientConnection(request ConnectionRequest, authenticator Authenticator, registry SessionRegistry, options ClientConnectionOptions) ClientConnectionExchange {
	_ = s
	negotiation := request.NegotiateCapabilities(options.CapabilityPolicy)
	connection := request.Authenticate(authenticator)
	connection.Capabilities = append(ClientCapabilities(nil), negotiation.Accepted...)
	connection.Diagnostics = mergeDiagnosticSets(connection.Diagnostics, negotiation.Diagnostics)
	exchange := ClientConnectionExchange{
		Request:     request.Clone(),
		Negotiation: cloneConnectionCapabilityNegotiation(negotiation),
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if !options.RegisterSession {
		return exchange
	}
	if registry == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "session registry is not configured"),
		})
		return exchange
	}
	if !registry.Put(connection.Session) {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "authenticated session could not be registered"),
		})
		return exchange
	}
	exchange.Registered = true
	return exchange
}

// Supported reports whether the connection is authenticated and metadata is usable.
func (e ClientConnectionExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts connection diagnostics into protocol-facing errors.
func (e ClientConnectionExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking connection error, if any.
func (e ClientConnectionExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

// PrepareClientConnectionClose prepares metadata for connection close and optional session cleanup.
func (s PlanningService) PrepareClientConnectionClose(connection ConnectionContext, registry SessionRegistry, options ClientConnectionCloseOptions) ClientConnectionCloseExchange {
	_ = s
	exchange := ClientConnectionCloseExchange{
		Connection:      cloneConnectionContext(connection),
		CloseConnection: true,
		Diagnostics:     cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	response := commandStatementResponse(connection.Protocol, "Connection close requested", nil)
	exchange.Response = cloneProtocolStatementResponse(response)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, response.Diagnostics)
	if exchange.Diagnostics.BlocksNative() || !options.RemoveSession {
		return exchange
	}
	if registry == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "session registry is not configured"),
		})
		return exchange
	}
	if !registry.Remove(connection.Session.ID) {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "session could not be removed"),
		})
		return exchange
	}
	exchange.RemovedSession = true
	return exchange
}

// Supported reports whether close metadata can proceed.
func (e ClientConnectionCloseExchange) Supported() bool {
	return e.Connection.Supported() && e.CloseConnection && e.Response.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts close diagnostics into protocol-facing errors.
func (e ClientConnectionCloseExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking close error, if any.
func (e ClientConnectionCloseExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}
