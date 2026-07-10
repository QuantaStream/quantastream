package qsbridge

// ClientCommandKind identifies a protocol command outside normal SQL text.
type ClientCommandKind string

const (
	// ClientCommandPing describes a protocol ping/health command.
	ClientCommandPing ClientCommandKind = "ping"
	// ClientCommandQuit describes a client-requested connection close.
	ClientCommandQuit ClientCommandKind = "quit"
	// ClientCommandResetConnection describes a protocol-level session reset.
	ClientCommandResetConnection ClientCommandKind = "reset_connection"
	// ClientCommandInitSchema describes a protocol-level current-schema change.
	ClientCommandInitSchema ClientCommandKind = "init_schema"
)

// ClientCommandOptions control adapter handling for protocol commands.
type ClientCommandOptions struct {
	ApplySession  bool
	RemoveSession bool
}

// ClientCommandExchange is metadata for a non-SQL protocol command.
//
// It lets MySQL and future adapters handle command traffic without pretending
// every packet is SQL text. qsbridge only builds response/session metadata; it
// does not own sockets, packet encoding, connection close, or live session
// mutation.
type ClientCommandExchange struct {
	Connection      ConnectionContext
	Kind            ClientCommandKind
	Payload         string
	CloseConnection bool
	Session         ClientSessionActionExchange
	Response        ProtocolStatementResponse
	Diagnostics     DiagnosticSet
}

// PrepareClientCommand prepares metadata for a non-SQL protocol command.
func (s PlanningService) PrepareClientCommand(connection ConnectionContext, registry SessionRegistry, kind ClientCommandKind, payload string, options ClientCommandOptions) ClientCommandExchange {
	exchange := ClientCommandExchange{
		Connection:  cloneConnectionContext(connection),
		Kind:        kind,
		Payload:     payload,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}

	switch kind {
	case ClientCommandPing:
		exchange.Response = commandStatementResponse(connection.Protocol, "OK", nil)
	case ClientCommandQuit:
		close := s.PrepareClientConnectionClose(connection, registry, ClientConnectionCloseOptions{RemoveSession: options.RemoveSession})
		exchange.CloseConnection = close.CloseConnection
		exchange.Response = cloneProtocolStatementResponse(close.Response)
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, close.Diagnostics)
	case ClientCommandResetConnection:
		exchange = s.prepareClientResetCommand(exchange, connection, registry, options)
	case ClientCommandInitSchema:
		schema := s.PrepareClientUseSchema(connection, registry, payload, ClientSchemaSelectionOptions{ApplySession: options.ApplySession})
		exchange.Session = schema.Session
		exchange.Response = cloneProtocolStatementResponse(schema.Response)
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, schema.Diagnostics)
	default:
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "unsupported client command: "+string(kind)),
		})
	}
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, exchange.Response.Diagnostics)
	return exchange
}

// Supported reports whether command metadata can proceed.
func (e ClientCommandExchange) Supported() bool {
	return e.Connection.Supported() && e.Response.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts command diagnostics into protocol-facing errors.
func (e ClientCommandExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking command error, if any.
func (e ClientCommandExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (s PlanningService) prepareClientResetCommand(exchange ClientCommandExchange, connection ConnectionContext, registry SessionRegistry, options ClientCommandOptions) ClientCommandExchange {
	_ = s
	reset := resetClientSession(connection.Session)
	transition := SessionTransition{
		Before:  connection.Session.Clone(),
		After:   reset.Clone(),
		Actions: []SessionAction{{Kind: SessionActionResetConnection}},
	}
	exchange.Session = ClientSessionActionExchange{
		Connection: cloneConnectionContext(connection),
		Transition: cloneSessionTransition(transition),
	}
	if options.ApplySession {
		if registry == nil {
			exchange.Session.Diagnostics = mergeDiagnosticSets(exchange.Session.Diagnostics, DiagnosticSet{
				ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "session registry is not configured"),
			})
		} else if !registry.Put(reset) {
			exchange.Session.Diagnostics = mergeDiagnosticSets(exchange.Session.Diagnostics, DiagnosticSet{
				ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "session reset could not be applied"),
			})
		} else {
			exchange.Session.Applied = true
		}
	}
	statement := StatementResult{
		Status:         "Connection reset",
		SessionActions: []SessionAction{{Kind: SessionActionResetConnection}},
	}
	exchange.Response = commandStatementResponse(connection.Protocol, statement.Status, statement.SessionActions)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, exchange.Session.Diagnostics)
	return exchange
}

func commandStatementResponse(profile ProtocolProfile, status string, actions []SessionAction) ProtocolStatementResponse {
	return StatementResult{
		Status:         status,
		SessionActions: cloneSessionActions(actions),
	}.ProtocolStatementResponse(profile)
}

func resetClientSession(session SessionContext) SessionContext {
	return SessionContext{
		ID:    session.ID,
		User:  session.User,
		Roles: append([]RoleName(nil), session.Roles...),
	}
}
