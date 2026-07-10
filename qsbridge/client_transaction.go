package qsbridge

// ClientTransactionOptions control adapter handling for transaction actions.
type ClientTransactionOptions struct {
	ApplySession bool
}

// ClientTransactionExchange is client-facing metadata for a transaction action.
type ClientTransactionExchange struct {
	Connection  ConnectionContext
	Action      SessionAction
	Session     ClientSessionActionExchange
	Response    ProtocolStatementResponse
	Diagnostics DiagnosticSet
}

// PrepareClientTransactionAction previews an adapter-owned transaction action.
func (s PlanningService) PrepareClientTransactionAction(connection ConnectionContext, registry SessionRegistry, action SessionAction, options ClientTransactionOptions) ClientTransactionExchange {
	exchange := ClientTransactionExchange{
		Connection:  cloneConnectionContext(connection),
		Action:      action,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if !action.Transactional() {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "transaction exchange requires a transaction action"),
		})
		return exchange
	}

	statement := StatementResult{
		Status:         transactionActionStatus(action),
		SessionActions: []SessionAction{action},
	}
	response := statement.ProtocolStatementResponse(connection.Protocol)
	exchange.Response = cloneProtocolStatementResponse(response)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, response.Diagnostics)
	if exchange.Diagnostics.BlocksNative() {
		return exchange
	}

	session := s.PrepareClientSessionActions(connection, registry, []SessionAction{action}, ClientSessionActionOptions{Apply: options.ApplySession})
	exchange.Session = session
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, session.Diagnostics)
	return exchange
}

// PrepareClientBeginTransaction previews a begin transaction request.
func (s PlanningService) PrepareClientBeginTransaction(connection ConnectionContext, registry SessionRegistry, options ClientTransactionOptions) ClientTransactionExchange {
	return s.PrepareClientTransactionAction(connection, registry, BeginTransactionAction(), options)
}

// PrepareClientCommitTransaction previews a commit transaction request.
func (s PlanningService) PrepareClientCommitTransaction(connection ConnectionContext, registry SessionRegistry, options ClientTransactionOptions) ClientTransactionExchange {
	return s.PrepareClientTransactionAction(connection, registry, CommitTransactionAction(), options)
}

// PrepareClientRollbackTransaction previews a rollback transaction request.
func (s PlanningService) PrepareClientRollbackTransaction(connection ConnectionContext, registry SessionRegistry, options ClientTransactionOptions) ClientTransactionExchange {
	return s.PrepareClientTransactionAction(connection, registry, RollbackTransactionAction(), options)
}

// Supported reports whether transaction metadata can proceed.
func (e ClientTransactionExchange) Supported() bool {
	return e.Connection.Supported() && e.Action.Transactional() && e.Response.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts transaction diagnostics into protocol-facing errors.
func (e ClientTransactionExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking transaction error, if any.
func (e ClientTransactionExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func transactionActionStatus(action SessionAction) string {
	switch action.Kind {
	case SessionActionBeginTransaction:
		return "Transaction started"
	case SessionActionCommitTransaction:
		return "Transaction committed"
	case SessionActionRollbackTransaction:
		return "Transaction rolled back"
	default:
		return "Transaction action"
	}
}
