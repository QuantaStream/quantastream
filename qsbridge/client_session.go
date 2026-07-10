package qsbridge

// ClientSessionActionOptions control how adapters handle requested session actions.
type ClientSessionActionOptions struct {
	Apply bool
}

// ClientSessionActionExchange is the metadata response for requested session mutations.
type ClientSessionActionExchange struct {
	Connection  ConnectionContext
	Transition  SessionTransition
	Applied     bool
	Diagnostics DiagnosticSet
}

// PrepareClientSessionActions previews and optionally applies session action metadata.
func (s PlanningService) PrepareClientSessionActions(connection ConnectionContext, registry SessionRegistry, actions []SessionAction, options ClientSessionActionOptions) ClientSessionActionExchange {
	_ = s
	transition := connection.Session.PreviewSessionTransition(actions)
	exchange := ClientSessionActionExchange{
		Connection:  cloneConnectionContext(connection),
		Transition:  cloneSessionTransition(transition),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, transition.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, validateClientSessionActions(connection.Protocol, actions))
	if exchange.Diagnostics.BlocksNative() || !options.Apply {
		return exchange
	}
	if registry == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "session registry is not configured"),
		})
		return exchange
	}
	applied, ok := registry.Apply(transition)
	if !ok {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "session transition could not be applied"),
		})
		return exchange
	}
	exchange.Transition.After = applied.Clone()
	exchange.Applied = true
	return exchange
}

// PrepareClientResultSessionActions previews and optionally applies result session metadata.
func (s PlanningService) PrepareClientResultSessionActions(connection ConnectionContext, registry SessionRegistry, result ExecutionResult, options ClientSessionActionOptions) ClientSessionActionExchange {
	exchange := s.PrepareClientSessionActions(connection, registry, result.SessionActions, options)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, result.Diagnostics)
	return exchange
}

// Supported reports whether session action metadata can proceed.
func (e ClientSessionActionExchange) Supported() bool {
	return e.Connection.Supported() && e.Transition.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts session action diagnostics into protocol-facing errors.
func (e ClientSessionActionExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking session action error, if any.
func (e ClientSessionActionExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func validateClientSessionActions(profile ProtocolProfile, actions []SessionAction) DiagnosticSet {
	if len(actions) == 0 || profile.Supports(ProtocolCapabilitySessionActions) {
		return nil
	}
	return DiagnosticSet{protocolCapabilityDiagnostic("session actions are not supported by protocol profile")}
}

func cloneSessionTransition(transition SessionTransition) SessionTransition {
	return SessionTransition{
		Before:      transition.Before.Clone(),
		After:       transition.After.Clone(),
		Actions:     cloneSessionActions(transition.Actions),
		Diagnostics: cloneDiagnosticSet(transition.Diagnostics),
	}
}
