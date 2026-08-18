package qsbridge

// BindSession binds a parser-neutral session-affecting statement.
//
// The returned QueryIR carries the requested session actions as OK-style result
// metadata. qsbridge does not apply the actions; protocol/session owners decide
// whether the authenticated session may change and then mutate their own state.
func BindSession(context *BindContext, sessionStmt UnboundSession) (QueryIR, DiagnosticSet) {
	diagnostics := make(DiagnosticSet, 0)
	if context != nil && sessionStmt.ValidateCatalog {
		for _, action := range sessionStmt.Actions {
			if action.Kind == SessionActionUseSchema {
				diagnostics = append(diagnostics, validateCatalogSchema(context, action.Value)...)
			}
		}
	}
	result := sessionStmt.Result
	if result.Kind == "" {
		result.Kind = ResultStatement
	}
	result.Statement.SessionActions = CloneSessionActions(sessionStmt.Actions)
	return QueryIR{
		Kind:     QueryKindSession,
		Result:   result,
		Blockers: append([]NativeBlocker(nil), sessionStmt.Blockers...),
	}, diagnostics
}

// SessionActions returns session changes requested by this statement.
func (q QueryIR) SessionActions() []SessionAction {
	return cloneSessionActions(q.Result.Statement.SessionActions)
}

// SessionActions returns session changes requested by this planning result.
func (r PlanResult) SessionActions() []SessionAction {
	return r.Query.SessionActions()
}

// SessionActions returns session changes requested by this prepared plan.
func (p PreparedPlan) SessionActions() []SessionAction {
	return cloneSessionActions(p.Statement.SessionActions)
}

func cloneSessionActions(actions []SessionAction) []SessionAction {
	return CloneSessionActions(actions)
}

// CloneSessionActions returns session actions with independent nested metadata.
func CloneSessionActions(actions []SessionAction) []SessionAction {
	if len(actions) == 0 {
		return nil
	}
	cloned := make([]SessionAction, 0, len(actions))
	for _, action := range actions {
		action.Table = cloneTableDefinition(action.Table)
		action.Rows = cloneTemporaryTableRows(action.Rows)
		cloned = append(cloned, action)
	}
	return cloned
}
