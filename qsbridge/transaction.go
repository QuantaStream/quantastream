package qsbridge

// BeginTransactionAction creates a metadata-only begin-transaction action.
func BeginTransactionAction() SessionAction {
	return SessionAction{Kind: SessionActionBeginTransaction}
}

// CommitTransactionAction creates a metadata-only commit action.
func CommitTransactionAction() SessionAction {
	return SessionAction{Kind: SessionActionCommitTransaction}
}

// RollbackTransactionAction creates a metadata-only rollback action.
func RollbackTransactionAction() SessionAction {
	return SessionAction{Kind: SessionActionRollbackTransaction}
}

// Transactional reports whether action affects transaction state.
func (a SessionAction) Transactional() bool {
	switch a.Kind {
	case SessionActionBeginTransaction, SessionActionCommitTransaction, SessionActionRollbackTransaction:
		return true
	default:
		return false
	}
}

// TransactionActions returns transaction-related actions requested by this query.
func (q QueryIR) TransactionActions() []SessionAction {
	return filterTransactionActions(q.SessionActions())
}

// TransactionActions returns transaction-related actions requested by this planning result.
func (r PlanResult) TransactionActions() []SessionAction {
	return r.Query.TransactionActions()
}

// TransactionActions returns transaction-related actions requested by this prepared plan.
func (p PreparedPlan) TransactionActions() []SessionAction {
	return filterTransactionActions(p.SessionActions())
}

func filterTransactionActions(actions []SessionAction) []SessionAction {
	filtered := make([]SessionAction, 0, len(actions))
	for _, action := range actions {
		if action.Transactional() {
			filtered = append(filtered, action)
		}
	}
	return filtered
}
