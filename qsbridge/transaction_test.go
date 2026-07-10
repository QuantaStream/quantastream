package qsbridge

import "testing"

func TestTransactionActionHelpersAreSessionActions(t *testing.T) {
	actions := []SessionAction{
		BeginTransactionAction(),
		CommitTransactionAction(),
		RollbackTransactionAction(),
	}
	for _, action := range actions {
		if !action.Transactional() {
			t.Fatalf("action = %#v, want transactional", action)
		}
	}
	if (SessionAction{Kind: SessionActionSetVariable}).Transactional() {
		t.Fatalf("set variable should not be transactional")
	}
}

func TestBindSessionCarriesTransactionActions(t *testing.T) {
	query, diagnostics := BindSession(nil, UnboundSession{
		Actions: []SessionAction{
			BeginTransactionAction(),
			{Kind: SessionActionSetVariable, Name: "autocommit", Value: "0"},
		},
		Result: ResultShape{
			Statement: StatementResult{Status: "Transaction started"},
		},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	actions := query.SessionActions()
	if len(actions) != 2 {
		t.Fatalf("actions = %#v, want all session actions", actions)
	}
	transactionActions := query.TransactionActions()
	if len(transactionActions) != 1 || transactionActions[0].Kind != SessionActionBeginTransaction {
		t.Fatalf("transaction actions = %#v, want begin", transactionActions)
	}
}

func TestTransactionActionsFlowThroughPreparedPlan(t *testing.T) {
	result := PlanResult{Query: QueryIR{
		Kind: QueryKindSession,
		Result: ResultShape{
			Kind: ResultStatement,
			Statement: StatementResult{
				SessionActions: []SessionAction{CommitTransactionAction()},
			},
		},
	}}

	prepared := result.PreparedPlan()
	actions := prepared.TransactionActions()
	if len(actions) != 1 || actions[0].Kind != SessionActionCommitTransaction {
		t.Fatalf("transaction actions = %#v, want commit", actions)
	}
	actions[0].Kind = SessionActionRollbackTransaction
	if prepared.TransactionActions()[0].Kind != SessionActionCommitTransaction {
		t.Fatalf("transaction actions leaked mutation")
	}
}
