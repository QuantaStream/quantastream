package qsbridge

import "testing"

func TestBindSessionCarriesSessionActionsAsStatementResult(t *testing.T) {
	query, diagnostics := BindSession(nil, UnboundSession{
		Actions: []SessionAction{{
			Kind:  SessionActionUseSchema,
			Value: "analytics",
		}},
		Result: ResultShape{
			Statement: StatementResult{Status: "Database changed"},
		},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindSession || query.Result.Kind != ResultStatement {
		t.Fatalf("query kind/result = %q/%q, want session/statement", query.Kind, query.Result.Kind)
	}
	actions := query.SessionActions()
	if len(actions) != 1 || actions[0].Kind != SessionActionUseSchema || actions[0].Value != "analytics" {
		t.Fatalf("actions = %#v, want use schema analytics", actions)
	}
}

func TestUnboundStatementBindsSessionStatement(t *testing.T) {
	statement := UnboundStatement{
		Kind: QueryKindSession,
		Session: UnboundSession{
			Actions: []SessionAction{{
				Kind:  SessionActionSetTimeZone,
				Value: "UTC",
			}},
		},
	}

	query, diagnostics := statement.Bind(nil)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	actions := query.SessionActions()
	if len(actions) != 1 || actions[0].Kind != SessionActionSetTimeZone || actions[0].Value != "UTC" {
		t.Fatalf("actions = %#v, want set time zone UTC", actions)
	}
}

func TestSessionActionsAreCopiedThroughPlanPreparedAndCache(t *testing.T) {
	result := PlanResult{Query: QueryIR{
		Kind: QueryKindSession,
		Result: ResultShape{
			Kind: ResultStatement,
			Statement: StatementResult{
				SessionActions: []SessionAction{{
					Kind:  SessionActionSetVariable,
					Name:  "autocommit",
					Value: "1",
				}},
			},
		},
	}}

	prepared := result.PreparedPlan()
	actions := prepared.SessionActions()
	actions[0].Value = "0"
	if prepared.SessionActions()[0].Value != "1" {
		t.Fatalf("prepared session actions were mutated")
	}

	cache := NewMemoryPreparedPlanCache()
	cache.Put(prepared)
	cached, ok := cache.Get(prepared.CacheKey())
	if !ok {
		t.Fatalf("expected cached prepared plan")
	}
	cached.Statement.SessionActions[0].Value = "0"
	cachedAgain, ok := cache.Get(prepared.CacheKey())
	if !ok {
		t.Fatalf("expected cached prepared plan after mutation")
	}
	if cachedAgain.Statement.SessionActions[0].Value != "1" {
		t.Fatalf("cached session actions were mutated: %#v", cachedAgain.Statement.SessionActions)
	}
}
