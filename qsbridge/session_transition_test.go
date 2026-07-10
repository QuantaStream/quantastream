package qsbridge

import "testing"

func TestSessionContextPreviewSessionTransitionAppliesSupportedActions(t *testing.T) {
	session := SessionContext{
		CurrentSchema: "quanta",
		TimeZone:      "America/Costa_Rica",
		SQLModes:      []SQLMode{"ANSI_QUOTES"},
		Variables: map[string]string{
			"autocommit": "1",
		},
	}

	transition := session.PreviewSessionTransition([]SessionAction{
		{Kind: SessionActionUseSchema, Value: "analytics"},
		{Kind: SessionActionSetVariable, Name: "autocommit", Value: "0"},
		{Kind: SessionActionSetVariable, Name: "sql_select_limit", Value: "100"},
		{Kind: SessionActionSetSQLMode, Value: "STRICT_TRANS_TABLES,NO_ZERO_DATE"},
		{Kind: SessionActionSetTimeZone, Value: "UTC"},
	})
	if !transition.Supported() {
		t.Fatalf("transition diagnostics = %#v, want supported", transition.Diagnostics)
	}
	if transition.Before.CurrentSchema != "quanta" || transition.After.CurrentSchema != "analytics" {
		t.Fatalf("schemas before/after = %q/%q, want quanta/analytics", transition.Before.CurrentSchema, transition.After.CurrentSchema)
	}
	if transition.After.TimeZone != "UTC" {
		t.Fatalf("time zone = %q, want UTC", transition.After.TimeZone)
	}
	if len(transition.After.SQLModes) != 2 || transition.After.SQLModes[0] != "STRICT_TRANS_TABLES" || transition.After.SQLModes[1] != "NO_ZERO_DATE" {
		t.Fatalf("sql modes = %#v, want parsed replacement modes", transition.After.SQLModes)
	}
	if transition.After.Variables["autocommit"] != "0" || transition.After.Variables["sql_select_limit"] != "100" {
		t.Fatalf("variables = %#v, want updated session variables", transition.After.Variables)
	}
}

func TestSessionContextPreviewSessionTransitionDoesNotMutateInputs(t *testing.T) {
	session := SessionContext{
		Roles:    []RoleName{"reader"},
		SQLModes: []SQLMode{"ANSI_QUOTES"},
		Variables: map[string]string{
			"autocommit": "1",
		},
	}
	actions := []SessionAction{{
		Kind:  SessionActionSetVariable,
		Name:  "autocommit",
		Value: "0",
	}}

	transition := session.PreviewSessionTransition(actions)
	transition.Before.Roles[0] = "mutated"
	transition.After.SQLModes[0] = "mutated"
	transition.Actions[0].Value = "mutated"
	transition.After.Variables["autocommit"] = "mutated"

	if session.Roles[0] != "reader" || session.SQLModes[0] != "ANSI_QUOTES" || session.Variables["autocommit"] != "1" {
		t.Fatalf("session was mutated: %#v", session)
	}
	if actions[0].Value != "0" {
		t.Fatalf("actions were mutated: %#v", actions)
	}
}

func TestExecutionRequestsExposeSessionTransition(t *testing.T) {
	prepared := PreparedPlan{
		Supported: true,
		Session:   SessionContext{CurrentSchema: "quanta"},
		Statement: StatementResult{
			SessionActions: []SessionAction{{Kind: SessionActionUseSchema, Value: "analytics"}},
		},
	}
	execution := prepared.ExecutionRequest(ExecutionOptions{})
	batch := prepared.BatchExecutionRequest(ExecutionOptions{}, ParameterValues())

	if execution.SessionTransition().After.CurrentSchema != "analytics" {
		t.Fatalf("execution transition = %#v, want analytics", execution.SessionTransition())
	}
	if batch.SessionTransition().After.CurrentSchema != "analytics" {
		t.Fatalf("batch transition = %#v, want analytics", batch.SessionTransition())
	}
}

func TestExecutionResultSessionTransitionUsesAdapterProvidedBeforeSession(t *testing.T) {
	result := ExecutionResult{
		SessionActions: []SessionAction{{Kind: SessionActionSetTimeZone, Value: "UTC"}},
	}
	transition := result.SessionTransition(SessionContext{TimeZone: "America/Costa_Rica"})
	if transition.Before.TimeZone != "America/Costa_Rica" || transition.After.TimeZone != "UTC" {
		t.Fatalf("transition = %#v, want timezone preview", transition)
	}
}
