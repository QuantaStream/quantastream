package qsbridge

import "testing"

func TestMemorySessionRegistryStoresAuthenticatedSession(t *testing.T) {
	registry := NewMemorySessionRegistry()
	session := AuthenticationRequest{
		User:          "moli",
		DefaultSchema: "quanta",
	}.Allow(AuthenticationPrincipal{
		Roles: []RoleName{"reader"},
	}).SessionContext("session-1")

	if !registry.Put(session) {
		t.Fatalf("expected session put")
	}
	stored, ok := registry.Get("session-1")
	if !ok {
		t.Fatalf("expected stored session")
	}
	if stored.User != "moli" || stored.CurrentSchema != "quanta" || !stored.HasRole("reader") {
		t.Fatalf("stored = %#v, want authenticated session metadata", stored)
	}
}

func TestMemorySessionRegistryAppliesSessionTransition(t *testing.T) {
	registry := NewMemorySessionRegistry()
	session := SessionContext{
		ID:            "session-1",
		CurrentSchema: "quanta",
		Variables:     map[string]string{"autocommit": "1"},
	}
	if !registry.Put(session) {
		t.Fatalf("expected session put")
	}

	transition := session.PreviewSessionTransition([]SessionAction{
		{Kind: SessionActionUseSchema, Value: "analytics"},
		{Kind: SessionActionSetVariable, Name: "autocommit", Value: "0"},
	})
	updated, ok := registry.Apply(transition)
	if !ok {
		t.Fatalf("expected transition apply")
	}
	if updated.CurrentSchema != "analytics" || updated.Variables["autocommit"] != "0" {
		t.Fatalf("updated = %#v, want applied transition", updated)
	}
	stored, ok := registry.Get("session-1")
	if !ok || stored.CurrentSchema != "analytics" {
		t.Fatalf("stored = %#v ok=%v, want updated session", stored, ok)
	}
}

func TestMemorySessionRegistryRejectsUnsupportedInputs(t *testing.T) {
	registry := NewMemorySessionRegistry()
	if registry.Put(SessionContext{}) {
		t.Fatalf("session without id should not store")
	}
	if _, ok := registry.Get(""); ok {
		t.Fatalf("empty id should not lookup")
	}
	if _, ok := registry.Apply(SessionTransition{After: SessionContext{ID: "session-1"}, Diagnostics: DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "bad transition"),
	}}); ok {
		t.Fatalf("unsupported transition should not apply")
	}
	if _, ok := registry.Apply(SessionContext{}.PreviewSessionTransition(nil)); ok {
		t.Fatalf("transition without after id should not apply")
	}
}

func TestMemorySessionRegistryCopiesMutableSessionMetadata(t *testing.T) {
	registry := NewMemorySessionRegistry()
	session := SessionContext{
		ID:       "session-1",
		Roles:    []RoleName{"reader"},
		SQLModes: []SQLMode{"ANSI_QUOTES"},
		Variables: map[string]string{
			"autocommit": "1",
		},
	}
	if !registry.Put(session) {
		t.Fatalf("expected session put")
	}
	session.Roles[0] = "mutated"
	session.SQLModes[0] = "mutated"
	session.Variables["autocommit"] = "mutated"

	stored, ok := registry.Get("session-1")
	if !ok {
		t.Fatalf("expected stored session")
	}
	stored.Roles[0] = "mutated-again"
	stored.Variables["autocommit"] = "mutated-again"
	second, ok := registry.Get("session-1")
	if !ok {
		t.Fatalf("expected second stored session")
	}
	if second.Roles[0] != "reader" || second.SQLModes[0] != "ANSI_QUOTES" || second.Variables["autocommit"] != "1" {
		t.Fatalf("registry leaked mutable session: %#v", second)
	}
}

func TestMemorySessionRegistryRemoveAndClear(t *testing.T) {
	registry := NewMemorySessionRegistry()
	_ = registry.Put(SessionContext{ID: "session-1"})
	if !registry.Remove("session-1") {
		t.Fatalf("expected remove")
	}
	if _, ok := registry.Get("session-1"); ok {
		t.Fatalf("expected removed session to be missing")
	}

	_ = registry.Put(SessionContext{ID: "session-2"})
	registry.Clear()
	if _, ok := registry.Get("session-2"); ok {
		t.Fatalf("expected clear to remove session")
	}
}
