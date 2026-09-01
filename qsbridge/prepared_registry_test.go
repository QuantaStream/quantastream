package qsbridge

import "testing"

func TestMemoryPreparedStatementRegistryAllocatesHandleAndStoresPlan(t *testing.T) {
	registry := NewMemoryPreparedStatementRegistry()
	prepared := testPreparedSelectPlan(t).PreparedPlan()

	description := registry.Register(prepared)
	if description.Handle.ID == 0 {
		t.Fatalf("description handle = %#v, want allocated id", description.Handle)
	}
	stored, ok := registry.Get(description.Handle)
	if !ok {
		t.Fatalf("expected registered prepared plan")
	}
	if stored.Handle.ID != description.Handle.ID || stored.SQL != prepared.SQL {
		t.Fatalf("stored = %#v, want registered plan with handle", stored)
	}
}

func TestMemoryPreparedStatementRegistrySupportsAdapterProvidedName(t *testing.T) {
	registry := NewMemoryPreparedStatementRegistry()
	prepared := testPreparedSelectPlan(t).PreparedPlan().WithHandle(PreparedStatementHandle{
		Name: "by_name",
	})

	description := registry.Register(prepared)
	if description.Handle.ID == 0 || description.Handle.Name != "by_name" {
		t.Fatalf("description handle = %#v, want allocated id and provided name", description.Handle)
	}
	byName, ok := registry.Get(PreparedStatementHandle{Name: "by_name"})
	if !ok {
		t.Fatalf("expected name lookup")
	}
	if byName.Handle.ID != description.Handle.ID {
		t.Fatalf("by name handle = %#v, want id %d", byName.Handle, description.Handle.ID)
	}
}

func TestMemoryPreparedStatementRegistryHonorsAdapterProvidedID(t *testing.T) {
	registry := NewMemoryPreparedStatementRegistry()
	prepared := testPreparedSelectPlan(t).PreparedPlan().WithHandle(PreparedStatementHandle{
		ID:   42,
		Name: "explicit",
	})

	description := registry.Register(prepared)
	if description.Handle.ID != 42 {
		t.Fatalf("handle id = %d, want adapter-provided 42", description.Handle.ID)
	}
	stored, ok := registry.Get(PreparedStatementHandle{ID: 42})
	if !ok || stored.Handle.Name != "explicit" {
		t.Fatalf("stored = %#v ok=%v, want explicit handle", stored, ok)
	}
}

func TestMemoryPreparedStatementRegistryRejectsNewStatementsAtLimit(t *testing.T) {
	registry := NewMemoryPreparedStatementRegistryWithLimit(2)

	first := registry.Register(testPreparedSelectPlan(t).PreparedPlan().WithHandle(PreparedStatementHandle{Name: "first"}))
	second := registry.Register(testPreparedSelectPlan(t).PreparedPlan().WithHandle(PreparedStatementHandle{Name: "second"}))
	rejected := registry.Register(testPreparedSelectPlan(t).PreparedPlan().WithHandle(PreparedStatementHandle{Name: "third"}))

	if rejected.SupportedForPrepare() {
		t.Fatalf("third prepared statement should be rejected at limit")
	}
	if codes := rejected.Diagnostics.Codes(); len(codes) != 1 || codes[0] != DiagnosticInvalidExecutionOption {
		t.Fatalf("diagnostics = %#v, want invalid execution option", codes)
	}
	for _, handle := range []PreparedStatementHandle{first.Handle, second.Handle} {
		if _, ok := registry.Get(handle); !ok {
			t.Fatalf("expected prepared statement %v to remain registered", handle)
		}
	}
	if plans := registry.List(); len(plans) != 2 {
		t.Fatalf("plans = %#v, want two live prepared statements", plans)
	}
}

func TestMemoryPreparedStatementRegistryReplacingIDClearsOldName(t *testing.T) {
	registry := NewMemoryPreparedStatementRegistryWithLimit(2)
	first := registry.Register(testPreparedSelectPlan(t).PreparedPlan().WithHandle(PreparedStatementHandle{ID: 42, Name: "old_name"}))
	second := registry.Register(testPreparedSelectPlan(t).PreparedPlan().WithHandle(PreparedStatementHandle{ID: 42, Name: "new_name"}))

	if first.Handle.ID != second.Handle.ID {
		t.Fatalf("handles = %#v %#v, want same id", first.Handle, second.Handle)
	}
	if _, ok := registry.Get(PreparedStatementHandle{Name: "old_name"}); ok {
		t.Fatalf("expected replaced prepared statement name to be removed")
	}
	if plan, ok := registry.Get(PreparedStatementHandle{Name: "new_name"}); !ok || plan.Handle.ID != 42 {
		t.Fatalf("new name lookup = %#v ok=%v, want id 42", plan.Handle, ok)
	}
}

func TestMemoryPreparedStatementRegistryCopiesMutablePreparedPlans(t *testing.T) {
	registry := NewMemoryPreparedStatementRegistry()
	prepared := testPreparedSelectPlan(t).PreparedPlan()
	originalType := prepared.Parameters[0].Type
	description := registry.Register(prepared)

	stored, ok := registry.Get(description.Handle)
	if !ok {
		t.Fatalf("expected registered prepared plan")
	}
	stored.Parameters[0].Type = DataTypeString
	second, ok := registry.Get(description.Handle)
	if !ok {
		t.Fatalf("expected second registered prepared plan")
	}
	if second.Parameters[0].Type != originalType {
		t.Fatalf("registry leaked mutable prepared plan: %#v", second.Parameters)
	}
}

func TestMemoryPreparedStatementRegistryCloseAndClear(t *testing.T) {
	registry := NewMemoryPreparedStatementRegistry()
	description := registry.Register(testPreparedSelectPlan(t).PreparedPlan().WithHandle(PreparedStatementHandle{Name: "close_me"}))

	if !registry.Close(PreparedPlan{}.WithHandle(description.Handle).CloseRequest()) {
		t.Fatalf("expected close by id to remove statement")
	}
	if _, ok := registry.Get(description.Handle); ok {
		t.Fatalf("expected statement to be closed")
	}
	description = registry.Register(testPreparedSelectPlan(t).PreparedPlan().WithHandle(PreparedStatementHandle{Name: "clear_me"}))
	if _, ok := registry.Get(PreparedStatementHandle{Name: "clear_me"}); !ok {
		t.Fatalf("expected name lookup before clear")
	}
	registry.Clear()
	if _, ok := registry.Get(description.Handle); ok {
		t.Fatalf("expected clear to remove statement")
	}
	if registry.Close(PreparedStatementCloseRequest{}) {
		t.Fatalf("unsupported close request should not remove anything")
	}
}
