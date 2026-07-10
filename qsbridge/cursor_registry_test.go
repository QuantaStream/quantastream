package qsbridge

import "testing"

func TestMemoryCursorRegistryOpensAndGetsCursor(t *testing.T) {
	registry := NewMemoryCursorRegistry()
	cursor := CursorDescriptor{
		ID:        "cursor-1",
		RequestID: "req-1",
		Mode:      CursorForwardOnly,
		State:     CursorStateOpen,
		BatchSize: 10,
	}

	opened, ok := registry.Open(cursor)
	if !ok {
		t.Fatalf("expected cursor to open")
	}
	if opened.ID != "cursor-1" || opened.BatchSize != 10 {
		t.Fatalf("opened = %#v, want copied cursor metadata", opened)
	}
	stored, ok := registry.Get("cursor-1")
	if !ok || stored.RequestID != "req-1" {
		t.Fatalf("stored = %#v ok=%v, want cursor metadata", stored, ok)
	}
}

func TestMemoryCursorRegistryRejectsInvalidCursorOpen(t *testing.T) {
	registry := NewMemoryCursorRegistry()
	if _, ok := registry.Open(CursorDescriptor{State: CursorStateOpen}); ok {
		t.Fatalf("cursor without id should not open")
	}
	if _, ok := registry.Open(CursorDescriptor{ID: "closed", State: CursorStateClosed}); ok {
		t.Fatalf("closed cursor should not open")
	}
}

func TestMemoryCursorRegistryAdvanceTracksPositionAndExhaustion(t *testing.T) {
	registry := NewMemoryCursorRegistry()
	_, _ = registry.Open(CursorDescriptor{ID: "cursor-1", State: CursorStateOpen})

	cursor, ok := registry.Advance("cursor-1", 3, false)
	if !ok || cursor.Position != 3 || cursor.State != CursorStateOpen {
		t.Fatalf("cursor = %#v ok=%v, want position 3/open", cursor, ok)
	}
	cursor, ok = registry.Advance("cursor-1", 2, true)
	if !ok || cursor.Position != 5 || cursor.State != CursorStateExhausted {
		t.Fatalf("cursor = %#v ok=%v, want position 5/exhausted", cursor, ok)
	}
}

func TestMemoryCursorRegistryCloseRemoveAndClear(t *testing.T) {
	registry := NewMemoryCursorRegistry()
	_, _ = registry.Open(CursorDescriptor{ID: "cursor-1", State: CursorStateOpen})

	closed, ok := registry.Close("cursor-1")
	if !ok || closed.State != CursorStateClosed {
		t.Fatalf("closed = %#v ok=%v, want closed cursor", closed, ok)
	}
	if _, ok := registry.Advance("cursor-1", 1, false); ok {
		t.Fatalf("closed cursor should not advance")
	}
	if !registry.Remove("cursor-1") {
		t.Fatalf("expected cursor remove")
	}
	if _, ok := registry.Get("cursor-1"); ok {
		t.Fatalf("expected removed cursor to be missing")
	}

	_, _ = registry.Open(CursorDescriptor{ID: "cursor-2", State: CursorStateOpen})
	registry.Clear()
	if _, ok := registry.Get("cursor-2"); ok {
		t.Fatalf("expected clear to remove cursor")
	}
}

func TestMemoryCursorRegistryOpensResultCursorDescriptor(t *testing.T) {
	registry := NewMemoryCursorRegistry()
	request := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "req-1", Cursor: CursorForwardOnly},
		Result:  ResultShape{Kind: ResultQuery},
	}
	result := request.EmptyResult()

	cursor, ok := registry.Open(result.Cursor)
	if !ok {
		t.Fatalf("expected result cursor to open")
	}
	if cursor.ID != "req-1" || cursor.RequestID != "req-1" {
		t.Fatalf("cursor = %#v, want result cursor ids", cursor)
	}
}
