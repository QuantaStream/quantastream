package qsbridge

import "testing"

func TestPlanningServiceOpenClientResultCursorStoresMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryCursorRegistry()
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityForwardOnlyCursor)
	result := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "req-1", Cursor: CursorForwardOnly, BatchSize: 10},
		Result:  ResultShape{Kind: ResultQuery},
	}.EmptyResult()

	opened := service.OpenClientResultCursor(connection, registry, result)
	if !opened.Supported() || !opened.Opened {
		t.Fatalf("opened = %#v, want supported cursor open", opened)
	}
	if opened.Cursor.ID != "req-1" || opened.Cursor.BatchSize != 10 {
		t.Fatalf("cursor = %#v, want request-derived cursor", opened.Cursor)
	}
	if stored, ok := registry.Get("req-1"); !ok || stored.State != CursorStateOpen {
		t.Fatalf("stored = %#v ok=%v, want open cursor", stored, ok)
	}
}

func TestPlanningServiceOpenClientResultCursorRejectsUnsupportedProtocol(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryCursorRegistry()
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	result := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "req-1", Cursor: CursorForwardOnly},
		Result:  ResultShape{Kind: ResultQuery},
	}.EmptyResult()

	opened := service.OpenClientResultCursor(connection, registry, result)
	if opened.Supported() || opened.Opened {
		t.Fatalf("opened = %#v, want unsupported cursor open", opened)
	}
	if !containsDiagnosticCode(opened.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", opened.Diagnostics)
	}
	if _, ok := registry.Get("req-1"); ok {
		t.Fatalf("unsupported protocol should not store cursor")
	}
}

func TestPlanningServiceAdvanceClientCursorTracksProgress(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryCursorRegistry()
	connection := clientStatementConnection()
	_, _ = registry.Open(CursorDescriptor{
		ID:        "cursor-1",
		RequestID: "req-1",
		Mode:      CursorForwardOnly,
		State:     CursorStateOpen,
	})

	advanced := service.AdvanceClientCursor(connection, registry, "cursor-1", 3, false)
	if !advanced.Supported() || !advanced.Advanced {
		t.Fatalf("advanced = %#v, want supported cursor advance", advanced)
	}
	if advanced.Cursor.Position != 3 || advanced.Cursor.State != CursorStateOpen {
		t.Fatalf("cursor = %#v, want position 3/open", advanced.Cursor)
	}
	advanced = service.AdvanceClientCursor(connection, registry, "cursor-1", 2, true)
	if !advanced.Supported() || advanced.Cursor.Position != 5 || advanced.Cursor.State != CursorStateExhausted {
		t.Fatalf("advanced = %#v, want position 5/exhausted", advanced)
	}
}

func TestPlanningServicePrepareClientCursorFetchUsesBatchSizeWithoutAdvancing(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryCursorRegistry()
	connection := clientStatementConnection()
	_, _ = registry.Open(CursorDescriptor{
		ID:        "cursor-1",
		RequestID: "req-1",
		Mode:      CursorForwardOnly,
		State:     CursorStateOpen,
		BatchSize: 25,
	})

	fetch := service.PrepareClientCursorFetch(connection, registry, "cursor-1", 0)
	if !fetch.Supported() {
		t.Fatalf("fetch = %#v, want supported cursor fetch", fetch)
	}
	if fetch.RequestedRows != 25 || fetch.Final {
		t.Fatalf("fetch = %#v, want batch-size fetch without final marker", fetch)
	}
	stored, ok := registry.Get("cursor-1")
	if !ok || stored.Position != 0 {
		t.Fatalf("stored = %#v ok=%v, want fetch validation not to advance cursor", stored, ok)
	}
}

func TestPlanningServicePrepareClientCursorFetchClipsAtMaxRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryCursorRegistry()
	connection := clientStatementConnection()
	_, _ = registry.Open(CursorDescriptor{
		ID:        "cursor-1",
		RequestID: "req-1",
		Mode:      CursorForwardOnly,
		State:     CursorStateOpen,
		MaxRows:   10,
		Position:  7,
	})

	fetch := service.PrepareClientCursorFetch(connection, registry, "cursor-1", 8)
	if !fetch.Supported() {
		t.Fatalf("fetch = %#v, want supported clipped fetch", fetch)
	}
	if fetch.RequestedRows != 3 || !fetch.Final {
		t.Fatalf("fetch = %#v, want clipped final fetch", fetch)
	}
}

func TestPlanningServicePrepareClientCursorFetchReportsInvalidRequests(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryCursorRegistry()
	connection := clientStatementConnection()
	_, _ = registry.Open(CursorDescriptor{ID: "closed", Mode: CursorForwardOnly, State: CursorStateClosed, BatchSize: 10})
	_, _ = registry.Open(CursorDescriptor{ID: "scroll", Mode: CursorScrollable, State: CursorStateOpen, BatchSize: 10})
	_, _ = registry.Open(CursorDescriptor{ID: "count", Mode: CursorForwardOnly, State: CursorStateOpen})

	for _, id := range []CursorID{"closed", "scroll", "count", "missing"} {
		fetch := service.PrepareClientCursorFetch(connection, registry, id, 0)
		if fetch.Supported() {
			t.Fatalf("fetch %s = %#v, want unsupported", id, fetch)
		}
		if !containsDiagnosticCode(fetch.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
			t.Fatalf("diagnostics for %s = %#v, want invalid execution option", id, fetch.Diagnostics)
		}
	}
}

func TestPlanningServiceCloseClientCursorMarksCursorClosed(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryCursorRegistry()
	connection := clientStatementConnection()
	_, _ = registry.Open(CursorDescriptor{ID: "cursor-1", Mode: CursorForwardOnly, State: CursorStateOpen})

	closed := service.CloseClientCursor(connection, registry, "cursor-1")
	if !closed.Supported() || !closed.Closed {
		t.Fatalf("closed = %#v, want supported cursor close", closed)
	}
	if closed.Cursor.State != CursorStateClosed {
		t.Fatalf("cursor = %#v, want closed state", closed.Cursor)
	}
	if advanced := service.AdvanceClientCursor(connection, registry, "cursor-1", 1, false); advanced.Supported() {
		t.Fatalf("closed cursor should not advance: %#v", advanced)
	}
}

func TestPlanningServiceClientCursorExchangesReportMissingRegistryAndID(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityForwardOnlyCursor)
	result := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "req-1", Cursor: CursorForwardOnly},
		Result:  ResultShape{Kind: ResultQuery},
	}.EmptyResult()

	opened := service.OpenClientResultCursor(connection, nil, result)
	if opened.Supported() || !containsDiagnosticCode(opened.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("opened = %#v, want missing registry diagnostic", opened)
	}
	advanced := service.AdvanceClientCursor(connection, NewMemoryCursorRegistry(), "", 1, false)
	if advanced.Supported() || !containsDiagnosticCode(advanced.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("advanced = %#v, want missing cursor id diagnostic", advanced)
	}
	closed := service.CloseClientCursor(connection, NewMemoryCursorRegistry(), "")
	if closed.Supported() || !containsDiagnosticCode(closed.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("closed = %#v, want missing cursor id diagnostic", closed)
	}
}

func TestPlanningServiceClientCursorExchangesCopyConnectionMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryCursorRegistry()
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityForwardOnlyCursor)
	result := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "req-1", Cursor: CursorForwardOnly},
		Result:  ResultShape{Kind: ResultQuery},
	}.EmptyResult()

	opened := service.OpenClientResultCursor(connection, registry, result)
	opened.Connection.Attributes["client"] = "mutated"
	if connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", connection.Attributes)
	}
}
