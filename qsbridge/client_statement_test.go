package qsbridge

import "testing"

func TestClientStatementBundleBuildsPlanRequests(t *testing.T) {
	connection := clientStatementConnection(ClientCapabilityMultiStatements)
	plan := ConnectionPlanOptions{
		DefaultSchema:  "fallback",
		CatalogVersion: "catalog-v1",
		Scope: PhysicalScope{
			Placement: PlacementLocal,
		},
	}

	bundle := NewClientStatementBundle(connection, plan, "select 1", "select 2")
	if !bundle.Supported() {
		t.Fatalf("diagnostics = %#v, want supported bundle", bundle.Diagnostics)
	}
	requests := bundle.PlanRequests()
	if len(requests) != 2 {
		t.Fatalf("plan requests = %#v, want two requests", requests)
	}
	if requests[0].SQL != "select 1" || requests[1].SQL != "select 2" {
		t.Fatalf("sql = %q/%q, want ordered statements", requests[0].SQL, requests[1].SQL)
	}
	if requests[0].DefaultSchema != "quanta" || requests[0].CatalogVersion != "catalog-v1" || requests[0].Scope.Placement != PlacementLocal {
		t.Fatalf("request = %#v, want connection/session planning metadata", requests[0])
	}
}

func TestClientStatementBundleRejectsMultiStatementWithoutCapability(t *testing.T) {
	connection := clientStatementConnection()

	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1", "select 2")
	if bundle.Supported() {
		t.Fatalf("expected multi-statement bundle to be unsupported")
	}
	if !containsDiagnosticCode(bundle.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", bundle.Diagnostics)
	}
	if requests := bundle.PlanRequests(); len(requests) != 0 {
		t.Fatalf("plan requests = %#v, want none for unsupported bundle", requests)
	}
}

func TestClientStatementBundleRejectsEmptyStatementList(t *testing.T) {
	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{})
	if bundle.Supported() {
		t.Fatalf("expected empty statement list to be unsupported")
	}
	if !containsDiagnosticCode(bundle.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", bundle.Diagnostics)
	}
}

func TestClientStatementBundleRejectsEmptySQLText(t *testing.T) {
	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "")
	if bundle.Supported() {
		t.Fatalf("expected empty SQL text to be unsupported")
	}
	if !containsDiagnosticCode(bundle.Diagnostics.Codes(), DiagnosticParserBoundary) {
		t.Fatalf("diagnostics = %#v, want parser boundary", bundle.Diagnostics)
	}
}

func TestClientStatementBundleCarriesConnectionDiagnostics(t *testing.T) {
	connection := clientStatementConnection()
	connection.Session.User = ""
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "not authenticated")}

	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")
	if bundle.Supported() {
		t.Fatalf("expected unauthenticated bundle to be unsupported")
	}
	if !containsDiagnosticCode(bundle.Diagnostics.Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", bundle.Diagnostics)
	}
	if _, ok := bundle.FirstProtocolError(); !ok {
		t.Fatalf("expected protocol error")
	}
}

func TestClientStatementBundleCopiesMutableMetadata(t *testing.T) {
	connection := clientStatementConnection(ClientCapabilityMultiStatements)
	plan := ConnectionPlanOptions{
		Scope: PhysicalScope{
			Shards:   ShardSet{Shards: []ShardID{"s1"}},
			Replicas: []ReplicaID{"r1"},
		},
	}

	bundle := NewClientStatementBundle(connection, plan, "select 1")
	bundle.Connection.Session.Roles[0] = "writer"
	bundle.Connection.Capabilities[0] = ClientCapabilityBatching
	bundle.PlanOptions.Scope.Shards.Shards[0] = "s2"
	bundle.PlanOptions.Scope.Replicas[0] = "r2"
	if connection.Session.Roles[0] != "reader" {
		t.Fatalf("connection roles leaked mutation: %#v", connection.Session.Roles)
	}
	if connection.Capabilities[0] != ClientCapabilityMultiStatements {
		t.Fatalf("connection capabilities leaked mutation: %#v", connection.Capabilities)
	}
	if plan.Scope.Shards.Shards[0] != "s1" || plan.Scope.Replicas[0] != "r1" {
		t.Fatalf("plan scope leaked mutation: %#v", plan.Scope)
	}
}

func clientStatementConnection(capabilities ...ClientCapability) ConnectionContext {
	return ConnectionContext{
		Session: SessionContext{
			ID:            "session-1",
			User:          "moli",
			Roles:         []RoleName{"reader"},
			CurrentSchema: "quanta",
		},
		Protocol:     NewProtocolProfile(ProtocolMySQL, "mysql"),
		Capabilities: append(ClientCapabilities(nil), capabilities...),
	}
}
