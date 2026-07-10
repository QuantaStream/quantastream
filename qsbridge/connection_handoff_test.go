package qsbridge

import "testing"

func TestPlanningServicePrepareConnectionProtocolExecutionHandoffSelectsNative(t *testing.T) {
	service := handoffPlanningService()
	connection := testConnectionContext()

	handoff := service.PrepareConnectionProtocolExecutionHandoff(
		connection,
		"select o_orderkey from orders where o_orderkey = ?",
		ConnectionPlanOptions{DefaultSchema: "fallback"},
		ProtocolPreparedExecution,
		ExecutionOptions{RequestID: "conn-native-1"},
		IndexedParameterValue(1, ValueInt, 7),
	)
	if !handoff.Supported() {
		t.Fatalf("diagnostics = %#v, want supported connection handoff", handoff.Diagnostics())
	}
	if handoff.HandoffKind() != ExecutionHandoffNative {
		t.Fatalf("handoff kind = %q, want native", handoff.HandoffKind())
	}
	native, ok := handoff.NativeRequest()
	if !ok {
		t.Fatalf("expected native request")
	}
	if native.Bound.Prepared.Session.User != "moli" || native.Bound.Prepared.DefaultSchema != "quanta" {
		t.Fatalf("prepared session/schema = %#v/%q, want connection metadata", native.Bound.Prepared.Session, native.Bound.Prepared.DefaultSchema)
	}
}

func TestPlanningServicePrepareConnectionProtocolExecutionHandoffStopsOnDeniedConnection(t *testing.T) {
	service := handoffPlanningService()
	connection := testConnectionContext()
	connection.Session.User = ""
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "not authenticated")}

	handoff := service.PrepareConnectionProtocolExecutionHandoff(
		connection,
		"select o_orderkey from orders where o_orderkey = ?",
		ConnectionPlanOptions{},
		ProtocolPreparedExecution,
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, 7),
	)
	if handoff.Supported() {
		t.Fatalf("expected denied connection handoff to be unsupported")
	}
	if handoff.HandoffKind() != ExecutionHandoffDenied {
		t.Fatalf("handoff kind = %q, want denied", handoff.HandoffKind())
	}
	if !containsDiagnosticCode(handoff.Diagnostics().Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want connection access denied", handoff.Diagnostics())
	}
	if handoff.Handoff.Prepared.SQL != "" {
		t.Fatalf("prepared SQL = %q, expected no planning for denied connection", handoff.Handoff.Prepared.SQL)
	}
	if _, ok := handoff.NativeRequest(); ok {
		t.Fatalf("did not expect native request")
	}
	if _, ok := handoff.LegacyFallbackRequest(); ok {
		t.Fatalf("did not expect fallback request")
	}
}

func TestPlanningServicePrepareConnectionProtocolBatchHandoffSelectsNative(t *testing.T) {
	service := handoffPlanningService()
	connection := testConnectionContext()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityBatchExecution)

	handoff := service.PrepareConnectionProtocolBatchHandoff(
		connection,
		"select o_orderkey from orders where o_orderkey = ?",
		ConnectionPlanOptions{},
		ExecutionOptions{RequestID: "conn-batch-1"},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
	)
	if !handoff.Supported() {
		t.Fatalf("diagnostics = %#v, want supported connection batch handoff", handoff.Diagnostics())
	}
	if handoff.HandoffKind() != ExecutionHandoffNative {
		t.Fatalf("handoff kind = %q, want native", handoff.HandoffKind())
	}
	native, ok := handoff.NativeRequest()
	if !ok || len(native.ParameterSets) != 1 {
		t.Fatalf("native batch = %#v/%v, want one set", native, ok)
	}
}

func TestConnectionProtocolExecutionHandoffCopiesConnectionMetadata(t *testing.T) {
	service := handoffPlanningService()
	connection := testConnectionContext()

	handoff := service.PrepareConnectionProtocolExecutionHandoff(
		connection,
		"select o_orderkey from orders where o_orderkey = ?",
		ConnectionPlanOptions{},
		ProtocolPreparedExecution,
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, 7),
	)
	handoff.Connection.Session.Roles[0] = "writer"
	handoff.Connection.Capabilities[0] = ClientCapabilityBatching
	handoff.Connection.Attributes["client"] = "mutated"
	if connection.Session.Roles[0] != "reader" {
		t.Fatalf("connection session leaked mutation: %#v", connection.Session)
	}
	if connection.Capabilities[0] != ClientCapabilityPreparedStatements {
		t.Fatalf("connection capabilities leaked mutation: %#v", connection.Capabilities)
	}
	if connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", connection.Attributes)
	}
}

func testConnectionContext() ConnectionContext {
	return ConnectionContext{
		Session: SessionContext{
			ID:            "session-1",
			User:          "moli",
			Roles:         []RoleName{"reader"},
			CurrentSchema: "quanta",
		},
		Protocol:     NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements),
		Capabilities: []ClientCapability{ClientCapabilityPreparedStatements},
		Attributes:   map[string]string{"client": "mysql"},
	}
}
