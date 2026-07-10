package qsbridge

import "testing"

func TestPlanningServiceListClientConnectionCapabilitiesReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(
		ClientCapabilitySessionTracking,
		ClientCapabilityTLS,
	)
	connection.Protocol = NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityStatementResults,
		ProtocolCapabilityPreparedStatements,
		ProtocolCapabilityStructuredExplain,
		ProtocolCapabilityPlanCachePolicy,
	)

	exchange := service.ListClientConnectionCapabilities(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported capability metadata", exchange)
	}
	if len(exchange.Capabilities) != 6 {
		t.Fatalf("capabilities = %#v, want protocol and client capabilities", exchange.Capabilities)
	}
	if exchange.Capabilities[0].Scope != ClientCapabilityScopeClient || exchange.Capabilities[0].Name != "session_tracking" {
		t.Fatalf("first capability = %#v, want sorted client capability", exchange.Capabilities[0])
	}
	if exchange.Capabilities[2].Scope != ClientCapabilityScopeProtocol || exchange.Capabilities[2].Name != "plan_cache_policy" {
		t.Fatalf("third capability = %#v, want sorted protocol cache-policy capability", exchange.Capabilities[2])
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 6 || len(exchange.ResultSchema.Columns) != 5 {
		t.Fatalf("result/schema = %#v/%#v, want capability metadata result", exchange.Result, exchange.ResultSchema)
	}
	row := exchange.Result.Chunks[0].Rows[2]
	if row[0].Value != "protocol" || row[2].Value != "mysql" || row[3].Value != "mysql" || row[4].Value != true {
		t.Fatalf("row = %#v, want protocol capability metadata", row)
	}
}

func TestPlanningServiceListClientConnectionCapabilitiesReturnsFailedEnvelopeForUnsupportedConnection(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Protocol:    NewProtocolProfile(ProtocolMySQL, "mysql"),
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.ListClientConnectionCapabilities(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 5 {
		t.Fatalf("result/schema = %#v/%#v, want failed capability envelope", exchange.Result, exchange.ResultSchema)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", exchange.Diagnostics)
	}
}

func TestPlanningServiceListClientConnectionCapabilitiesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilitySessionTracking)
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)

	exchange := service.ListClientConnectionCapabilities(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Capabilities[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.ListClientConnectionCapabilities(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Capabilities[0].Name != "session_tracking" || again.Result.Chunks[0].Rows[0][1].Value != "session_tracking" {
		t.Fatalf("capability metadata leaked mutation: %#v/%#v", again.Capabilities, again.Result.Chunks)
	}
}
