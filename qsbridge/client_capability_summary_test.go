package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientConnectionCapabilitiesReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(
		ClientCapabilityPreparedStatements,
		ClientCapabilityBatching,
		ClientCapabilityCompression,
		ClientCapabilityTLS,
		ClientCapabilitySessionTracking,
	)
	connection.Protocol = NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityPreparedStatements,
		ProtocolCapabilityBatchExecution,
		ProtocolCapabilityStreamingResults,
		ProtocolCapabilityCancellation,
		ProtocolCapabilityStructuredExplain,
		ProtocolCapabilityProfile,
		ProtocolCapabilitySessionActions,
	)

	exchange := service.SummarizeClientConnectionCapabilities(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported capability summary", exchange)
	}
	row := exchange.Row
	if row.CapabilityCount != 12 || row.ProtocolCapabilityCount != 7 || row.ClientCapabilityCount != 5 || row.AdvertisedCount != 12 {
		t.Fatalf("row = %#v, want protocol and client capability counts", row)
	}
	if row.PreparedCount != 2 || row.BatchCount != 2 || row.StreamingCount != 1 || row.CancellationCount != 1 {
		t.Fatalf("row = %#v, want execution capability family counts", row)
	}
	if row.ExplainCount != 1 || row.ProfileCount != 1 || row.SessionActionCount != 1 {
		t.Fatalf("row = %#v, want management capability family counts", row)
	}
	if row.TLSCount != 1 || row.CompressionCount != 1 || row.SessionTrackingCount != 1 {
		t.Fatalf("row = %#v, want accepted client transport/session counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 14 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 12 || resultRow[1].Value != 7 || resultRow[11].Value != 1 {
		t.Fatalf("result row = %#v, want capability summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientConnectionCapabilitiesReturnsFailedEnvelopeForUnsupportedConnection(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Protocol:    NewProtocolProfile(ProtocolMySQL, "mysql"),
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.SummarizeClientConnectionCapabilities(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || exchange.Result.RowsReturned != 0 {
		t.Fatalf("result = %#v, want failed rowless capability summary", exchange.Result)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", exchange.Diagnostics)
	}
}

func TestPlanningServiceSummarizeClientConnectionCapabilitiesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilitySessionTracking)
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)

	exchange := service.SummarizeClientConnectionCapabilities(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.CapabilityCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientConnectionCapabilities(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.CapabilityCount != 2 {
		t.Fatalf("summary leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Capability_count" || again.ResultSchema.Columns[0].Name != "Capability_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 2 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
