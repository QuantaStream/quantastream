package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientProtocolExecutionNegotiationReturnsSupportedCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityPreparedStatements,
		ProtocolCapabilityStreamingResults,
		ProtocolCapabilityForwardOnlyCursor,
		ProtocolCapabilityCancellation,
		ProtocolCapabilityExplain,
	)

	exchange := service.SummarizeClientProtocolExecutionNegotiation(connection, ProtocolPreparedExecution, ExecutionOptions{
		RequestID:    "req-1",
		Streaming:    true,
		Cursor:       CursorForwardOnly,
		Cancelable:   true,
		TraceExplain: true,
	})
	if !exchange.Supported() || !exchange.Negotiation.Supported() {
		t.Fatalf("exchange = %#v, want supported protocol negotiation summary", exchange)
	}
	row := exchange.Row
	if row.NegotiationCount != 1 || row.SupportedCount != 1 || row.UnsupportedCount != 0 {
		t.Fatalf("row = %#v, want supported negotiation counts", row)
	}
	if row.CapabilityCount != 5 || row.DiagnosticCount != 0 || row.StreamingRequestedCount != 1 || row.CursorRequestedCount != 1 {
		t.Fatalf("row = %#v, want capability and option counts", row)
	}
	if row.CancelableCount != 1 || row.ExplainCount != 1 || row.ProfileCount != 0 {
		t.Fatalf("row = %#v, want execution option counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want protocol negotiation summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 1 || resultRow[1].Value != 1 || resultRow[3].Value != 5 {
		t.Fatalf("result row = %#v, want negotiation summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientProtocolExecutionNegotiationReportsUnsupportedOptionsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.SummarizeClientProtocolExecutionNegotiation(connection, ProtocolBatchExecution, ExecutionOptions{
		Streaming:      true,
		Cursor:         CursorForwardOnly,
		Cancelable:     true,
		TraceExplain:   true,
		IncludeProfile: true,
	})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want metadata exchange supported", exchange)
	}
	row := exchange.Row
	if row.SupportedCount != 0 || row.UnsupportedCount != 1 || row.DiagnosticCount != 6 {
		t.Fatalf("row = %#v, want unsupported negotiation diagnostics as data", row)
	}
	if row.StreamingRequestedCount != 1 || row.ProfileCount != 1 {
		t.Fatalf("row = %#v, want requested option counts preserved", row)
	}
}

func TestPlanningServiceSummarizeClientProtocolExecutionNegotiationCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)

	exchange := service.SummarizeClientProtocolExecutionNegotiation(connection, ProtocolPreparedExecution, ExecutionOptions{RequestID: "req-1"})
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Negotiation.Profile.Capabilities[0] = ProtocolCapabilityBatchExecution
	exchange.Row.CapabilityCount = 99
	exchange.Result.Chunks[0].Rows[0][3].Value = 99

	again := service.SummarizeClientProtocolExecutionNegotiation(connection, ProtocolPreparedExecution, ExecutionOptions{RequestID: "req-1"})
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Negotiation.Profile.Capabilities[0] != ProtocolCapabilityPreparedStatements {
		t.Fatalf("negotiation leaked mutation: %#v", again.Negotiation.Profile.Capabilities)
	}
	if again.Row.CapabilityCount != 1 || again.Result.Chunks[0].Rows[0][3].Value != 1 {
		t.Fatalf("negotiation summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}
