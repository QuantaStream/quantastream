package qsbridge

import "testing"

func TestPlanningServiceNegotiateClientProtocolExecutionReturnsSupportedRow(t *testing.T) {
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

	exchange := service.NegotiateClientProtocolExecution(connection, ProtocolPreparedExecution, ExecutionOptions{
		RequestID:    "req-1",
		MaxRows:      100,
		BatchSize:    25,
		Streaming:    true,
		Cursor:       CursorForwardOnly,
		Cancelable:   true,
		TraceExplain: true,
	})
	if !exchange.Supported() || !exchange.Negotiation.Supported() {
		t.Fatalf("exchange = %#v, want supported protocol negotiation", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one negotiation row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.Protocol != ProtocolMySQL || row.Mode != ProtocolPreparedExecution || !row.Streaming || row.Cursor != CursorForwardOnly {
		t.Fatalf("row = %#v, want prepared streaming cursor negotiation", row)
	}
	if row.RequestID != "req-1" || row.MaxRows != 100 || row.BatchSize != 25 || !row.Cancelable || !row.Explain {
		t.Fatalf("row = %#v, want execution options copied", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 14 {
		t.Fatalf("result/schema = %#v/%#v, want one negotiation row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "mysql" || resultRow[2].Value != "prepared" || resultRow[3].Value != true {
		t.Fatalf("result row = %#v, want supported prepared negotiation cells", resultRow)
	}
}

func TestPlanningServiceNegotiateClientProtocolExecutionReportsUnsupportedOptionsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.NegotiateClientProtocolExecution(connection, ProtocolBatchExecution, ExecutionOptions{
		Streaming:      true,
		Cursor:         CursorForwardOnly,
		Cancelable:     true,
		TraceExplain:   true,
		IncludeProfile: true,
	})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want metadata exchange supported", exchange)
	}
	if exchange.Negotiation.Supported() || exchange.Rows[0].Supported {
		t.Fatalf("negotiation = %#v rows=%#v, want unsupported requested shape", exchange.Negotiation, exchange.Rows)
	}
	if got := len(exchange.Rows[0].DiagnosticCodes); got != 6 {
		t.Fatalf("diagnostics = %#v, want mode plus five option diagnostics", exchange.Rows[0].DiagnosticCodes)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result = %#v, want successful metadata result", exchange.Result)
	}
}

func TestPlanningServiceNegotiateClientProtocolExecutionCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)

	exchange := service.NegotiateClientProtocolExecution(connection, ProtocolPreparedExecution, ExecutionOptions{RequestID: "req-1"})
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Negotiation.Profile.Capabilities[0] = ProtocolCapabilityBatchExecution
	exchange.Rows[0].Capabilities[0] = ProtocolCapabilityBatchExecution
	exchange.Result.Chunks[0].Rows[0][12].Value = "mutated"

	again := service.NegotiateClientProtocolExecution(connection, ProtocolPreparedExecution, ExecutionOptions{RequestID: "req-1"})
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Negotiation.Profile.Capabilities[0] != ProtocolCapabilityPreparedStatements || again.Rows[0].Capabilities[0] != ProtocolCapabilityPreparedStatements {
		t.Fatalf("negotiation leaked mutation: %#v/%#v", again.Negotiation.Profile.Capabilities, again.Rows[0].Capabilities)
	}
	if again.Result.Chunks[0].Rows[0][12].Value != string(ProtocolCapabilityPreparedStatements) {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
