package qsbridge

import "testing"

func TestDefaultRoutePolicySummaryAggregatesProfiles(t *testing.T) {
	summary := DefaultRoutePolicySummary()
	if summary.PolicyCount != 5 || summary.DefaultCount != 1 {
		t.Fatalf("summary = %#v, want five policies with one default", summary)
	}
	if summary.NativeAllowedCount != 2 || summary.FallbackAllowedCount != 3 {
		t.Fatalf("summary = %#v, want native and fallback policy counts", summary)
	}
	if summary.RejectsUnsupportedCount != 2 {
		t.Fatalf("summary = %#v, want native-only policies to reject unsupported requests", summary)
	}
	if summary.NativeRoutingEnabledCount != 2 || summary.NativeRoutingDisabledCount != 2 {
		t.Fatalf("summary = %#v, want enabled and disabled native routing counts", summary)
	}
}

func TestPlanningServiceSummarizeClientRoutePoliciesReturnsOneRow(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.SummarizeClientRoutePolicies(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported route policy summary metadata", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one route policy summary", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 7 || exchange.ResultSchema.Columns[0].Name != "Policies" {
		t.Fatalf("schema = %#v, want route policy summary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 {
		t.Fatalf("result = %#v, want one row returned", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientRoutePoliciesReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.SummarizeClientRoutePolicies(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientRoutePoliciesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientRoutePolicies(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].PolicyCount = -1
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.SummarizeClientRoutePolicies(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].PolicyCount != 5 {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Policies" || again.ResultSchema.Columns[0].Name != "Policies" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
