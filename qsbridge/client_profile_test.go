package qsbridge

import "testing"

func TestPlanningServicePrepareClientExecutionProfileBuildsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	profile := ExecutionProfile{
		RequestID:      "req-1",
		TraceExplain:   true,
		IncludeProfile: true,
		LogicalPlan:    "scan(orders)",
		PhysicalPlan:   "physical_scan(orders)",
		Timings: []ExecutionTiming{{
			Name:    "scan",
			Elapsed: 12,
			Unit:    "ms",
		}},
		Counters: []ExecutionCounter{{
			Name:  "rows",
			Value: 7,
			Unit:  "rows",
		}},
		Diagnostics: DiagnosticSet{{
			Code:     DiagnosticNativeBlocker,
			Severity: SeverityWarning,
			Phase:    PhasePlan,
			Message:  "note",
		}},
	}

	exchange := service.PrepareClientExecutionProfile(connection, profile)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported profile metadata", exchange)
	}
	if exchange.Profile.RequestID != "req-1" || len(exchange.Rows) != 5 {
		t.Fatalf("exchange = %#v, want copied profile and five rows", exchange)
	}
	if exchange.Rows[0].Section != "explain" || exchange.Rows[0].Name != "logical" || exchange.Rows[0].Value.Value != "scan(orders)" {
		t.Fatalf("logical row = %#v, want logical explain row", exchange.Rows[0])
	}
	if exchange.Rows[2].Section != "timing" || exchange.Rows[2].Value.Value != int64(12) || exchange.Rows[2].Unit != "ms" {
		t.Fatalf("timing row = %#v, want timing row", exchange.Rows[2])
	}
	if exchange.Rows[3].Section != "counter" || exchange.Rows[3].Value.Value != uint64(7) || exchange.Rows[3].Unit != "rows" {
		t.Fatalf("counter row = %#v, want counter row", exchange.Rows[3])
	}
	if exchange.Rows[4].Section != "diagnostic" || exchange.Rows[4].Name != string(DiagnosticNativeBlocker) || exchange.Rows[4].Detail != "note" {
		t.Fatalf("diagnostic row = %#v, want diagnostic row", exchange.Rows[4])
	}
	if len(exchange.ResultSchema.Columns) != 5 || exchange.ResultSchema.Columns[0].Name != "Section" || exchange.ResultSchema.Columns[4].Name != "Detail" {
		t.Fatalf("schema = %#v, want profile result schema", exchange.ResultSchema)
	}
	if exchange.Result.RequestID != "req-1" || exchange.Result.RowsReturned != 5 || exchange.Result.Chunks[0].Rows[1][2].Value != "physical_scan(orders)" {
		t.Fatalf("result = %#v, want profile result rows", exchange.Result)
	}
}

func TestPlanningServicePrepareClientExecutionProfileReturnsFailedEnvelopeForBlockingDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	profile := ExecutionProfile{
		RequestID: "req-1",
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "profile failed"),
		},
	}

	exchange := service.PrepareClientExecutionProfile(connection, profile)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocking profile diagnostics", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 5 {
		t.Fatalf("result/schema = %#v/%#v, want failed profile envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServicePrepareClientExecutionProfileCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	profile := ExecutionProfile{
		Timings:  []ExecutionTiming{{Name: "scan"}},
		Counters: []ExecutionCounter{{Name: "rows"}},
		Diagnostics: DiagnosticSet{{
			Code:   DiagnosticNativeBlocker,
			Fields: []FieldRef{{Name: "original"}},
		}},
	}

	exchange := service.PrepareClientExecutionProfile(connection, profile)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Profile.Timings[0].Name = "mutated"
	exchange.Profile.Counters[0].Name = "mutated"
	exchange.Profile.Diagnostics[0].Fields[0].Name = "mutated"
	exchange.Rows[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Profile.Timings[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.PrepareClientExecutionProfile(connection, profile)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Profile.Timings[0].Name != "scan" || again.Profile.Counters[0].Name != "rows" {
		t.Fatalf("profile leaked mutation: %#v", again.Profile)
	}
	if again.Profile.Diagnostics[0].Fields[0].Name != "original" {
		t.Fatalf("profile diagnostics leaked mutation: %#v", again.Profile.Diagnostics)
	}
	if again.Rows[0].Name != "scan" {
		t.Fatalf("rows leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Section" || again.ResultSchema.Columns[0].Name != "Section" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Profile.Timings[0].Name != "scan" || again.Result.Chunks[0].Rows[0][1].Value != "scan" {
		t.Fatalf("result profile/rows leaked mutation: %#v/%#v", again.Result.Profile, again.Result.Chunks)
	}
}
