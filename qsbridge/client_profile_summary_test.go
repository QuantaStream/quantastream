package qsbridge

import "testing"

func TestPlanningServicePrepareClientExecutionProfileSummaryReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	profile := ExecutionProfile{
		RequestID:      "req-1",
		AccessIntent:   PhysicalAccessRead,
		Lifecycle:      ClientPlanLifecycleSelect,
		LifecycleSteps: 7,
		TraceExplain:   true,
		IncludeProfile: true,
		LogicalPlan:    "scan(orders)",
		PhysicalPlan:   "physical_scan(orders)",
		Timings:        []ExecutionTiming{{Name: "scan", Elapsed: 12, Unit: "ms"}},
		Counters:       []ExecutionCounter{{Name: "rows", Value: 7, Unit: "rows"}},
		Diagnostics: DiagnosticSet{{
			Code:     DiagnosticNativeBlocker,
			Severity: SeverityWarning,
			Phase:    PhasePlan,
			Message:  "note",
		}},
	}

	exchange := service.PrepareClientExecutionProfileSummary(connection, profile)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported profile summary", exchange)
	}
	row := exchange.Row
	if row.RowCount != 5 || row.ExplainCount != 2 || row.TimingCount != 1 || row.CounterCount != 1 || row.DiagnosticCount != 1 {
		t.Fatalf("row = %#v, want profile section counts", row)
	}
	if row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want read/select lifecycle metadata", row)
	}
	if !row.TraceExplain || !row.IncludeProfile {
		t.Fatalf("row = %#v, want profile request flags", row)
	}
	if exchange.Result.RequestID != "req-1" || exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want profile summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 5 || resultRow[1].Value != string(PhysicalAccessRead) || resultRow[4].Value != 2 || resultRow[9].Value != true {
		t.Fatalf("result row = %#v, want profile summary cells", resultRow)
	}
}

func TestPlanningServicePrepareClientExecutionProfileSummaryCountsExecutorCounters(t *testing.T) {
	service := simpleRunnerPlanningService()
	connection := clientStatementConnection()
	handoff := service.PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o order by o.o_orderkey"},
		ExecutionOptions{RequestID: "profile-1", IncludeProfile: true, BatchSize: 1, MaxRows: 2},
	)
	result := ExecutionDispatcher{
		Native: NewInMemoryNativeExecutor(simpleRunnerOrdersFixture()...),
	}.Dispatch(handoff)

	exchange := service.PrepareClientExecutionProfileSummary(connection, result.Profile)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported profile summary", exchange)
	}
	if exchange.Row.RowCount != 3 || exchange.Row.CounterCount != 3 {
		t.Fatalf("row = %#v, want three executor counter rows", exchange.Row)
	}
	if !exchange.Row.IncludeProfile || exchange.Row.AccessIntent != PhysicalAccessRead || exchange.Row.Lifecycle != ClientPlanLifecycleSelect {
		t.Fatalf("row = %#v, want profiled read/select summary", exchange.Row)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][6].Value != 3 {
		t.Fatalf("result = %#v, want profile summary counter count row", exchange.Result)
	}
}

func TestExecutionProfileCarriesPreparedPlanLifecycle(t *testing.T) {
	prepared := PreparedPlan{Kind: QueryKindSelect}
	profile := newExecutionProfile(ExecutionOptions{RequestID: "req-1"}, prepared)

	if profile.RequestID != "req-1" || profile.AccessIntent != PhysicalAccessRead || profile.Lifecycle != ClientPlanLifecycleSelect || profile.LifecycleSteps != 7 {
		t.Fatalf("profile = %#v, want prepared plan lifecycle metadata", profile)
	}
}

func TestPlanningServicePrepareClientExecutionProfileSummaryReturnsFailedEnvelopeForBlockingDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	profile := ExecutionProfile{
		RequestID:   "req-1",
		LogicalPlan: "scan(orders)",
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "profile failed"),
		},
	}

	exchange := service.PrepareClientExecutionProfileSummary(connection, profile)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocking profile diagnostics", exchange)
	}
	if exchange.Row.RowCount != 2 || exchange.Row.ExplainCount != 1 || exchange.Row.DiagnosticCount != 1 {
		t.Fatalf("row = %#v, want profile rows counted even when result fails", exchange.Row)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want failed profile summary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServicePrepareClientExecutionProfileSummaryCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	profile := ExecutionProfile{
		RequestID: "req-1",
		Timings:   []ExecutionTiming{{Name: "scan"}},
	}

	exchange := service.PrepareClientExecutionProfileSummary(connection, profile)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Profile.Timings[0].Name = "mutated"
	exchange.Row.RowCount = 99
	exchange.Result.Profile.Timings[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.PrepareClientExecutionProfileSummary(connection, profile)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Profile.Timings[0].Name != "scan" || again.Result.Profile.Timings[0].Name != "scan" {
		t.Fatalf("profile leaked mutation: %#v/%#v", again.Profile, again.Result.Profile)
	}
	if again.Row.RowCount != 1 || again.Row.TimingCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("profile summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}
