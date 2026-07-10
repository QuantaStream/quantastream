package qsbridge

import "testing"

func TestExecutionRequestHandoffTraceSummarizesNativeBoundary(t *testing.T) {
	request := testPreparedSelectPlan(t).PreparedPlan().ExecutionRequest(
		ExecutionOptions{
			RequestID:    "req-1",
			MaxRows:      10,
			BatchSize:    4,
			Streaming:    true,
			Cancelable:   true,
			TraceExplain: true,
		},
		IndexedParameterValue(1, ValueFloat, 100.5),
	)

	trace := request.ExecutionHandoffTrace()
	if !trace.Supported {
		t.Fatalf("trace supported = false diagnostics=%#v", trace.Diagnostics)
	}
	if trace.RequestID != "req-1" || trace.Kind != QueryKindSelect {
		t.Fatalf("request/kind = %q/%q, want req-1/select", trace.RequestID, trace.Kind)
	}
	if trace.AccessIntent != PhysicalAccessRead || trace.Lifecycle != ClientPlanLifecycleSelect || trace.LifecycleSteps != 7 {
		t.Fatalf("access/lifecycle = %q/%q/%d, want read select lifecycle", trace.AccessIntent, trace.Lifecycle, trace.LifecycleSteps)
	}
	if trace.PhysicalRoot != PhysicalNodeProject || trace.PhysicalNodes == 0 {
		t.Fatalf("physical root/nodes = %q/%d, want project and nodes", trace.PhysicalRoot, trace.PhysicalNodes)
	}
	if !physicalTraceHas(trace.Strategies, PhysicalStrategyEncodingRange) {
		t.Fatalf("strategies = %#v, want encoding range", trace.Strategies)
	}
	if trace.ParameterCount != 1 || trace.ParameterValues != 1 {
		t.Fatalf("parameter counts = %d/%d, want 1/1", trace.ParameterCount, trace.ParameterValues)
	}
	if len(trace.ResultColumns) != 1 || trace.ResultColumns[0].Name != "order_id" {
		t.Fatalf("result columns = %#v, want order_id", trace.ResultColumns)
	}
	if !stringTraceHas(trace.RequiredFields, "o.o_orderkey") || !stringTraceHas(trace.RequiredFields, "o.o_totalprice") {
		t.Fatalf("required fields = %#v, want orderkey and totalprice", trace.RequiredFields)
	}
	if len(trace.Access) != 1 || trace.Access[0].Privilege != AccessSelect {
		t.Fatalf("access = %#v, want select requirement", trace.Access)
	}
	if !trace.Options.Streaming || !trace.Options.Cancelable || !trace.Options.TraceExplain {
		t.Fatalf("options = %#v, want streaming/cancelable/explain", trace.Options)
	}
}

func TestExecutionRequestHandoffTraceCarriesExecutionDiagnostics(t *testing.T) {
	request := testPreparedSelectPlan(t).PreparedPlan().ExecutionRequest(
		ExecutionOptions{RequestID: "req-bad", MaxRows: -1},
		IndexedParameterValue(1, ValueFloat, 100.5),
	)

	trace := request.ExecutionHandoffTrace()
	if trace.Supported {
		t.Fatalf("trace supported = true, want invalid options to block")
	}
	if !containsDiagnosticCode(trace.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", trace.Diagnostics.Codes())
	}
	if trace.RequestID != "req-bad" || trace.PhysicalRoot != PhysicalNodeProject {
		t.Fatalf("trace request/root = %q/%q, want diagnostics without losing plan shape", trace.RequestID, trace.PhysicalRoot)
	}
}

func TestExecutionRequestHandoffTraceCarriesWriteIntent(t *testing.T) {
	prepared := PreparedPlan{
		Kind:      QueryKindUpdate,
		Supported: true,
		Result:    ResultShape{Kind: ResultStatement},
		Statement: StatementResult{AffectedRows: 2},
	}
	trace := prepared.ExecutionRequest(ExecutionOptions{RequestID: "req-write"}).ExecutionHandoffTrace()
	if trace.RequestID != "req-write" || trace.Kind != QueryKindUpdate {
		t.Fatalf("request/kind = %q/%q, want req-write/update", trace.RequestID, trace.Kind)
	}
	if trace.AccessIntent != PhysicalAccessWrite || trace.Lifecycle != ClientPlanLifecycleMutation || trace.LifecycleSteps != 7 {
		t.Fatalf("access/lifecycle = %q/%q/%d, want write mutation lifecycle", trace.AccessIntent, trace.Lifecycle, trace.LifecycleSteps)
	}
	if trace.Statement.AffectedRows != 2 {
		t.Fatalf("statement = %#v, want affected rows", trace.Statement)
	}
}

func physicalTraceHas(strategies []PhysicalStrategy, want PhysicalStrategy) bool {
	for _, strategy := range strategies {
		if strategy == want {
			return true
		}
	}
	return false
}

func stringTraceHas(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
