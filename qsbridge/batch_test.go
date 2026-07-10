package qsbridge

import "testing"

func TestPreparedPlanBatchExecutionRequestValidatesMultipleParameterSets(t *testing.T) {
	prepared := PreparedPlan{
		SQL:        "insert into orders values (?)",
		Kind:       QueryKindInsert,
		Supported:  true,
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeInt}},
		Result:     ResultShape{Kind: ResultStatement},
		Statement:  StatementResult{Status: "queued"},
	}

	request := prepared.BatchExecutionRequest(
		ExecutionOptions{RequestID: "batch-1", BatchSize: 2},
		ParameterValues(IndexedParameterValue(1, ValueInt, 10)),
		ParameterValues(IndexedParameterValue(1, ValueInt, 11)),
	)
	if !request.SupportedForExecution() {
		t.Fatalf("unexpected diagnostics: %#v", request.Diagnostics)
	}
	if len(request.ParameterSets) != 2 {
		t.Fatalf("parameter sets = %d, want 2", len(request.ParameterSets))
	}
	if request.ParameterSets[1].Bindings[0].Value.Value != 11 {
		t.Fatalf("second binding = %#v, want value 11", request.ParameterSets[1].Bindings)
	}
	if request.Options.RequestID != "batch-1" || request.Statement.Status != "queued" {
		t.Fatalf("request metadata = %#v/%#v, want copied options and statement", request.Options, request.Statement)
	}
}

func TestPreparedPlanBatchExecutionRequestReportsInvalidSets(t *testing.T) {
	prepared := PreparedPlan{
		Supported:  true,
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeInt}},
	}

	request := prepared.BatchExecutionRequest(
		ExecutionOptions{},
		ParameterValues(IndexedParameterValue(1, ValueInt, 10)),
		ParameterValues(IndexedParameterValue(1, ValueString, "bad")),
	)
	if request.SupportedForExecution() {
		t.Fatalf("expected unsupported batch request")
	}
	if !containsDiagnosticCode(request.Diagnostics.Codes(), DiagnosticParameterTypeMismatch) {
		t.Fatalf("diagnostics = %#v, want parameter mismatch", request.Diagnostics.Codes())
	}
	if len(request.ParameterSets) != 2 || !request.ParameterSets[1].Diagnostics.BlocksNative() {
		t.Fatalf("parameter sets = %#v, want second set diagnostics", request.ParameterSets)
	}
}

func TestPreparedPlanBatchExecutionRequestRequiresOneSet(t *testing.T) {
	request := PreparedPlan{Supported: true}.BatchExecutionRequest(ExecutionOptions{})
	if request.SupportedForExecution() {
		t.Fatalf("expected empty batch to be unsupported")
	}
	if !containsDiagnosticCode(request.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", request.Diagnostics.Codes())
	}
}

func TestBatchExecutionRequestRouteUsesExecutionDiagnostics(t *testing.T) {
	request := PreparedPlan{Supported: true}.BatchExecutionRequest(
		ExecutionOptions{},
		ParameterValues(),
	)
	decision := request.Route(CompatibilityRoutingPolicy())
	if decision.Kind != RouteNative {
		t.Fatalf("decision = %#v, want native route for valid empty-parameter batch", decision)
	}

	invalid := PreparedPlan{
		Supported:  true,
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeInt}},
	}.BatchExecutionRequest(
		ExecutionOptions{},
		ParameterValues(IndexedParameterValue(1, ValueString, "bad")),
	)
	decision = invalid.Route(NativeOnlyRoutingPolicy())
	if decision.Kind != RouteRejected {
		t.Fatalf("decision = %#v, want rejected native-only invalid batch", decision)
	}
}
