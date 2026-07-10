package qsbridge

import (
	"context"
	"testing"
)

type testNativeSubqueryStepExecutor struct {
	request NativeSubqueryStepExecutionRequest
}

func (e *testNativeSubqueryStepExecutor) ExecuteNativeSubqueryStep(ctx context.Context, request NativeSubqueryStepExecutionRequest) (NativeSubqueryStepExecutionResult, error) {
	e.request = request
	return NativeSubqueryStepExecutionResult{
		Step: request.Step,
		Outputs: map[string]ResultCell{
			request.Step.Outputs[0]: {Kind: ValueInt, Value: int64(7)},
		},
	}, nil
}

func TestNativeSubqueryStepsPromotesNativeReadyHelpers(t *testing.T) {
	root := ScalarSubqueryNode{Intents: []SubqueryPlanIntent{{
		Kind: SubqueryIntentScalar,
		Scalar: &ScalarSubqueryIntent{
			OutputName: "scalar_value",
		},
	}, {
		Kind: SubqueryIntentCorrelatedAggregate,
		HelperIntents: []SubqueryHelperIntent{{
			Name:               "correlated_parent_keys",
			Kind:               string(SubqueryHelperPlanParentKeyLookup),
			Outputs:            []string{"p.p_partkey"},
			Materialization:    "parent key set",
			BitmapNativeTarget: "bitmap-filtered parent key rownum set",
		}, {
			Name:               "correlated_average_thresholds",
			Kind:               string(SubqueryHelperPlanAggregateThresholdLookup),
			Outputs:            []string{"p.p_partkey", "threshold"},
			Materialization:    "per-key aggregate threshold map",
			BitmapNativeTarget: "aggregate-threshold helper kernel feeding bitmap predicate branches",
		}},
	}}}

	steps := NativeSubquerySteps(root)
	if got, want := len(steps), 3; got != want {
		t.Fatalf("native steps = %d, want %d: %#v", got, want, steps)
	}
	wantKinds := []NativeSubqueryStepKind{
		NativeSubqueryStepScalarMaterialization,
		NativeSubqueryStepParentKeyLookup,
		NativeSubqueryStepAggregateThresholdLookup,
	}
	for i, want := range wantKinds {
		if steps[i].Kind != want || steps[i].Lifecycle != SubqueryStepNativeReady {
			t.Fatalf("native step[%d] = %#v, want %s native-ready step", i, steps[i], want)
		}
	}
	if steps[0].ExecutionMode != "sql_backed_until_bitmap_native_executor_exists" {
		t.Fatalf("execution mode = %q", steps[0].ExecutionMode)
	}
}

func TestNativeSubqueryStepExecutionContractTracesResult(t *testing.T) {
	step := NativeSubqueryStep{
		Name:          "scalar_value",
		Kind:          NativeSubqueryStepScalarMaterialization,
		Lifecycle:     SubqueryStepNativeReady,
		SubqueryKind:  SubqueryIntentScalar,
		Outputs:       []string{"scalar_value"},
		ExecutionMode: "sql_backed_until_bitmap_native_executor_exists",
	}
	executor := &testNativeSubqueryStepExecutor{}

	result, err := executor.ExecuteNativeSubqueryStep(context.Background(), NativeSubqueryStepExecutionRequest{Step: step})
	if err != nil {
		t.Fatalf("execute native step: %v", err)
	}
	if executor.request.Step.Name != "scalar_value" {
		t.Fatalf("request step = %#v", executor.request.Step)
	}
	trace := result.Trace()
	if trace.StepKind != NativeSubqueryStepScalarMaterialization || trace.OutputCount != 1 || trace.Diagnostics != 0 {
		t.Fatalf("trace = %#v", trace)
	}
}
