package qsruntime

import (
	"math/big"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestPlanLegacyExecutionCallIncludesBitmapAndMaterializationSteps(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
			Value:     big.NewInt(1000),
		}},
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index:   "orders",
			Field:   "o_orderkey",
			Type:    qsbridge.DataTypeInt,
			Visible: true,
		}},
	})

	plan, diagnostics := PlanLegacyExecutionCall(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if plan.RootIndex != "orders" {
		t.Fatalf("root index = %q, want orders", plan.RootIndex)
	}
	if plan.FragmentCount != 1 {
		t.Fatalf("fragment count = %d, want 1", plan.FragmentCount)
	}
	if plan.ProjectionCount != 1 {
		t.Fatalf("projection count = %d, want 1", plan.ProjectionCount)
	}
	if !plan.HasMaterialization {
		t.Fatalf("has materialization = false, want true; %s", plan.Summary())
	}
	if !plan.UsesSessionPool {
		t.Fatalf("uses session pool = false, want true")
	}
	if got := plan.StepStatus(LegacyExecutionStepQueryBitIndex); got != LegacyExecutionStepStatusConcrete {
		t.Fatalf("query bit index status = %q, want concrete", got)
	}
	if got := plan.StepStatus(LegacyExecutionStepMaterializeProjection); got != LegacyExecutionStepStatusPlanned {
		t.Fatalf("materialize projection status = %q, want planned", got)
	}
	assertLegacyCallPlanSteps(t, plan, []LegacyExecutionCallStep{
		LegacyExecutionStepConstructSource,
		LegacyExecutionStepBorrowSession,
		LegacyExecutionStepBuildBitmapQuery,
		LegacyExecutionStepQueryBitIndex,
		LegacyExecutionStepAdaptBitmapResult,
		LegacyExecutionStepBuildCandidateSet,
		LegacyExecutionStepBuildMaterializationRequest,
		LegacyExecutionStepMaterializeProjection,
		LegacyExecutionStepReleaseSession,
	})
}

func TestPlanLegacyExecutionCallReportsMissingRootIndex(t *testing.T) {
	plan, diagnostics := PlanLegacyExecutionCall(NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", diagnostics)
	}
	if diagnostics[0].Code != qsbridge.DiagnosticInvalidExecutionOption {
		t.Fatalf("diagnostic code = %s, want %s", diagnostics[0].Code, qsbridge.DiagnosticInvalidExecutionOption)
	}
	if plan.RootIndex != "" {
		t.Fatalf("root index = %q, want empty", plan.RootIndex)
	}
}

func TestPlanLegacyExecutionCallKeepsSQLAssemblySeparate(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"orders"}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "order_count", Type: qsbridge.DataTypeInt}}
	request.Result = qsbridge.ResultShape{Limit: 10, Offset: 5}

	plan, diagnostics := PlanLegacyExecutionCall(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if plan.RootIndex != "orders" {
		t.Fatalf("root index = %q, want orders", plan.RootIndex)
	}
	if plan.SQLAggregateCount != 1 {
		t.Fatalf("SQL aggregate count = %d, want 1", plan.SQLAggregateCount)
	}
	if plan.HasMaterialization {
		t.Fatalf("has materialization = true, want false")
	}
	if !plan.Contains(LegacyExecutionStepApplySQLResultAssembly) {
		t.Fatalf("plan missing SQL result assembly step: %v", plan.Steps)
	}
	if len(plan.Notes) == 0 {
		t.Fatalf("notes = nil, want SQL assembly note")
	}
}

func TestPlanLegacyExecutionCallRecordsNativeAggregateHandoff(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index:   "lineitem",
			Field:   "l_shipmode",
			Type:    qsbridge.DataTypeString,
			Visible: true,
		}},
	})
	request.Aggregates = []qsbridge.QuantaAggregateRequest{{
		Operation: qsbridge.QuantaAggregateProjectorRank,
		Function:  "topn",
		Alias:     "topn_l_shipmode",
		Input: qsbridge.QuantaProjectionField{
			Index: "lineitem",
			Field: "l_shipmode",
			Type:  qsbridge.DataTypeString,
		},
	}}

	plan, diagnostics := PlanLegacyExecutionCall(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if plan.NativeAggregateCount != 1 {
		t.Fatalf("native aggregate count = %d, want 1", plan.NativeAggregateCount)
	}
	if !plan.Contains(LegacyExecutionStepAdaptNativeAggregateResult) {
		t.Fatalf("plan missing native aggregate result step: %v", plan.Steps)
	}
}

func TestPlanLegacyExecutionCallRecordsGroupedFilterAdapterStep(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "orders", Field: "o_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterLeaf,
			Fragment:  qsbridge.QuantaQueryFragment{Index: "orders", Field: "o_orderkey"},
		},
	})

	plan, diagnostics := PlanLegacyExecutionCall(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if !plan.Contains(LegacyExecutionStepAdaptFilterExpression) {
		t.Fatalf("plan missing filter adapter step: %v", plan.Steps)
	}
	if got := plan.StepStatus(LegacyExecutionStepAdaptFilterExpression); got != LegacyExecutionStepStatusPlanned {
		t.Fatalf("filter adapter status = %q, want planned", got)
	}
	if len(plan.Notes) == 0 {
		t.Fatalf("notes = nil, want grouped filter adapter note")
	}
}

func TestPlanLegacyExecutionCallMapsFixtureStatuses(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
			Value:     big.NewInt(1000),
		}},
	})

	plan, diagnostics := PlanLegacyExecutionCall(request)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	plan = plan.WithRuntimeProfile(FixtureRuntimeProfile("fixture"))
	if got := plan.StepStatus(LegacyExecutionStepBorrowSession); got != LegacyExecutionStepStatusPending {
		t.Fatalf("borrow session status = %q, want pending", got)
	}
	if got := plan.StepStatus(LegacyExecutionStepQueryBitIndex); got != LegacyExecutionStepStatusFixture {
		t.Fatalf("query bit index status = %q, want fixture", got)
	}
}

func assertLegacyCallPlanSteps(t *testing.T, plan LegacyExecutionCallPlan, expected []LegacyExecutionCallStep) {
	t.Helper()
	if len(plan.Steps) != len(expected) {
		t.Fatalf("steps = %v, want %v", plan.Steps, expected)
	}
	for i, step := range expected {
		if plan.Steps[i] != step {
			t.Fatalf("steps[%d] = %s, want %s; all steps = %v", i, plan.Steps[i], step, plan.Steps)
		}
	}
}
