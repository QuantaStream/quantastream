package qsbridge

import "context"

// SubqueryStepLifecycle records how far a subquery-related plan shape has moved
// from compatibility scaffolding toward native execution.
type SubqueryStepLifecycle string

const (
	// SubqueryStepCompatibility marks work still owned by compatibility/preflight scratchpad code.
	SubqueryStepCompatibility SubqueryStepLifecycle = "compatibility"
	// SubqueryStepNativeReady marks work with a stable native contract but no bitmap-native executor yet.
	SubqueryStepNativeReady SubqueryStepLifecycle = "native_ready"
	// SubqueryStepNativeExecutable marks work with a native executor implementation.
	SubqueryStepNativeExecutable SubqueryStepLifecycle = "native_executable"
)

// NativeSubqueryStepKind identifies one first-class subquery execution step.
type NativeSubqueryStepKind string

const (
	// NativeSubqueryStepScalarMaterialization materializes one scalar subquery value.
	NativeSubqueryStepScalarMaterialization NativeSubqueryStepKind = "scalar_subquery_materialization"
	// NativeSubqueryStepParentKeyLookup materializes parent keys that seed correlated helper work.
	NativeSubqueryStepParentKeyLookup NativeSubqueryStepKind = "parent_key_lookup"
	// NativeSubqueryStepAggregateThresholdLookup materializes aggregate thresholds keyed by correlated values.
	NativeSubqueryStepAggregateThresholdLookup NativeSubqueryStepKind = "aggregate_threshold_lookup"
)

// NativeSubqueryStep is the executor-facing contract for first-class subquery
// work. The initial scalar implementation may still delegate to SQL-backed
// compatibility execution, but callers should consume this contract rather than
// SQL-text rewrite details.
type NativeSubqueryStep struct {
	Name               string
	Kind               NativeSubqueryStepKind
	Lifecycle          SubqueryStepLifecycle
	SubqueryKind       SubqueryIntentKind
	Inputs             []string
	Outputs            []string
	Materialization    string
	BitmapNativeTarget string
	ExecutionMode      string
	Diagnostics        DiagnosticSet
}

// NativeSubqueryStepReport is a compact explain/inspection view of a native step.
type NativeSubqueryStepReport struct {
	Name               string
	Kind               NativeSubqueryStepKind
	Lifecycle          SubqueryStepLifecycle
	SubqueryKind       SubqueryIntentKind
	Inputs             []string
	Outputs            []string
	Materialization    string
	BitmapNativeTarget string
	ExecutionMode      string
	Diagnostics        int
}

// NativeSubqueryStepExecutionRequest is the executor-facing request envelope for
// one native subquery step.
type NativeSubqueryStepExecutionRequest struct {
	Step       NativeSubqueryStep
	Parameters []ParameterValue
}

// NativeSubqueryStepExecutionResult is the executor-facing result envelope for
// one native subquery step.
type NativeSubqueryStepExecutionResult struct {
	Step        NativeSubqueryStep
	Outputs     map[string]ResultCell
	RowSet      QuantaProjectedRowSet
	Diagnostics DiagnosticSet
}

// NativeSubqueryStepTrace summarizes an attempted native subquery step execution.
type NativeSubqueryStepTrace struct {
	StepName      string
	StepKind      NativeSubqueryStepKind
	Lifecycle     SubqueryStepLifecycle
	ExecutionMode string
	OutputCount   int
	RowCount      int
	Diagnostics   int
}

// Trace returns a compact execution trace for management inspection.
func (r NativeSubqueryStepExecutionResult) Trace() NativeSubqueryStepTrace {
	return NativeSubqueryStepTrace{
		StepName:      r.Step.Name,
		StepKind:      r.Step.Kind,
		Lifecycle:     r.Step.Lifecycle,
		ExecutionMode: r.Step.ExecutionMode,
		OutputCount:   len(r.Outputs),
		RowCount:      r.RowSet.CandidateCount(),
		Diagnostics:   len(r.Diagnostics),
	}
}

// NativeSubqueryStepExecutor executes first-class subquery work.
type NativeSubqueryStepExecutor interface {
	// ExecuteNativeSubqueryStep runs one native subquery step.
	ExecuteNativeSubqueryStep(context.Context, NativeSubqueryStepExecutionRequest) (NativeSubqueryStepExecutionResult, error)
}

// Report returns a compact inspection view of a native subquery step.
func (s NativeSubqueryStep) Report() NativeSubqueryStepReport {
	return NativeSubqueryStepReport{
		Name:               s.Name,
		Kind:               s.Kind,
		Lifecycle:          s.Lifecycle,
		SubqueryKind:       s.SubqueryKind,
		Inputs:             append([]string(nil), s.Inputs...),
		Outputs:            append([]string(nil), s.Outputs...),
		Materialization:    s.Materialization,
		BitmapNativeTarget: s.BitmapNativeTarget,
		ExecutionMode:      s.ExecutionMode,
		Diagnostics:        len(s.Diagnostics),
	}
}

// NativeSubquerySteps lowers helper-plan sketches that have a first-class
// native execution contract into native step descriptors.
func NativeSubquerySteps(root LogicalNode) []NativeSubqueryStep {
	return nativeSubqueryStepsFromHelperPlans(LowerSubqueryHelperPlans(root))
}

// NativeSubqueryStepReports returns compact reports for all native subquery steps.
func NativeSubqueryStepReports(root LogicalNode) []NativeSubqueryStepReport {
	return nativeSubqueryStepReports(NativeSubquerySteps(root))
}

func nativeSubqueryStepReportsForIntents(intents []SubqueryPlanIntent) []NativeSubqueryStepReport {
	return nativeSubqueryStepReports(nativeSubqueryStepsFromHelperPlans(lowerSubqueryHelperPlansForIntents(intents)))
}

func nativeSubqueryStepReports(steps []NativeSubqueryStep) []NativeSubqueryStepReport {
	if len(steps) == 0 {
		return nil
	}
	reports := make([]NativeSubqueryStepReport, 0, len(steps))
	for _, step := range steps {
		reports = append(reports, step.Report())
	}
	return reports
}

func nativeSubqueryStepsFromHelperPlans(plans []SubqueryHelperPlan) []NativeSubqueryStep {
	steps := make([]NativeSubqueryStep, 0, len(plans))
	for _, plan := range plans {
		if step, ok := plan.NativeStep(); ok {
			steps = append(steps, step)
		}
	}
	return steps
}

func cloneNativeSubqueryStepReports(reports []NativeSubqueryStepReport) []NativeSubqueryStepReport {
	if len(reports) == 0 {
		return nil
	}
	cloned := make([]NativeSubqueryStepReport, 0, len(reports))
	for _, report := range reports {
		report.Inputs = append([]string(nil), report.Inputs...)
		report.Outputs = append([]string(nil), report.Outputs...)
		cloned = append(cloned, report)
	}
	return cloned
}
