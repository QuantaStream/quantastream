package qsruntime

import (
	"fmt"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// LegacyExecutionCallStep names one boundary call in the direct legacy runtime path.
type LegacyExecutionCallStep string

// Legacy execution call steps document the expected legacy handoff sequence.
const (
	LegacyExecutionStepConstructSource             LegacyExecutionCallStep = "construct_source"
	LegacyExecutionStepBorrowSession               LegacyExecutionCallStep = "borrow_session"
	LegacyExecutionStepBuildBitmapQuery            LegacyExecutionCallStep = "build_bitmap_query"
	LegacyExecutionStepAdaptFilterExpression       LegacyExecutionCallStep = "adapt_filter_expression"
	LegacyExecutionStepQueryBitIndex               LegacyExecutionCallStep = "query_bit_index"
	LegacyExecutionStepAdaptBitmapResult           LegacyExecutionCallStep = "adapt_bitmap_result"
	LegacyExecutionStepBuildCandidateSet           LegacyExecutionCallStep = "build_candidate_set"
	LegacyExecutionStepBuildMaterializationRequest LegacyExecutionCallStep = "build_materialization_request"
	LegacyExecutionStepMaterializeProjection       LegacyExecutionCallStep = "materialize_projection"
	LegacyExecutionStepAdaptNativeAggregateResult  LegacyExecutionCallStep = "adapt_native_aggregate_result"
	LegacyExecutionStepApplySQLResultAssembly      LegacyExecutionCallStep = "apply_sql_result_assembly"
	LegacyExecutionStepReleaseSession              LegacyExecutionCallStep = "release_session"
)

// LegacyExecutionStepStatus describes how real a call-plan step is for a runtime implementation.
type LegacyExecutionStepStatus string

const (
	// LegacyExecutionStepStatusPlanned means the step is designed but not wired to a concrete adapter yet.
	LegacyExecutionStepStatusPlanned LegacyExecutionStepStatus = "planned"
	// LegacyExecutionStepStatusConcrete means the runtime path has a real adapter boundary for the step.
	LegacyExecutionStepStatusConcrete LegacyExecutionStepStatus = "concrete"
	// LegacyExecutionStepStatusFixture means a fixture runtime simulates the step deterministically.
	LegacyExecutionStepStatusFixture LegacyExecutionStepStatus = "fixture"
	// LegacyExecutionStepStatusPending means this implementation intentionally does not perform the step yet.
	LegacyExecutionStepStatusPending LegacyExecutionStepStatus = "pending"
)

// LegacyExecutionCallPlan summarizes how a neutral request maps to legacy Quanta calls.
type LegacyExecutionCallPlan struct {
	RootIndex            string
	Steps                []LegacyExecutionCallStep
	FragmentCount        int
	ProjectionCount      int
	SQLAggregateCount    int
	NativeAggregateCount int
	HasMaterialization   bool
	UsesSessionPool      bool
	Notes                []string
	StepStatuses         map[LegacyExecutionCallStep]LegacyExecutionStepStatus
}

// PlanLegacyExecutionCall describes the direct legacy runtime handoff without executing it.
func PlanLegacyExecutionCall(request ExecutionRequest) (LegacyExecutionCallPlan, qsbridge.DiagnosticSet) {
	rootIndex, ok := request.RootIndex()
	if !ok {
		return LegacyExecutionCallPlan{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInvalidExecutionOption,
				qsbridge.PhaseExecute,
				"legacy execution call plan requires a root index",
			),
		}
	}

	plan := LegacyExecutionCallPlan{
		RootIndex:            rootIndex,
		FragmentCount:        request.FragmentCount(),
		ProjectionCount:      request.ProjectionCount(),
		SQLAggregateCount:    len(request.SQLAggregates),
		NativeAggregateCount: request.AggregateCount(),
		UsesSessionPool:      true,
	}
	plan.Steps = append(plan.Steps,
		LegacyExecutionStepConstructSource,
		LegacyExecutionStepBorrowSession,
		LegacyExecutionStepBuildBitmapQuery,
	)
	if !request.Query.Filter.Empty() {
		plan.Steps = append(plan.Steps, LegacyExecutionStepAdaptFilterExpression)
		plan.Notes = append(plan.Notes, "grouped filter expressions require a runtime adapter before bitmap query execution")
	}
	plan.Steps = append(plan.Steps,
		LegacyExecutionStepQueryBitIndex,
		LegacyExecutionStepAdaptBitmapResult,
		LegacyExecutionStepBuildCandidateSet,
	)

	if request.ProjectionCount() > 0 || request.Materialization.ProjectionCount() > 0 {
		plan.HasMaterialization = true
		plan.Steps = append(plan.Steps,
			LegacyExecutionStepBuildMaterializationRequest,
			LegacyExecutionStepMaterializeProjection,
		)
	}
	if request.AggregateCount() > 0 {
		plan.Steps = append(plan.Steps, LegacyExecutionStepAdaptNativeAggregateResult)
		plan.Notes = append(plan.Notes, "native aggregate handoffs remain explicit runtime adapter calls")
	}
	if len(request.SQLAggregates) > 0 || len(request.OrderBy) > 0 || request.Result.Limit > 0 || request.Result.Offset > 0 {
		plan.Steps = append(plan.Steps, LegacyExecutionStepApplySQLResultAssembly)
		plan.Notes = append(plan.Notes, "SQL-visible aggregation, ordering, limit, and offset are assembled after bitmap execution")
	}
	plan.Steps = append(plan.Steps, LegacyExecutionStepReleaseSession)
	return plan.WithRuntimeProfile(LegacyDirectRuntimeProfile()), nil
}

// Contains reports whether the call plan includes a step.
func (p LegacyExecutionCallPlan) Contains(step LegacyExecutionCallStep) bool {
	for _, candidate := range p.Steps {
		if candidate == step {
			return true
		}
	}
	return false
}

// Summary returns a compact human-readable description for diagnostics and tests.
func (p LegacyExecutionCallPlan) Summary() string {
	return fmt.Sprintf(
		"root=%s fragments=%d projections=%d sql_aggregates=%d native_aggregates=%d materialization=%t steps=%d",
		p.RootIndex,
		p.FragmentCount,
		p.ProjectionCount,
		p.SQLAggregateCount,
		p.NativeAggregateCount,
		p.HasMaterialization,
		len(p.Steps),
	)
}

// StepStatus returns the implementation status for a call-plan step.
func (p LegacyExecutionCallPlan) StepStatus(step LegacyExecutionCallStep) LegacyExecutionStepStatus {
	if p.StepStatuses == nil {
		return LegacyExecutionStepStatusPlanned
	}
	if status := p.StepStatuses[step]; status != "" {
		return status
	}
	return LegacyExecutionStepStatusPlanned
}

// WithRuntimeProfile returns a copy with step statuses mapped for one runtime implementation.
func (p LegacyExecutionCallPlan) WithRuntimeProfile(profile RuntimeInspectionProfile) LegacyExecutionCallPlan {
	profile = profile.Effective()
	statuses := make(map[LegacyExecutionCallStep]LegacyExecutionStepStatus, len(p.Steps))
	for _, step := range p.Steps {
		statuses[step] = legacyExecutionStepStatus(profile, step)
	}
	p.StepStatuses = statuses
	return p
}

func legacyExecutionStepStatus(profile RuntimeInspectionProfile, step LegacyExecutionCallStep) LegacyExecutionStepStatus {
	if profile.Implementation == RuntimeImplementationFixture {
		switch step {
		case LegacyExecutionStepConstructSource, LegacyExecutionStepBorrowSession, LegacyExecutionStepReleaseSession:
			return LegacyExecutionStepStatusPending
		default:
			return LegacyExecutionStepStatusFixture
		}
	}
	switch step {
	case LegacyExecutionStepConstructSource,
		LegacyExecutionStepBorrowSession,
		LegacyExecutionStepBuildBitmapQuery,
		LegacyExecutionStepQueryBitIndex,
		LegacyExecutionStepAdaptBitmapResult,
		LegacyExecutionStepReleaseSession:
		return LegacyExecutionStepStatusConcrete
	default:
		return LegacyExecutionStepStatusPlanned
	}
}
