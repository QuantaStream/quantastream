package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// NativeSubqueryPreparationSummary records native subquery work performed after
// SQL binding and before Quanta intermediate lowering.
type NativeSubqueryPreparationSummary struct {
	CorrelatedAggregates int
	NativePredicates     NativePredicateSet
	HelperReports        []PreflightHelperExecutionRequestReport
	Optimization         qsbridge.OptimizationTrace
	Diagnostics          qsbridge.DiagnosticSet
}

// Empty reports whether no native subquery preparation work was applied.
func (s NativeSubqueryPreparationSummary) Empty() bool {
	return s.CorrelatedAggregates == 0 && s.NativePredicates.Empty() && len(s.HelperReports) == 0 && len(s.Diagnostics) == 0
}

// HelperExecutionReports returns helper/native-step reports produced while
// preparing native subquery state.
func (s NativeSubqueryPreparationSummary) HelperExecutionReports() []PreflightHelperExecutionRequestReport {
	if len(s.HelperReports) == 0 {
		return nil
	}
	reports := make([]PreflightHelperExecutionRequestReport, 0, len(s.HelperReports))
	for _, report := range s.HelperReports {
		reports = append(reports, report)
	}
	return reports
}

func (r SQLRuntime) materializeCorrelatedAggregatePredicates(ctx context.Context, request qsbridge.ExecutionRequest, values ...qsbridge.ParameterValue) (qsbridge.ExecutionRequest, NativeSubqueryPreparationSummary, qsbridge.DiagnosticSet, error) {
	summary := NativeSubqueryPreparationSummary{Optimization: qsbridge.NewOptimizationTrace()}
	query := request.Bound.Prepared.Query
	if len(query.Subqueries) == 0 {
		return request, summary, nil, nil
	}

	remaining := make([]qsbridge.SubqueryPlanIntent, 0, len(query.Subqueries))
	changed := false
	for _, intent := range query.Subqueries {
		if intent.Kind != qsbridge.SubqueryIntentCorrelatedAggregate || intent.CorrelatedAggregate == nil {
			remaining = append(remaining, intent)
			continue
		}
		match, ok := correlatedAverageQuantityMatchFromIntent(request.Bound.Prepared.SQL, query, intent)
		if !ok {
			remaining = append(remaining, intent)
			continue
		}
		predicate, reports, optimization, diagnostics, err := r.correlatedAverageNativePredicateForMatch(ctx, match, request.Options, values...)
		summary.HelperReports = append(summary.HelperReports, reports...)
		summary.Diagnostics = append(summary.Diagnostics, diagnostics...)
		summary.Optimization = mergeRuntimeOptimizationTrace(summary.Optimization, optimization)
		if err != nil || diagnostics.BlocksNative() {
			return request, summary, diagnostics, err
		}
		summary.CorrelatedAggregates++
		summary.NativePredicates.CorrelatedAggregate = append(summary.NativePredicates.CorrelatedAggregate, predicate)
		changed = true
	}
	if !changed {
		return request, summary, nil, nil
	}
	query.Subqueries = remaining
	request = applyNativeSubqueryPlanningState(request, query, summary.Optimization)
	return request, summary, nil, nil
}
