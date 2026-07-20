package qsruntime

import (
	"context"
	"strconv"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func (r DirectBitmapRuntime) directBitmapApplySameRowResiduals(ctx context.Context, request ExecutionRequest, bitmapResult BitmapQueryResult) (BitmapQueryResult, []ExecutionProbe, qsbridge.DiagnosticSet, error, bool) {
	plans, ok := directBitmapSameRowResidualPlans(request)
	if !ok || len(plans) == 0 {
		return bitmapResult, nil, nil, nil, false
	}
	probes := []ExecutionProbe{{
		Section: "direct_bitmap_same_row",
		Name:    "plan_count",
		Value:   strconv.Itoa(len(plans)),
	}}
	current := append([]qsbridge.QuantaRownum(nil), bitmapResult.Rownums...)
	for _, plan := range plans {
		prefix := plan.ProbeName + "_"
		decision := sameRowComparisonPolicy(plan, len(current))
		probes = append(probes,
			ExecutionProbe{Section: "direct_bitmap_same_row", Name: prefix + "policy", Value: sameRowComparisonPolicyName(decision)},
			ExecutionProbe{Section: "direct_bitmap_same_row", Name: prefix + "policy_reason", Value: decision.reason},
		)
		if !decision.useNative {
			continue
		}
		comparisonRequest := plan.Request(current)
		comparisonRequest.FromEpochMillis = request.Materialization.FromEpochMillis
		comparisonRequest.ToEpochMillis = request.Materialization.ToEpochMillis
		comparison, err := r.sameRowComparisonKernel().CompareSameRowFields(ctx, comparisonRequest)
		probes = append(probes, comparison.Probes...)
		if err != nil || comparison.Diagnostics.BlocksNative() {
			return bitmapResult, probes, comparison.Diagnostics, err, true
		}
		current = append([]qsbridge.QuantaRownum(nil), comparison.Domain.Rownums...)
	}
	bitmapResult.Rownums = current
	bitmapResult.Count = uint64(len(current))
	return bitmapResult, probes, nil, nil, true
}

func directBitmapSameRowResidualPlans(request ExecutionRequest) ([]qsbridge.SameRowComparisonPlan, bool) {
	if len(request.Joins) > 0 {
		return nil, false
	}
	residuals := make([]qsbridge.Predicate, 0, len(request.Predicates))
	for _, predicate := range request.Predicates {
		if predicate.Placement == qsbridge.PredicateResidualScan {
			residuals = append(residuals, predicate)
		}
	}
	if len(residuals) == 0 {
		return nil, false
	}
	plans := make([]qsbridge.SameRowComparisonPlan, 0, len(residuals))
	for i, predicate := range residuals {
		left, right, op, ok := qsbridge.SameRowBSIComparisonPredicate(predicate)
		if !ok {
			return nil, false
		}
		plans = append(plans, qsbridge.SameRowComparisonPlan{
			ID:             "direct_bitmap_same_row_" + strconv.Itoa(i+1),
			ProbeName:      "direct_bitmap_same_row_" + strconv.Itoa(i+1),
			Left:           left,
			Right:          right,
			Operator:       op,
			Kind:           qsbridge.SameRowComparisonBSI,
			Domain:         qsbridge.RownumDomain{Table: left.Table, Role: qsbridge.TableInstanceID(materializationFieldRole(left.Table.Table, left))},
			PredicateScope: qsbridge.PredicateScopeWhere,
			PredicateIndex: i,
		})
	}
	return plans, true
}

func directBitmapWithoutSameRowResidualPredicates(request ExecutionRequest) ExecutionRequest {
	predicates := make([]qsbridge.Predicate, 0, len(request.Predicates))
	for _, predicate := range request.Predicates {
		if predicate.Placement != qsbridge.PredicateResidualScan {
			predicates = append(predicates, predicate)
			continue
		}
		if _, _, _, ok := qsbridge.SameRowBSIComparisonPredicate(predicate); !ok {
			predicates = append(predicates, predicate)
		}
	}
	request.Predicates = predicates
	request.Materialization.ProjectionFields = materializationFieldsFromExecutionRequest(request)
	return request
}
