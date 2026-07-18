package qsbridge

// SubqueryHelperPlanKind identifies one executor-neutral helper-plan sketch.
type SubqueryHelperPlanKind string

const (
	// SubqueryHelperPlanScalarSubquery evaluates one scalar subquery value.
	SubqueryHelperPlanScalarSubquery SubqueryHelperPlanKind = "scalar_subquery"
	// SubqueryHelperPlanParentKeyLookup resolves parent keys for a correlated helper.
	SubqueryHelperPlanParentKeyLookup SubqueryHelperPlanKind = "parent_key_lookup"
	// SubqueryHelperPlanAggregateThresholdLookup computes aggregate thresholds keyed by correlated values.
	SubqueryHelperPlanAggregateThresholdLookup SubqueryHelperPlanKind = "aggregate_threshold_lookup"
	// SubqueryHelperPlanSiblingMembership evaluates correlated sibling-domain semi/anti membership.
	SubqueryHelperPlanSiblingMembership SubqueryHelperPlanKind = "sibling_membership"
)

// SubqueryHelperPlan is an executor-neutral sketch of helper work implied by
// subquery placeholders. It is intentionally not executable yet; it gives the
// planner and inspection layers a native target to grow toward.
type SubqueryHelperPlan struct {
	Name               string
	Kind               SubqueryHelperPlanKind
	SubqueryKind       SubqueryIntentKind
	Lifecycle          SubqueryStepLifecycle
	Inputs             []string
	Outputs            []string
	Materialization    string
	BitmapNativeTarget string
}

// SubqueryHelperPlanReport is a compact inspection view of a helper-plan sketch.
type SubqueryHelperPlanReport struct {
	Name               string
	Kind               SubqueryHelperPlanKind
	SubqueryKind       SubqueryIntentKind
	Lifecycle          SubqueryStepLifecycle
	Inputs             []string
	Outputs            []string
	Materialization    string
	BitmapNativeTarget string
	NativeStep         *NativeSubqueryStepReport
}

// Report returns a compact inspection view of a helper-plan sketch.
func (p SubqueryHelperPlan) Report() SubqueryHelperPlanReport {
	lifecycle := p.Lifecycle
	if lifecycle == "" {
		lifecycle = SubqueryStepCompatibility
	}
	var nativeStep *NativeSubqueryStepReport
	if step, ok := p.NativeStep(); ok {
		report := step.Report()
		nativeStep = &report
	}
	return SubqueryHelperPlanReport{
		Name:               p.Name,
		Kind:               p.Kind,
		SubqueryKind:       p.SubqueryKind,
		Lifecycle:          lifecycle,
		Inputs:             append([]string(nil), p.Inputs...),
		Outputs:            append([]string(nil), p.Outputs...),
		Materialization:    p.Materialization,
		BitmapNativeTarget: p.BitmapNativeTarget,
		NativeStep:         nativeStep,
	}
}

// LowerSubqueryHelperPlans extracts executor-neutral helper-plan sketches from
// subquery placeholder nodes in logical traversal order.
func LowerSubqueryHelperPlans(root LogicalNode) []SubqueryHelperPlan {
	plans := make([]SubqueryHelperPlan, 0)
	WalkLogicalPlan(root, func(node LogicalNode) bool {
		switch n := node.(type) {
		case ScalarSubqueryNode:
			for _, intent := range n.Intents {
				plans = append(plans, lowerSubqueryHelperIntentPlans(intent)...)
			}
		case CorrelatedAggregateSubqueryNode:
			for _, intent := range n.Intents {
				plans = append(plans, lowerSubqueryHelperIntentPlans(intent)...)
			}
		}
		return true
	})
	return plans
}

// SubqueryHelperPlanReports returns compact reports for all lowered helper-plan sketches.
func SubqueryHelperPlanReports(root LogicalNode) []SubqueryHelperPlanReport {
	return subqueryHelperPlanReports(LowerSubqueryHelperPlans(root))
}

func subqueryHelperPlanReportsForIntents(intents []SubqueryPlanIntent) []SubqueryHelperPlanReport {
	return subqueryHelperPlanReports(lowerSubqueryHelperPlansForIntents(intents))
}

func lowerSubqueryHelperPlansForIntents(intents []SubqueryPlanIntent) []SubqueryHelperPlan {
	plans := make([]SubqueryHelperPlan, 0)
	for _, intent := range intents {
		plans = append(plans, lowerSubqueryHelperIntentPlans(intent)...)
	}
	return plans
}

func subqueryHelperPlanReports(plans []SubqueryHelperPlan) []SubqueryHelperPlanReport {
	if len(plans) == 0 {
		return nil
	}
	reports := make([]SubqueryHelperPlanReport, 0, len(plans))
	for _, plan := range plans {
		reports = append(reports, plan.Report())
	}
	return reports
}

func lowerSubqueryHelperIntentPlans(intent SubqueryPlanIntent) []SubqueryHelperPlan {
	if len(intent.HelperIntents) == 0 {
		if fallback, ok := fallbackSubqueryHelperPlan(intent); ok {
			return []SubqueryHelperPlan{fallback}
		}
		return nil
	}
	plans := make([]SubqueryHelperPlan, 0, len(intent.HelperIntents))
	for _, helper := range intent.HelperIntents {
		kind := SubqueryHelperPlanKind(helper.Kind)
		plans = append(plans, SubqueryHelperPlan{
			Name:               helper.Name,
			Kind:               kind,
			SubqueryKind:       intent.Kind,
			Lifecycle:          defaultSubqueryHelperLifecycle(kind),
			Inputs:             append([]string(nil), helper.Inputs...),
			Outputs:            append([]string(nil), helper.Outputs...),
			Materialization:    helper.Materialization,
			BitmapNativeTarget: helper.BitmapNativeTarget,
		})
	}
	return plans
}

func fallbackSubqueryHelperPlan(intent SubqueryPlanIntent) (SubqueryHelperPlan, bool) {
	switch intent.Kind {
	case SubqueryIntentScalar:
		if intent.Scalar == nil || intent.Scalar.OutputName == "" {
			return SubqueryHelperPlan{}, false
		}
		return SubqueryHelperPlan{
			Name:               intent.Scalar.OutputName,
			Kind:               SubqueryHelperPlanScalarSubquery,
			SubqueryKind:       intent.Kind,
			Lifecycle:          SubqueryStepNativeReady,
			Outputs:            []string{intent.Scalar.OutputName},
			Materialization:    "one-row single-cell scalar",
			BitmapNativeTarget: "planner scalar expression input evaluated without SQL text replacement",
		}, true
	case SubqueryIntentCorrelatedAggregate:
		if intent.CorrelatedAggregate == nil {
			return SubqueryHelperPlan{}, false
		}
		innerKey := correlatedAggregateFieldName(intent.CorrelatedAggregate.InnerKeyRef, intent.CorrelatedAggregate.InnerKey)
		innerValue := correlatedAggregateFieldName(intent.CorrelatedAggregate.InnerValueRef, intent.CorrelatedAggregate.InnerValue)
		outerKey := correlatedAggregateFieldName(intent.CorrelatedAggregate.OuterKeyRef, intent.CorrelatedAggregate.OuterKey)
		outputs := []string{outerKey, "threshold"}
		return SubqueryHelperPlan{
			Name:               "correlated_aggregate_thresholds",
			Kind:               SubqueryHelperPlanAggregateThresholdLookup,
			SubqueryKind:       intent.Kind,
			Lifecycle:          SubqueryStepNativeReady,
			Inputs:             []string{innerKey, innerValue},
			Outputs:            outputs,
			Materialization:    "per-key aggregate threshold map",
			BitmapNativeTarget: "aggregate-threshold kernel feeding native correlated aggregate predicate thresholds",
		}, true
	case SubqueryIntentCorrelatedMembership:
		if intent.CorrelatedMembership == nil {
			return SubqueryHelperPlan{}, false
		}
		name := intent.CorrelatedMembership.OutputName
		if name == "" {
			name = "correlated_sibling_membership"
		}
		target := intent.CorrelatedMembership.BitmapNativeTarget
		if target == "" {
			target = "semi/anti membership over repeated table aliases using relationship-vector domains"
		}
		return SubqueryHelperPlan{
			Name:               name,
			Kind:               SubqueryHelperPlanSiblingMembership,
			SubqueryKind:       intent.Kind,
			Lifecycle:          SubqueryStepNativeReady,
			Inputs:             []string{intent.CorrelatedMembership.OuterKeyRef, intent.CorrelatedMembership.InnerKeyRef},
			Outputs:            []string{name},
			Materialization:    "outer rownum keep/drop domain",
			BitmapNativeTarget: target,
		}, true
	default:
		return SubqueryHelperPlan{}, false
	}
}

// NativeStep returns a first-class native step when a helper sketch has a
// stable executor-facing contract.
func (p SubqueryHelperPlan) NativeStep() (NativeSubqueryStep, bool) {
	var kind NativeSubqueryStepKind
	switch p.Kind {
	case SubqueryHelperPlanScalarSubquery:
		kind = NativeSubqueryStepScalarMaterialization
	case SubqueryHelperPlanParentKeyLookup:
		kind = NativeSubqueryStepParentKeyLookup
	case SubqueryHelperPlanAggregateThresholdLookup:
		kind = NativeSubqueryStepAggregateThresholdLookup
	case SubqueryHelperPlanSiblingMembership:
		kind = NativeSubqueryStepSiblingMembership
	default:
		return NativeSubqueryStep{}, false
	}
	if p.Name == "" {
		return NativeSubqueryStep{}, false
	}
	lifecycle := p.Lifecycle
	if lifecycle == "" || lifecycle == SubqueryStepCompatibility {
		lifecycle = SubqueryStepNativeReady
	}
	return NativeSubqueryStep{
		Name:               p.Name,
		Kind:               kind,
		Lifecycle:          lifecycle,
		SubqueryKind:       p.SubqueryKind,
		Inputs:             append([]string(nil), p.Inputs...),
		Outputs:            append([]string(nil), p.Outputs...),
		Materialization:    p.Materialization,
		BitmapNativeTarget: p.BitmapNativeTarget,
		ExecutionMode:      "sql_backed_until_bitmap_native_executor_exists",
	}, true
}

func defaultSubqueryHelperLifecycle(kind SubqueryHelperPlanKind) SubqueryStepLifecycle {
	switch kind {
	case SubqueryHelperPlanScalarSubquery,
		SubqueryHelperPlanParentKeyLookup,
		SubqueryHelperPlanAggregateThresholdLookup,
		SubqueryHelperPlanSiblingMembership:
		return SubqueryStepNativeReady
	default:
		return SubqueryStepCompatibility
	}
}

func cloneSubqueryHelperPlanReports(reports []SubqueryHelperPlanReport) []SubqueryHelperPlanReport {
	if len(reports) == 0 {
		return nil
	}
	cloned := make([]SubqueryHelperPlanReport, 0, len(reports))
	for _, report := range reports {
		report.Inputs = append([]string(nil), report.Inputs...)
		report.Outputs = append([]string(nil), report.Outputs...)
		if report.NativeStep != nil {
			nativeStep := *report.NativeStep
			nativeStep.Inputs = append([]string(nil), report.NativeStep.Inputs...)
			nativeStep.Outputs = append([]string(nil), report.NativeStep.Outputs...)
			report.NativeStep = &nativeStep
		}
		cloned = append(cloned, report)
	}
	return cloned
}
