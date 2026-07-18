package qsruntime

import "github.com/QuantaStream/quantastream/qsbridge"

// PreflightRewriteInventory records one temporary preflight transform that
// should eventually become a first-class parser, IR, planner, or executor
// capability.
type PreflightRewriteInventory struct {
	Rule                qsbridge.RewriteRuleID
	Reason              string
	SourceSQLShape      string
	TemporaryStrategy   string
	HelperPlanKinds     []PreflightRewriteHelperPlanKind
	RegressionCoverage  []string
	FutureIRReplacement string
}

// NativePromotionCandidate records the next helper shape that should move from
// compatibility scratchpad work into a native-step contract.
type NativePromotionCandidate struct {
	HelperKind PreflightRewriteHelperPlanKind
	Rule       qsbridge.RewriteRuleID
	Reason     string
	Follows    qsbridge.NativeSubqueryStepKind
}

// PreflightSurfaceDisposition classifies helper-shaped preflight code by its
// long-term destination.
type PreflightSurfaceDisposition string

const (
	// PreflightSurfaceTypedNativeStep marks helper-shaped code that already
	// carries a stable native subquery step contract.
	PreflightSurfaceTypedNativeStep PreflightSurfaceDisposition = "typed_native_step"
	// PreflightSurfaceTemporaryTransform marks preflight work that should be
	// deleted when the planner/executor owns the shape directly.
	PreflightSurfaceTemporaryTransform PreflightSurfaceDisposition = "temporary_transform"
	// PreflightSurfaceCompatibilityFallback marks SQL-backed fallback code that
	// remains compatibility debt.
	PreflightSurfaceCompatibilityFallback PreflightSurfaceDisposition = "compatibility_fallback"
)

// PreflightSurfaceInventory records whether a helper-shaped surface is real
// remaining compatibility debt or native machinery carrying old helper names.
type PreflightSurfaceInventory struct {
	Name              string
	Disposition       PreflightSurfaceDisposition
	Rule              qsbridge.RewriteRuleID
	HelperKind        PreflightRewriteHelperPlanKind
	NativeStepKind    qsbridge.NativeSubqueryStepKind
	CurrentContract   string
	DeletionCondition string
}

func preflightRewriteInventory() []PreflightRewriteInventory {
	return nil
}

func preflightSurfaceInventory() []PreflightSurfaceInventory {
	return []PreflightSurfaceInventory{
		{
			Name:              "scalar_subquery_materialization",
			Disposition:       PreflightSurfaceTypedNativeStep,
			HelperKind:        PreflightHelperPlanScalarSubquery,
			NativeStepKind:    qsbridge.NativeSubqueryStepScalarMaterialization,
			CurrentContract:   "typed scalar subquery materialization through NativeSubqueryStepExecutionRequest",
			DeletionCondition: "rename helper-shaped request/report wrappers after scalar materialization is owned directly by the planner/executor pipeline",
		},
		{
			Name:              "parent_key_lookup",
			Disposition:       PreflightSurfaceTypedNativeStep,
			Rule:              qsbridge.RewriteCorrelatedAggregatePreflight,
			HelperKind:        PreflightHelperPlanParentKeyLookup,
			NativeStepKind:    qsbridge.NativeSubqueryStepParentKeyLookup,
			CurrentContract:   "typed parent-key lookup feeding correlated aggregate threshold work",
			DeletionCondition: "rename helper-shaped request/report wrappers after correlated aggregate planning consumes NativeSubqueryStep directly",
		},
		{
			Name:              "aggregate_threshold_lookup",
			Disposition:       PreflightSurfaceTypedNativeStep,
			Rule:              qsbridge.RewriteCorrelatedAggregatePreflight,
			HelperKind:        PreflightHelperPlanAggregateThresholdLookup,
			NativeStepKind:    qsbridge.NativeSubqueryStepAggregateThresholdLookup,
			CurrentContract:   "typed aggregate-threshold lookup feeding native correlated aggregate predicate thresholds",
			DeletionCondition: "replace preflight orchestration with planner-owned aggregate-threshold execution",
		},
		{
			Name:              "correlated_aggregate_preflight_transform",
			Disposition:       PreflightSurfaceTemporaryTransform,
			Rule:              qsbridge.RewriteCorrelatedAggregatePreflight,
			CurrentContract:   "temporary typed transform that consumes Q17-style correlated aggregate intent and attaches native predicate metadata",
			DeletionCondition: "delete when correlated aggregate subqueries are represented and executed as native planner nodes",
		},
		{
			Name:              "sql_backed_preflight_helper_executor",
			Disposition:       PreflightSurfaceCompatibilityFallback,
			CurrentContract:   "fallback executor that routes helper SQL through SQLRuntime only when no native step is available or the default adapter cannot build a native runtime request yet",
			DeletionCondition: "delete when scalar, parent-key, aggregate-threshold, and sibling-membership paths all have required native executors and the default adapter no longer needs SQL fallback",
		},
	}
}

func nextPreflightNativePromotionCandidate() NativePromotionCandidate {
	return NativePromotionCandidate{}
}

func preflightRewriteInventoryByRule() map[qsbridge.RewriteRuleID]PreflightRewriteInventory {
	inventory := make(map[qsbridge.RewriteRuleID]PreflightRewriteInventory)
	for _, item := range preflightRewriteInventory() {
		inventory[item.Rule] = item
	}
	return inventory
}
