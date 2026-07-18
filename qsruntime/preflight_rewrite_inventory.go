package qsruntime

import "github.com/QuantaStream/quantastream/qsbridge"

// PreflightRewriteInventory records one temporary SQL-to-SQL rewrite that
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

func preflightRewriteInventory() []PreflightRewriteInventory {
	return []PreflightRewriteInventory{
		{
			Rule:              qsbridge.RewriteCorrelatedAggregatePreflight,
			Reason:            "TPC-H-style correlated aggregate predicates are useful read-path shapes, but correlated aggregate subquery intent is not planner-native yet.",
			SourceSQLShape:    "A correlated average quantity predicate such as l_quantity < (select factor * avg(l2.l_quantity) from lineitem l2 where l2.l_partkey = p.p_partkey).",
			TemporaryStrategy: "Route parent-key lookup and per-key aggregate-threshold materialization through native subquery step contracts, execute them with SQL-backed adapters for now, then expand the predicate into equivalent per-key threshold branches before native planning.",
			HelperPlanKinds: []PreflightRewriteHelperPlanKind{
				PreflightHelperPlanParentKeyLookup,
				PreflightHelperPlanAggregateThresholdLookup,
			},
			RegressionCoverage: []string{
				"qsruntime/sql_runtime_test.go correlated aggregate preflight trace and ExecuteSQL coverage",
				"inabox-direct TPCH Q17-style probes when present in SQLRunner suites",
			},
			FutureIRReplacement: "Represent correlated aggregate subqueries as typed IR nodes, then lower them through planner-owned aggregate-threshold and semi-join kernels instead of rewriting SQL text.",
		},
	}
}

func nextPreflightNativePromotionCandidate() NativePromotionCandidate {
	return NativePromotionCandidate{
		HelperKind: PreflightHelperPlanAggregateThresholdLookup,
		Rule:       qsbridge.RewriteCorrelatedAggregatePreflight,
		Reason:     "parent-key lookup now has a native-step contract, so aggregate-threshold lookup is the next execution shape to make bitmap-native",
		Follows:    qsbridge.NativeSubqueryStepParentKeyLookup,
	}
}

func preflightRewriteInventoryByRule() map[qsbridge.RewriteRuleID]PreflightRewriteInventory {
	inventory := make(map[qsbridge.RewriteRuleID]PreflightRewriteInventory)
	for _, item := range preflightRewriteInventory() {
		inventory[item.Rule] = item
	}
	return inventory
}
