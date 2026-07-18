package qsbridge

import "strings"

// RewriteRuleID identifies one optimizer rewrite or advisory rule.
type RewriteRuleID string

const (
	// RewritePredicatePushdown records predicate movement toward native scans.
	RewritePredicatePushdown RewriteRuleID = "predicate_pushdown"
	// RewriteHiddenProjection records planner-added fields required by later stages.
	RewriteHiddenProjection RewriteRuleID = "hidden_projection"
	// RewriteMembershipToDifference records anti-membership lowering to bitmap difference.
	RewriteMembershipToDifference RewriteRuleID = "membership_to_difference"
	// RewriteJoinReorder records join order changes.
	RewriteJoinReorder RewriteRuleID = "join_reorder"
	// RewriteOuterJoinBoundary records blocked rewrites across null-extension boundaries.
	RewriteOuterJoinBoundary RewriteRuleID = "outer_join_boundary"
	// RewriteTopNGroupedCount records grouped count queries that could lower to native topn().
	RewriteTopNGroupedCount RewriteRuleID = "topn_grouped_count"
	// RewriteScalarSubqueryPreflight records scalar subquery evaluation before native planning.
	RewriteScalarSubqueryPreflight RewriteRuleID = "scalar_subquery_preflight"
	// RewriteCorrelatedAggregatePreflight records correlated aggregate preflight expansion before native planning.
	RewriteCorrelatedAggregatePreflight RewriteRuleID = "correlated_aggregate_preflight"
	// RewriteCorrelatedAggregateNativePredicate records correlated aggregate lowering into executor-owned native predicate metadata.
	RewriteCorrelatedAggregateNativePredicate RewriteRuleID = "correlated_aggregate_native_predicate"
	// RewriteFullTableScan records scan shapes with no source-row filtering predicate.
	RewriteFullTableScan RewriteRuleID = "full_table_scan"
	// RewriteRelationshipVectorStrategy records relation-vector execution strategy opportunities.
	RewriteRelationshipVectorStrategy RewriteRuleID = "relationship_vector_strategy"
	// RewriteShardTimeWindowAwareness records predicates that may align with shard-window pruning.
	RewriteShardTimeWindowAwareness RewriteRuleID = "shard_time_window_awareness"
)

// RewriteCategory identifies the optimizer decision family.
type RewriteCategory string

const (
	// RewriteCategoryUnknown means the decision family has not been classified.
	RewriteCategoryUnknown RewriteCategory = ""
	// RewriteCategoryCompatibility records rewrites needed to reach a supported native shape.
	RewriteCategoryCompatibility RewriteCategory = "compatibility"
	// RewriteCategoryPerformance records tuning or cost-oriented optimizer decisions.
	RewriteCategoryPerformance RewriteCategory = "performance"
	// RewriteCategoryPhysical records placement, routing, or executor-shape decisions.
	RewriteCategoryPhysical RewriteCategory = "physical"
	// RewriteCategorySafety records blocked rewrites that preserve SQL semantics.
	RewriteCategorySafety RewriteCategory = "safety"
)

// RewriteImpact describes what changed, or would change, because of one decision.
type RewriteImpact string

const (
	// RewriteImpactUnknown means the decision impact has not been classified.
	RewriteImpactUnknown RewriteImpact = ""
	// RewriteImpactNone means the optimizer made no query or execution shape change.
	RewriteImpactNone RewriteImpact = "none"
	// RewriteImpactLogicalShape means the logical query shape changed.
	RewriteImpactLogicalShape RewriteImpact = "logical_shape"
	// RewriteImpactPhysicalShape means the physical execution shape changed.
	RewriteImpactPhysicalShape RewriteImpact = "physical_shape"
	// RewriteImpactDiagnosticsOnly means only explain, warning, or advisory metadata changed.
	RewriteImpactDiagnosticsOnly RewriteImpact = "diagnostics_only"
)

// RewriteStatus records whether an optimizer rule changed the query shape.
type RewriteStatus string

const (
	// RewriteApplied means a rule produced an equivalent transformed shape.
	RewriteApplied RewriteStatus = "applied"
	// RewriteAdvisory means a rule emitted tuning guidance without changing shape.
	RewriteAdvisory RewriteStatus = "advisory"
	// RewriteBlocked means a candidate rewrite was intentionally not applied.
	RewriteBlocked RewriteStatus = "blocked"
	// RewriteSkipped means a rule did not apply to this query.
	RewriteSkipped RewriteStatus = "skipped"
)

// RewriteRecord is a stable audit record for one optimizer decision.
type RewriteRecord struct {
	Rule         RewriteRuleID
	Status       RewriteStatus
	Category     RewriteCategory
	Impact       RewriteImpact
	Reason       string
	Before       string
	After        string
	Capabilities []PlanCapability
	Diagnostics  DiagnosticSet
	Fields       []FieldRef
}

// BlocksNative reports whether the rewrite decision contains blocking diagnostics.
func (r RewriteRecord) BlocksNative() bool {
	if r.Status == RewriteBlocked && r.Diagnostics.BlocksNative() {
		return true
	}
	return r.Diagnostics.BlocksNative()
}

// OptimizationTrace records optimizer decisions without hiding them in planner behavior.
type OptimizationTrace struct {
	Supported   bool
	Rewrites    []RewriteRecord
	Diagnostics DiagnosticSet
}

// OptimizationSummary is a compact aggregate view of an optimization trace.
type OptimizationSummary struct {
	Supported      bool
	Total          int
	Applied        int
	Advisory       int
	Blocked        int
	Skipped        int
	Diagnostics    int
	Blocking       int
	Compatibility  int
	Performance    int
	Physical       int
	Safety         int
	LogicalImpact  int
	PhysicalImpact int
	DiagnosticOnly int
	NoImpact       int
}

// NewOptimizationTrace creates an empty supported optimization trace.
func NewOptimizationTrace() OptimizationTrace {
	return OptimizationTrace{Supported: true}
}

// Add appends one optimizer decision and updates aggregate diagnostics.
func (t *OptimizationTrace) Add(record RewriteRecord) {
	if t == nil {
		return
	}
	t.Rewrites = append(t.Rewrites, record.Clone())
	if len(record.Diagnostics) > 0 {
		t.Diagnostics = append(t.Diagnostics, record.Diagnostics...)
	}
	if t.Supported == false {
		return
	}
	if record.BlocksNative() {
		t.Supported = false
	}
}

// Clone returns a deep copy of the optimization trace.
func (t OptimizationTrace) Clone() OptimizationTrace {
	copied := OptimizationTrace{
		Supported:   t.Supported,
		Rewrites:    make([]RewriteRecord, 0, len(t.Rewrites)),
		Diagnostics: append(DiagnosticSet(nil), t.Diagnostics...),
	}
	for _, rewrite := range t.Rewrites {
		copied.Rewrites = append(copied.Rewrites, rewrite.Clone())
	}
	return copied
}

// Applied returns rewrite records with applied status.
func (t OptimizationTrace) Applied() []RewriteRecord {
	return t.withStatus(RewriteApplied)
}

// Blocked returns rewrite records with blocked status.
func (t OptimizationTrace) Blocked() []RewriteRecord {
	return t.withStatus(RewriteBlocked)
}

// Advisories returns rewrite records with advisory status.
func (t OptimizationTrace) Advisories() []RewriteRecord {
	return t.withStatus(RewriteAdvisory)
}

// Summary returns aggregate optimizer decision counts.
func (t OptimizationTrace) Summary() OptimizationSummary {
	summary := OptimizationSummary{
		Supported:   t.Supported && !t.Diagnostics.BlocksNative(),
		Total:       len(t.Rewrites),
		Diagnostics: len(t.Diagnostics),
	}
	for _, diagnostic := range t.Diagnostics {
		if diagnostic.BlocksNative() {
			summary.Blocking++
		}
	}
	for _, rewrite := range t.Rewrites {
		switch rewrite.Status {
		case RewriteApplied:
			summary.Applied++
		case RewriteAdvisory:
			summary.Advisory++
		case RewriteBlocked:
			summary.Blocked++
		case RewriteSkipped:
			summary.Skipped++
		}
		switch rewrite.Category {
		case RewriteCategoryCompatibility:
			summary.Compatibility++
		case RewriteCategoryPerformance:
			summary.Performance++
		case RewriteCategoryPhysical:
			summary.Physical++
		case RewriteCategorySafety:
			summary.Safety++
		}
		switch rewrite.Impact {
		case RewriteImpactLogicalShape:
			summary.LogicalImpact++
		case RewriteImpactPhysicalShape:
			summary.PhysicalImpact++
		case RewriteImpactDiagnosticsOnly:
			summary.DiagnosticOnly++
		case RewriteImpactNone:
			summary.NoImpact++
		}
	}
	return summary
}

func (t OptimizationTrace) withStatus(status RewriteStatus) []RewriteRecord {
	records := make([]RewriteRecord, 0)
	for _, rewrite := range t.Rewrites {
		if rewrite.Status == status {
			records = append(records, rewrite.Clone())
		}
	}
	return records
}

// Clone returns a deep copy of the rewrite record.
func (r RewriteRecord) Clone() RewriteRecord {
	return RewriteRecord{
		Rule:         r.Rule,
		Status:       r.Status,
		Category:     r.Category,
		Impact:       r.Impact,
		Reason:       r.Reason,
		Before:       r.Before,
		After:        r.After,
		Capabilities: append([]PlanCapability(nil), r.Capabilities...),
		Diagnostics:  append(DiagnosticSet(nil), r.Diagnostics...),
		Fields:       append([]FieldRef(nil), r.Fields...),
	}
}

// RewriteAppliedRecord creates an applied rewrite audit record.
func RewriteAppliedRecord(rule RewriteRuleID, reason string, before string, after string) RewriteRecord {
	return RewriteRecord{
		Rule:     rule,
		Status:   RewriteApplied,
		Category: RewriteCategoryCompatibility,
		Impact:   RewriteImpactLogicalShape,
		Reason:   reason,
		Before:   before,
		After:    after,
	}
}

// RewriteAdvisoryRecord creates a tuning advisory audit record.
func RewriteAdvisoryRecord(rule RewriteRuleID, reason string, fields ...FieldRef) RewriteRecord {
	return RewriteRecord{
		Rule:     rule,
		Status:   RewriteAdvisory,
		Category: RewriteCategoryPerformance,
		Impact:   RewriteImpactDiagnosticsOnly,
		Reason:   reason,
		Fields:   append([]FieldRef(nil), fields...),
	}
}

// RewriteBlockedRecord creates a blocked rewrite audit record.
func RewriteBlockedRecord(rule RewriteRuleID, reason string, diagnostics DiagnosticSet, fields ...FieldRef) RewriteRecord {
	return RewriteRecord{
		Rule:        rule,
		Status:      RewriteBlocked,
		Category:    RewriteCategorySafety,
		Impact:      RewriteImpactNone,
		Reason:      reason,
		Diagnostics: append(DiagnosticSet(nil), diagnostics...),
		Fields:      append([]FieldRef(nil), fields...),
	}
}

// AnalyzeOptimizationCandidates returns advisory optimizer records for recognized query shapes.
//
// It does not rewrite the query. The trace is intentionally diagnostics-only so
// callers can validate optimizer detection before any physical lowering exists.
func AnalyzeOptimizationCandidates(query QueryIR) OptimizationTrace {
	trace := NewOptimizationTrace()
	if record, ok := topNGroupedCountAdvisory(query); ok {
		trace.Add(record)
	}
	if record, ok := fullTableScanAdvisory(query); ok {
		trace.Add(record)
	}
	for _, record := range relationshipVectorStrategyAdvisories(query) {
		trace.Add(record)
	}
	if record, ok := shardTimeWindowAdvisory(query); ok {
		trace.Add(record)
	}
	return trace
}

func mergeOptimizationTraces(primary OptimizationTrace, secondary OptimizationTrace) OptimizationTrace {
	merged := primary.Clone()
	if optimizationTraceZero(merged) {
		merged = NewOptimizationTrace()
	}
	for _, rewrite := range secondary.Rewrites {
		merged.Add(rewrite)
	}
	if len(secondary.Diagnostics) > 0 {
		merged.Diagnostics = mergeDiagnosticSets(merged.Diagnostics, secondary.Diagnostics)
		if merged.Diagnostics.BlocksNative() {
			merged.Supported = false
		}
	}
	return merged
}

func optimizationTraceZero(trace OptimizationTrace) bool {
	return !trace.Supported && len(trace.Rewrites) == 0 && len(trace.Diagnostics) == 0
}

func topNGroupedCountAdvisory(query QueryIR) (RewriteRecord, bool) {
	if query.Kind != QueryKindSelect || len(query.GroupBy) != 1 || len(query.OrderBy) != 1 || query.Result.Limit <= 0 {
		return RewriteRecord{}, false
	}
	groupField, ok := exprFieldRef(query.GroupBy[0])
	if !ok {
		return RewriteRecord{}, false
	}
	sortRef, ok := exprAggregateRef(query.OrderBy[0].Expr)
	if !ok || query.OrderBy[0].Direction != SortDescending || sortRef.Index < 0 || sortRef.Index >= len(query.Aggregates) {
		return RewriteRecord{}, false
	}
	aggregate := query.Aggregates[sortRef.Index]
	if !isCountAllAggregate(aggregate) {
		return RewriteRecord{}, false
	}
	record := RewriteAdvisoryRecord(
		RewriteTopNGroupedCount,
		"grouped count ordered by descending count with LIMIT can be lowered to native topn() when the physical representation supports it",
		groupField,
	)
	record.Before = "group_by(field)+count(*)+sort(count desc)+limit"
	record.After = "native_topn(field)"
	record.Capabilities = []PlanCapability{CapabilityNativeTopN}
	return record, true
}

func isCountAllAggregate(aggregate Aggregate) bool {
	return aggregate.Input == nil && aggregate.Filter == nil && aggregate.Mode != AggregateDistinct && strings.EqualFold(aggregate.Function, "count")
}

func exprFieldRef(expr Expr) (FieldRef, bool) {
	switch typed := expr.(type) {
	case FieldExpr:
		return typed.Ref, true
	case *FieldExpr:
		if typed == nil {
			return FieldRef{}, false
		}
		return typed.Ref, true
	default:
		return FieldRef{}, false
	}
}

func exprAggregateRef(expr Expr) (AggregateRefExpr, bool) {
	switch typed := expr.(type) {
	case AggregateRefExpr:
		return typed, true
	case *AggregateRefExpr:
		if typed == nil {
			return AggregateRefExpr{}, false
		}
		return *typed, true
	default:
		return AggregateRefExpr{}, false
	}
}

func fullTableScanAdvisory(query QueryIR) (RewriteRecord, bool) {
	scan := summarizeScan(query)
	if !scan.FullTable {
		return RewriteRecord{}, false
	}
	record := RewriteAdvisoryRecord(
		RewriteFullTableScan,
		"query has no source-row filtering predicate; session policy may reject this shape when full scans are disabled",
	)
	record.Category = RewriteCategorySafety
	record.Before = "unfiltered_source_scan"
	record.After = "allow_full_table_scan_policy_gate"
	return record, true
}

func relationshipVectorStrategyAdvisories(query QueryIR) []RewriteRecord {
	records := make([]RewriteRecord, 0)
	for _, edge := range query.Joins {
		if !relationshipJoinNeedsVectorExecution(edge) {
			continue
		}
		record := RewriteAdvisoryRecord(
			RewriteRelationshipVectorStrategy,
			"join edge can use relationship-vector reduction instead of materialized equality comparison",
			edge.Left,
			edge.Right,
		)
		record.Category = RewriteCategoryPhysical
		record.Before = "join_equality_materialization"
		record.After = "relationship_vector_reduction"
		record.Capabilities = append(record.Capabilities, RelationshipPlanCapabilities(edge.Encoding)...)
		records = append(records, record)
	}
	return records
}

func shardTimeWindowAdvisory(query QueryIR) (RewriteRecord, bool) {
	window := summarizeShardWindow(query)
	if window.CandidatePredicates == 0 {
		return RewriteRecord{}, false
	}
	record := RewriteAdvisoryRecord(
		RewriteShardTimeWindowAwareness,
		"time predicates can inform shard-window pruning and found-set-preserving relationship follow-up retrieval",
		timeWindowAdvisoryFields(query)...,
	)
	record.Category = RewriteCategoryPhysical
	record.Before = "generic_predicate_scan"
	record.After = "shard_window_aware_scan"
	return record, true
}

func timeWindowAdvisoryFields(query QueryIR) []FieldRef {
	collector := newFieldCollector()
	addPredicate := func(predicate Predicate) {
		for _, field := range FieldRefs(predicate.Expr) {
			if field.Type == DataTypeTime || field.Index == IndexDateTime {
				collector.addField(field)
			}
		}
	}
	for _, predicate := range query.Predicates {
		addPredicate(predicate)
	}
	for _, predicate := range query.Having {
		addPredicate(predicate)
	}
	for _, predicate := range query.Mutation.Predicates {
		addPredicate(predicate)
	}
	for _, edge := range query.Joins {
		for _, predicate := range edge.On {
			addPredicate(predicate)
		}
	}
	for _, edge := range query.Memberships {
		for _, predicate := range edge.Predicates {
			addPredicate(predicate)
		}
	}
	return collector.refs
}
