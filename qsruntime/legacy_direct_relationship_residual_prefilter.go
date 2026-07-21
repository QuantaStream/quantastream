package qsruntime

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type legacyDirectRelationshipResidualRolePrefilter struct {
	role             string
	table            string
	predicates       []qsbridge.Predicate
	predicateIndexes []int
	fields           []qsbridge.QuantaProjectionField
}

type legacyDirectRelationshipResidualRolePrefilterTiming struct {
	materializationElapsed time.Duration
	evaluationElapsed      time.Duration
	evaluatedRows          int
}

type legacyDirectRelationshipResidualPlacementObservation struct {
	ordinal      int
	role         string
	table        string
	fields       int
	placement    string
	rowsBefore   int
	rowsAfter    int
	reducedRows  int
	recommended  string
	reason       string
	reductionPct int
}

type legacyDirectRelationshipTupleResidualClassification struct {
	nativeSameRow       []qsbridge.SameRowComparisonPlan
	materialized        []qsbridge.Predicate
	materializedIndexes map[int]bool
	unsupported         []qsbridge.Predicate
}

type sameRowComparisonPolicyDecision struct {
	useNative bool
	reason    string
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipApplyResidualRolePrefilters(ctx context.Context, request ExecutionRequest, rowsByRole map[string][]qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge) ([]ExecutionProbe, []int, qsbridge.DiagnosticSet, error) {
	plans := legacyDirectRelationshipResidualRolePrefilterPlans(request)
	probes := []ExecutionProbe{
		legacyDirectRelationshipProbe("residual_prefilter_roles", strconv.Itoa(len(plans))),
	}
	if len(plans) == 0 {
		return probes, nil, nil, nil
	}
	appliedPredicateIndexes := make([]int, 0, len(plans))
	materialization := e.projectionMaterializationKernel()
	for i, plan := range plans {
		rows := rowsByRole[plan.role]
		prefix := "residual_prefilter_" + strconv.Itoa(i+1) + "_"
		deferPrefilter, deferReason := legacyDirectRelationshipResidualPrefilterShouldDefer(plan, edges)
		if deferPrefilter {
			probes = append(probes,
				legacyDirectRelationshipProbe(prefix+"role", plan.role),
				legacyDirectRelationshipProbe(prefix+"table", plan.table),
				legacyDirectRelationshipProbe(prefix+"predicates", strconv.Itoa(len(plan.predicates))),
				legacyDirectRelationshipProbe(prefix+"field_count", strconv.Itoa(len(plan.fields))),
				legacyDirectRelationshipProbe(prefix+"rows_before", strconv.Itoa(len(rows))),
				legacyDirectRelationshipProbe(prefix+"rows_after", strconv.Itoa(len(rows))),
				legacyDirectRelationshipProbe(prefix+"rows_removed", "0"),
				legacyDirectRelationshipProbe(prefix+"rows_evaluated", "0"),
				legacyDirectRelationshipProbe(prefix+"placement", "deferred_after_graph_reduction"),
				legacyDirectRelationshipProbe(prefix+"placement_reason", deferReason),
				legacyDirectRelationshipProbe(prefix+"elapsed", "0s"),
				legacyDirectRelationshipProbe(prefix+"materialization_elapsed", "0s"),
				legacyDirectRelationshipProbe(prefix+"evaluation_elapsed", "0s"),
			)
			continue
		}
		filterStart := time.Now()
		filtered, timing, diagnostics, err := e.legacyDirectRelationshipFilterRoleRowsByResiduals(ctx, request, materialization, plan, rows)
		filterElapsed := time.Since(filterStart)
		if err != nil || diagnostics.BlocksNative() {
			return probes, nil, diagnostics, err
		}
		rowsByRole[plan.role] = filtered
		appliedPredicateIndexes = append(appliedPredicateIndexes, plan.predicateIndexes...)
		probes = append(probes,
			legacyDirectRelationshipProbe(prefix+"role", plan.role),
			legacyDirectRelationshipProbe(prefix+"table", plan.table),
			legacyDirectRelationshipProbe(prefix+"predicates", strconv.Itoa(len(plan.predicates))),
			legacyDirectRelationshipProbe(prefix+"fields", strconv.Itoa(len(plan.fields))),
			legacyDirectRelationshipProbe(prefix+"rows_before", strconv.Itoa(len(rows))),
			legacyDirectRelationshipProbe(prefix+"rows_after", strconv.Itoa(len(filtered))),
			legacyDirectRelationshipProbe(prefix+"rows_removed", strconv.Itoa(len(rows)-len(filtered))),
			legacyDirectRelationshipProbe(prefix+"rows_evaluated", strconv.Itoa(timing.evaluatedRows)),
			legacyDirectRelationshipProbe(prefix+"elapsed", filterElapsed.String()),
			legacyDirectRelationshipProbe(prefix+"materialization_elapsed", timing.materializationElapsed.String()),
			legacyDirectRelationshipProbe(prefix+"evaluation_elapsed", timing.evaluationElapsed.String()),
		)
	}
	sort.Ints(appliedPredicateIndexes)
	return probes, appliedPredicateIndexes, nil, nil
}

func legacyDirectRelationshipResidualPrefilterShouldDefer(plan legacyDirectRelationshipResidualRolePrefilter, edges []legacyDirectRelationshipEdge) (bool, string) {
	if !legacyDirectRelationshipResidualRoleHasRelationshipEdge(plan.role, edges) {
		return false, "role_not_connected_to_relationship_graph"
	}
	for _, predicate := range plan.predicates {
		if !legacyDirectRelationshipResidualExprIsSameRoleFieldComparison(predicate.Expr, plan.role) {
			return false, "residual_is_not_same_role_field_comparison"
		}
	}
	return true, "same_role_field_comparison_on_relationship_role"
}

func legacyDirectRelationshipResidualRoleHasRelationshipEdge(role string, edges []legacyDirectRelationshipEdge) bool {
	for _, edge := range edges {
		if edge.childKey() == role || edge.parentKey() == role {
			return true
		}
	}
	return false
}

func legacyDirectRelationshipResidualExprIsSameRoleFieldComparison(expr qsbridge.Expr, role string) bool {
	switch typed := expr.(type) {
	case qsbridge.BinaryExpr:
		return legacyDirectRelationshipResidualBinaryIsSameRoleFieldComparison(typed, role)
	case *qsbridge.BinaryExpr:
		if typed == nil {
			return false
		}
		return legacyDirectRelationshipResidualBinaryIsSameRoleFieldComparison(*typed, role)
	default:
		return false
	}
}

func legacyDirectRelationshipResidualBinaryIsSameRoleFieldComparison(binary qsbridge.BinaryExpr, role string) bool {
	if binary.Op == qsbridge.BinaryOpAnd {
		return legacyDirectRelationshipResidualExprIsSameRoleFieldComparison(binary.Left, role) &&
			legacyDirectRelationshipResidualExprIsSameRoleFieldComparison(binary.Right, role)
	}
	if !legacyDirectRelationshipResidualBinaryOpIsComparison(binary.Op) {
		return false
	}
	left, leftOK := legacyDirectRelationshipResidualFieldExpr(binary.Left)
	right, rightOK := legacyDirectRelationshipResidualFieldExpr(binary.Right)
	if !leftOK || !rightOK {
		return false
	}
	return strings.EqualFold(materializationFieldRole(left.Table.Table, left), role) &&
		strings.EqualFold(materializationFieldRole(right.Table.Table, right), role)
}

func legacyDirectRelationshipResidualBinaryOpIsComparison(op qsbridge.BinaryOp) bool {
	switch op {
	case qsbridge.BinaryOpEqual,
		qsbridge.BinaryOpNotEqual,
		qsbridge.BinaryOpLess,
		qsbridge.BinaryOpLessEqual,
		qsbridge.BinaryOpGreater,
		qsbridge.BinaryOpGreaterEqual:
		return true
	default:
		return false
	}
}

func legacyDirectRelationshipResidualFieldExpr(expr qsbridge.Expr) (qsbridge.FieldRef, bool) {
	switch typed := expr.(type) {
	case qsbridge.FieldExpr:
		return typed.Ref, true
	case *qsbridge.FieldExpr:
		if typed == nil {
			return qsbridge.FieldRef{}, false
		}
		return typed.Ref, true
	default:
		return qsbridge.FieldRef{}, false
	}
}

func legacyDirectRelationshipResidualPlacementPolicyProbes(prefilterProbes []ExecutionProbe, rowsByRole map[string][]qsbridge.QuantaRownum) []ExecutionProbe {
	observations := legacyDirectRelationshipResidualPlacementObservations(prefilterProbes, rowsByRole)
	probes := []ExecutionProbe{
		legacyDirectRelationshipProbe("residual_prefilter_placement_policies", strconv.Itoa(len(observations))),
	}
	for _, observation := range observations {
		prefix := "residual_prefilter_placement_" + strconv.Itoa(observation.ordinal) + "_"
		probes = append(probes,
			legacyDirectRelationshipProbe(prefix+"observe_only", "true"),
			legacyDirectRelationshipProbe(prefix+"role", observation.role),
			legacyDirectRelationshipProbe(prefix+"table", observation.table),
			legacyDirectRelationshipProbe(prefix+"current", observation.placement),
			legacyDirectRelationshipProbe(prefix+"recommended", observation.recommended),
			legacyDirectRelationshipProbe(prefix+"reason", observation.reason),
			legacyDirectRelationshipProbe(prefix+"fields", strconv.Itoa(observation.fields)),
			legacyDirectRelationshipProbe(prefix+"rows_before", strconv.Itoa(observation.rowsBefore)),
			legacyDirectRelationshipProbe(prefix+"rows_after_eager", strconv.Itoa(observation.rowsAfter)),
			legacyDirectRelationshipProbe(prefix+"rows_after_graph", strconv.Itoa(observation.reducedRows)),
			legacyDirectRelationshipProbe(prefix+"graph_reduction_pct", strconv.Itoa(observation.reductionPct)),
		)
	}
	return probes
}

func legacyDirectRelationshipResidualPlacementObservations(prefilterProbes []ExecutionProbe, rowsByRole map[string][]qsbridge.QuantaRownum) []legacyDirectRelationshipResidualPlacementObservation {
	byOrdinal := map[int]*legacyDirectRelationshipResidualPlacementObservation{}
	ordinals := []int{}
	observationFor := func(ordinal int) *legacyDirectRelationshipResidualPlacementObservation {
		observation := byOrdinal[ordinal]
		if observation != nil {
			return observation
		}
		observation = &legacyDirectRelationshipResidualPlacementObservation{ordinal: ordinal}
		byOrdinal[ordinal] = observation
		ordinals = append(ordinals, ordinal)
		return observation
	}
	for _, probe := range prefilterProbes {
		ordinal, suffix, ok := legacyDirectRelationshipResidualPrefilterProbeParts(probe.Name)
		if !ok {
			continue
		}
		observation := observationFor(ordinal)
		switch suffix {
		case "role":
			observation.role = probe.Value
		case "table":
			observation.table = probe.Value
		case "fields":
			observation.fields = legacyDirectRelationshipProbeInt(probe.Value)
		case "field_count":
			observation.fields = legacyDirectRelationshipProbeInt(probe.Value)
		case "placement":
			observation.placement = probe.Value
		case "rows_before":
			observation.rowsBefore = legacyDirectRelationshipProbeInt(probe.Value)
		case "rows_after":
			observation.rowsAfter = legacyDirectRelationshipProbeInt(probe.Value)
		}
	}
	sort.Ints(ordinals)
	result := make([]legacyDirectRelationshipResidualPlacementObservation, 0, len(ordinals))
	for _, ordinal := range ordinals {
		observation := *byOrdinal[ordinal]
		if observation.placement == "" {
			observation.placement = "eager_prefilter"
		}
		observation.reducedRows = len(rowsByRole[observation.role])
		observation.recommended = "keep_eager_prefilter"
		observation.reason = "no_large_graph_reduction_observed"
		if observation.rowsAfter > 0 {
			observation.reductionPct = ((observation.rowsAfter - observation.reducedRows) * 100) / observation.rowsAfter
		}
		if observation.placement == "deferred_after_graph_reduction" {
			observation.recommended = "defer_after_graph_reduction"
			observation.reason = "internal_deferred_prefilter_applied"
			result = append(result, observation)
			continue
		}
		if observation.fields > 0 && observation.rowsAfter >= 10 && observation.reducedRows*10 <= observation.rowsAfter {
			observation.recommended = "defer_after_graph_reduction"
			observation.reason = "graph_reduced_prefiltered_role_by_order_of_magnitude"
		}
		result = append(result, observation)
	}
	return result
}

func legacyDirectRelationshipResidualPrefilterProbeParts(name string) (int, string, bool) {
	const prefix = "residual_prefilter_"
	if !strings.HasPrefix(name, prefix) {
		return 0, "", false
	}
	rest := strings.TrimPrefix(name, prefix)
	indexEnd := strings.Index(rest, "_")
	if indexEnd <= 0 {
		return 0, "", false
	}
	ordinal := legacyDirectRelationshipProbeInt(rest[:indexEnd])
	if ordinal <= 0 {
		return 0, "", false
	}
	return ordinal, rest[indexEnd+1:], true
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipFilterRoleRowsByResiduals(ctx context.Context, request ExecutionRequest, kernel ProjectionMaterializationKernel, plan legacyDirectRelationshipResidualRolePrefilter, rows []qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, legacyDirectRelationshipResidualRolePrefilterTiming, qsbridge.DiagnosticSet, error) {
	var timing legacyDirectRelationshipResidualRolePrefilterTiming
	if len(rows) == 0 || len(plan.predicates) == 0 {
		return append([]qsbridge.QuantaRownum(nil), rows...), timing, nil, nil
	}
	if comparisonPlan, ok := legacyDirectRelationshipSameRowComparisonPlan(plan); ok {
		evaluationStart := time.Now()
		comparisonRequest := comparisonPlan.Request(rows)
		comparisonRequest.FromEpochMillis = request.Materialization.FromEpochMillis
		comparisonRequest.ToEpochMillis = request.Materialization.ToEpochMillis
		comparison, err := e.sameRowComparisonKernel().CompareSameRowFields(ctx, comparisonRequest)
		timing.evaluationElapsed = time.Since(evaluationStart)
		timing.evaluatedRows = len(rows)
		if err != nil || comparison.Diagnostics.BlocksNative() {
			return nil, timing, comparison.Diagnostics, err
		}
		return append([]qsbridge.QuantaRownum(nil), comparison.Domain.Rownums...), timing, comparison.Diagnostics, nil
	}
	materialization := e.legacyDirectRelationshipTimeMaterializationForRole(request, plan.table, plan.role)
	materialization.Index = plan.table
	materialization.Rownums = append([]qsbridge.QuantaRownum(nil), rows...)
	materialization.ProjectionFields = plan.fields
	materializationStart := time.Now()
	rowSet, diagnostics, _, err := directBitmapMaterializeWithKernel(ctx, kernel, materialization)
	timing.materializationElapsed = time.Since(materializationStart)
	if err != nil || diagnostics.BlocksNative() {
		return nil, timing, diagnostics, err
	}
	keep := make([]qsbridge.QuantaRownum, 0, len(rows))
	evaluationStart := time.Now()
	for i, rownum := range rows {
		timing.evaluatedRows++
		matched, diagnostics := directBitmapEvaluateResidualPredicates(plan.predicates, rowSet, i)
		if diagnostics.BlocksNative() {
			return nil, timing, diagnostics, nil
		}
		if matched {
			keep = append(keep, rownum)
		}
	}
	timing.evaluationElapsed = time.Since(evaluationStart)
	return keep, timing, nil, nil
}

func legacyDirectRelationshipSameRowComparisonPlan(plan legacyDirectRelationshipResidualRolePrefilter) (qsbridge.SameRowComparisonPlan, bool) {
	if len(plan.predicates) != 1 {
		return qsbridge.SameRowComparisonPlan{}, false
	}
	left, right, op, ok := qsbridge.SameRowBSIComparisonPredicate(plan.predicates[0])
	if !ok {
		return qsbridge.SameRowComparisonPlan{}, false
	}
	if !strings.EqualFold(materializationFieldRole(left.Table.Table, left), plan.role) ||
		!strings.EqualFold(materializationFieldRole(right.Table.Table, right), plan.role) {
		return qsbridge.SameRowComparisonPlan{}, false
	}
	return qsbridge.SameRowComparisonPlan{
		ID:             "relationship_residual_same_row_" + plan.role,
		ProbeName:      "relationship_residual_same_row_" + plan.role,
		Left:           left,
		Right:          right,
		Operator:       op,
		Kind:           qsbridge.SameRowComparisonBSI,
		Domain:         qsbridge.RownumDomain{Table: left.Table, Role: qsbridge.TableInstanceID(plan.role)},
		PredicateScope: qsbridge.PredicateScopeWhere,
	}, true
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipApplyTupleSameRowResiduals(ctx context.Context, request ExecutionRequest, tupleRows RelationshipTupleRowSet, alignedRows map[string][]qsbridge.QuantaRownum) (RelationshipTupleRowSet, map[string][]qsbridge.QuantaRownum, ExecutionRequest, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	classification := legacyDirectRelationshipClassifyTupleResiduals(request)
	plans := classification.nativeSameRow
	probes := []ExecutionProbe{
		legacyDirectRelationshipProbe("graph_tuple_same_row_plans", strconv.Itoa(len(plans))),
		legacyDirectRelationshipProbe("graph_tuple_residual_native_same_row", strconv.Itoa(len(classification.nativeSameRow))),
		legacyDirectRelationshipProbe("graph_tuple_residual_materialized", strconv.Itoa(len(classification.materialized))),
		legacyDirectRelationshipProbe("graph_tuple_residual_unsupported", strconv.Itoa(len(classification.unsupported))),
	}
	if len(plans) == 0 || tupleRows.CandidateCount() == 0 {
		return tupleRows, alignedRows, request, probes, nil, nil
	}
	currentTupleRows := tupleRows
	currentAlignedRows := legacyDirectRelationshipCopyAlignedRows(alignedRows)
	for i, plan := range plans {
		role := plan.Domain.Name()
		rows := currentAlignedRows[role]
		prefix := "graph_tuple_same_row_" + strconv.Itoa(i+1) + "_"
		if len(rows) == 0 {
			return RelationshipTupleRowSet{}, nil, request, probes, legacyDirectRelationshipDiagnostic("relationship-vector same-row residual cannot find aligned role " + role), nil
		}
		decision := sameRowComparisonPolicy(plan, len(rows))
		probes = append(probes,
			legacyDirectRelationshipProbe(prefix+"policy", sameRowComparisonPolicyName(decision)),
			legacyDirectRelationshipProbe(prefix+"policy_reason", decision.reason),
		)
		if !decision.useNative {
			continue
		}
		comparisonRequest := plan.Request(rows)
		comparisonRequest.FromEpochMillis = request.Materialization.FromEpochMillis
		comparisonRequest.ToEpochMillis = request.Materialization.ToEpochMillis
		comparison, err := e.sameRowComparisonKernel().CompareSameRowFields(ctx, comparisonRequest)
		probes = append(probes, comparison.Probes...)
		if err != nil || comparison.Diagnostics.BlocksNative() {
			return RelationshipTupleRowSet{}, nil, request, probes, comparison.Diagnostics, err
		}
		keep := legacyDirectRelationshipSameRowKeepIndexes(rows, comparison.Domain.Rownums)
		currentTupleRows = currentTupleRows.FilterByIndexes(keep)
		currentAlignedRows = legacyDirectRelationshipFilterAlignedRowsByIndexes(currentAlignedRows, keep)
		probes = append(probes,
			legacyDirectRelationshipProbe(prefix+"role", role),
			legacyDirectRelationshipProbe(prefix+"rows_before", strconv.Itoa(len(rows))),
			legacyDirectRelationshipProbe(prefix+"rows_after", strconv.Itoa(len(comparison.Domain.Rownums))),
			legacyDirectRelationshipProbe(prefix+"rows_removed", strconv.Itoa(len(rows)-len(comparison.Domain.Rownums))),
		)
	}
	return currentTupleRows, currentAlignedRows, legacyDirectRelationshipRequestWithMaterializedTupleResiduals(request, classification), probes, nil, nil
}

func legacyDirectRelationshipTupleSameRowComparisonPlans(request ExecutionRequest) []qsbridge.SameRowComparisonPlan {
	return legacyDirectRelationshipClassifyTupleResiduals(request).nativeSameRow
}

func legacyDirectRelationshipClassifyTupleResiduals(request ExecutionRequest) legacyDirectRelationshipTupleResidualClassification {
	residuals := directBitmapResidualScanPredicates(request)
	classification := legacyDirectRelationshipTupleResidualClassification{
		nativeSameRow:       make([]qsbridge.SameRowComparisonPlan, 0, len(residuals)),
		materialized:        make([]qsbridge.Predicate, 0, len(residuals)),
		materializedIndexes: make(map[int]bool, len(residuals)),
	}
	for i, predicate := range residuals {
		plan, ok := legacyDirectRelationshipTupleSameRowComparisonPlan(predicate, i)
		if ok {
			classification.nativeSameRow = append(classification.nativeSameRow, plan)
			continue
		}
		classification.materialized = append(classification.materialized, predicate)
		classification.materializedIndexes[i] = true
	}
	return classification
}

func legacyDirectRelationshipTupleSameRowComparisonPlan(predicate qsbridge.Predicate, index int) (qsbridge.SameRowComparisonPlan, bool) {
	left, right, op, ok := qsbridge.SameRowBSIComparisonPredicate(predicate)
	if !ok {
		return qsbridge.SameRowComparisonPlan{}, false
	}
	role := materializationFieldRole(left.Table.Table, left)
	if role == "" || !strings.EqualFold(role, materializationFieldRole(right.Table.Table, right)) {
		return qsbridge.SameRowComparisonPlan{}, false
	}
	return qsbridge.SameRowComparisonPlan{
		ID:             "relationship_tuple_same_row_" + strconv.Itoa(index+1),
		ProbeName:      "relationship_tuple_same_row_" + strconv.Itoa(index+1),
		Left:           left,
		Right:          right,
		Operator:       op,
		Kind:           qsbridge.SameRowComparisonBSI,
		Domain:         qsbridge.RownumDomain{Table: left.Table, Role: qsbridge.TableInstanceID(role)},
		PredicateScope: qsbridge.PredicateScopeWhere,
		PredicateIndex: index,
	}, true
}

func legacyDirectRelationshipRequestWithMaterializedTupleResiduals(request ExecutionRequest, classification legacyDirectRelationshipTupleResidualClassification) ExecutionRequest {
	predicates := make([]qsbridge.Predicate, 0, len(request.Predicates))
	residualIndex := 0
	for _, predicate := range request.Predicates {
		if predicate.Placement != qsbridge.PredicateResidualScan {
			predicates = append(predicates, predicate)
			continue
		}
		if classification.materializedIndexes[residualIndex] {
			predicates = append(predicates, predicate)
		}
		residualIndex++
	}
	request.Predicates = predicates
	return request
}

func sameRowComparisonPolicy(plan qsbridge.SameRowComparisonPlan, candidateCount int) sameRowComparisonPolicyDecision {
	if plan.Kind != qsbridge.SameRowComparisonBSI {
		return sameRowComparisonPolicyDecision{reason: "unsupported_same_row_kind"}
	}
	if candidateCount == 0 {
		return sameRowComparisonPolicyDecision{useNative: true, reason: "empty_candidate_set"}
	}
	return sameRowComparisonPolicyDecision{useNative: true, reason: "native_compares_bsi_values_without_sql_projection"}
}

func sameRowComparisonPolicyName(decision sameRowComparisonPolicyDecision) string {
	if decision.useNative {
		return "native_same_row"
	}
	return "materialized_residual"
}

func legacyDirectRelationshipSameRowKeepIndexes(input []qsbridge.QuantaRownum, output []qsbridge.QuantaRownum) []int {
	keep := make([]int, 0, len(output))
	next := 0
	for i, rownum := range input {
		if next >= len(output) {
			break
		}
		if rownum != output[next] {
			continue
		}
		keep = append(keep, i)
		next++
	}
	return keep
}

func legacyDirectRelationshipCopyAlignedRows(alignedRows map[string][]qsbridge.QuantaRownum) map[string][]qsbridge.QuantaRownum {
	copied := make(map[string][]qsbridge.QuantaRownum, len(alignedRows))
	for role, rows := range alignedRows {
		copied[role] = append([]qsbridge.QuantaRownum(nil), rows...)
	}
	return copied
}

func legacyDirectRelationshipFilterAlignedRowsByIndexes(alignedRows map[string][]qsbridge.QuantaRownum, keep []int) map[string][]qsbridge.QuantaRownum {
	filtered := make(map[string][]qsbridge.QuantaRownum, len(alignedRows))
	for role, rows := range alignedRows {
		out := make([]qsbridge.QuantaRownum, 0, len(keep))
		for _, index := range keep {
			if index >= 0 && index < len(rows) {
				out = append(out, rows[index])
			}
		}
		filtered[role] = out
	}
	return filtered
}

func legacyDirectRelationshipResidualRolePrefilterPlans(request ExecutionRequest) []legacyDirectRelationshipResidualRolePrefilter {
	plansByRole := make(map[string]legacyDirectRelationshipResidualRolePrefilter)
	for index, predicate := range request.Predicates {
		if predicate.Placement != qsbridge.PredicateResidualScan {
			continue
		}
		if predicate.Scope != "" && predicate.Scope != qsbridge.PredicateScopeWhere {
			continue
		}
		if predicate.Combinator != "" && predicate.Combinator != qsbridge.PredicateCombinatorAnd {
			continue
		}
		role, table, fields, ok := legacyDirectRelationshipResidualPrefilterFields(predicate)
		if !ok {
			continue
		}
		plan := plansByRole[role]
		if plan.role == "" {
			plan.role = role
			plan.table = table
		}
		plan.predicates = append(plan.predicates, predicate)
		plan.predicateIndexes = append(plan.predicateIndexes, index)
		plan.fields = legacyDirectRelationshipMergeProjectionFields(plan.fields, fields)
		plansByRole[role] = plan
	}
	roles := make([]string, 0, len(plansByRole))
	for role := range plansByRole {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	plans := make([]legacyDirectRelationshipResidualRolePrefilter, 0, len(roles))
	for _, role := range roles {
		plans = append(plans, plansByRole[role])
	}
	return plans
}

func legacyDirectRelationshipRequestWithoutAppliedResidualPrefilters(request ExecutionRequest, appliedPredicateIndexes []int) ExecutionRequest {
	if len(appliedPredicateIndexes) == 0 {
		return request
	}
	applied := make(map[int]struct{}, len(appliedPredicateIndexes))
	for _, index := range appliedPredicateIndexes {
		applied[index] = struct{}{}
	}
	predicates := make([]qsbridge.Predicate, 0, len(request.Predicates)-len(applied))
	for index, predicate := range request.Predicates {
		if _, ok := applied[index]; ok && predicate.Placement == qsbridge.PredicateResidualScan {
			continue
		}
		predicates = append(predicates, predicate)
	}
	request.Predicates = predicates
	return request
}

func legacyDirectRelationshipResidualPrefilterFields(predicate qsbridge.Predicate) (string, string, []qsbridge.QuantaProjectionField, bool) {
	required := directBitmapMembershipRequiredFields(predicate.Expr)
	if len(required) == 0 {
		return "", "", nil, false
	}
	var role string
	var table string
	fields := make([]qsbridge.QuantaProjectionField, 0, len(required))
	for _, field := range required {
		fieldTable := field.Table.Table
		fieldRole := strings.ToLower(materializationFieldRole(fieldTable, field))
		if fieldTable == "" || fieldRole == "" {
			return "", "", nil, false
		}
		if role == "" {
			role = fieldRole
			table = fieldTable
		}
		if role != fieldRole || !strings.EqualFold(table, fieldTable) {
			return "", "", nil, false
		}
		projection := directBitmapMembershipProjectionField(field)
		projection.Role = qsbridge.TableInstanceID(fieldRole)
		fields = legacyDirectRelationshipMergeProjectionFields(fields, []qsbridge.QuantaProjectionField{projection})
	}
	return role, table, fields, true
}

func legacyDirectRelationshipMergeProjectionFields(left []qsbridge.QuantaProjectionField, right []qsbridge.QuantaProjectionField) []qsbridge.QuantaProjectionField {
	result := append([]qsbridge.QuantaProjectionField(nil), left...)
	for _, field := range right {
		if legacyDirectRelationshipHasProjectionField(result, field) {
			continue
		}
		result = append(result, field)
	}
	return result
}

func legacyDirectRelationshipHasProjectionField(fields []qsbridge.QuantaProjectionField, want qsbridge.QuantaProjectionField) bool {
	for _, field := range fields {
		if strings.EqualFold(field.Index, want.Index) && strings.EqualFold(string(field.Role), string(want.Role)) && field.Field == want.Field && field.PhysicalName == want.PhysicalName {
			return true
		}
	}
	return false
}
