package qsruntime

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// ExecutionInspectionRow is qsbridge's protocol-neutral inspection record.
type ExecutionInspectionRow = qsbridge.ExecutionInspectionRow

// ExecutionInspectionResultColumns returns stable metadata for inspection result rows.
func ExecutionInspectionResultColumns() []qsbridge.ResultColumn {
	return qsbridge.ExecutionInspectionResultColumns()
}

// Rows returns stable adapter-facing rows for runtime inspection metadata.
func (i ExecutionInspection) Rows() []ExecutionInspectionRow {
	rows := []ExecutionInspectionRow{
		{Section: "summary", Name: "supported", Value: strconv.FormatBool(i.Supported())},
		{Section: "route", Name: "path", Value: string(i.Route.Path)},
		{Section: "route", Name: "discovery", Value: string(i.Route.Discovery)},
		{Section: "route", Name: "target", Value: runtimeTargetValue(i.Route.Target)},
		{Section: "executor", Name: "selected", Value: string(i.SelectedExecutor)},
		{Section: "runtime", Name: "implementation", Value: string(i.RuntimeProfile.Effective().Implementation)},
		{Section: "runtime", Name: "detail", Value: i.RuntimeProfile.Effective().Detail},
	}
	rows = append(rows, executionJoinInspectionRows(i.Joins)...)
	rows = append(rows, relationshipVectorAdapterRows(i.JoinPlan)...)
	rows = append(rows, executionShapeInspectionRows(i.Shape)...)
	rows = append(rows, materializationCapabilityInspectionRows(i.Materialization)...)
	rows = append(rows, filterDomainInspectionRows(i.FilterDomain)...)
	rows = append(rows, filterDomainNormalizationInspectionRows(i.FilterDomainPlan)...)
	if i.CallPlan.RootIndex != "" {
		rows = append(rows,
			ExecutionInspectionRow{Section: "call_plan", Name: "root_index", Value: i.CallPlan.RootIndex, Detail: i.CallPlan.Summary()},
			ExecutionInspectionRow{Section: "call_plan", Name: "fragments", Value: strconv.Itoa(i.CallPlan.FragmentCount)},
			ExecutionInspectionRow{Section: "call_plan", Name: "projections", Value: strconv.Itoa(i.CallPlan.ProjectionCount)},
			ExecutionInspectionRow{Section: "call_plan", Name: "sql_aggregates", Value: strconv.Itoa(i.CallPlan.SQLAggregateCount)},
			ExecutionInspectionRow{Section: "call_plan", Name: "native_aggregates", Value: strconv.Itoa(i.CallPlan.NativeAggregateCount)},
			ExecutionInspectionRow{Section: "call_plan", Name: "materialization", Value: strconv.FormatBool(i.CallPlan.HasMaterialization)},
			ExecutionInspectionRow{Section: "call_plan", Name: "session_pool", Value: strconv.FormatBool(i.CallPlan.UsesSessionPool)},
		)
	}
	for index, step := range i.CallPlan.Steps {
		status := i.CallPlan.StepStatus(step)
		rows = append(rows, ExecutionInspectionRow{
			Section: "step",
			Name:    fmt.Sprintf("%03d", index+1),
			Value:   string(step),
			Detail:  "status=" + string(status),
		})
	}
	for index, note := range i.CallPlan.Notes {
		rows = append(rows, ExecutionInspectionRow{
			Section: "note",
			Name:    fmt.Sprintf("%03d", index+1),
			Value:   note,
		})
	}
	rows = append(rows, inspectionDiagnosticRows(i.Diagnostics)...)
	return rows
}

func materializationCapabilityInspectionRows(report ProjectionMaterializationCapabilityReport) []ExecutionInspectionRow {
	if len(report.Fields) == 0 && report.NativeFieldCount == 0 && report.CompatFallbackFieldCount == 0 && !report.RuntimeFallbackObserved {
		return nil
	}
	rows := []ExecutionInspectionRow{
		{Section: "materialization_capability", Name: "native_fields", Value: strconv.Itoa(report.NativeFieldCount)},
		{Section: "materialization_capability", Name: "compat_fallback_fields", Value: strconv.Itoa(report.CompatFallbackFieldCount)},
		{Section: "materialization_capability", Name: "runtime_fallback_observed", Value: strconv.FormatBool(report.RuntimeFallbackObserved)},
		{Section: "materialization_capability", Name: "fallback_diagnostic_count", Value: strconv.Itoa(report.FallbackDiagnosticCount)},
		{Section: "materialization_capability", Name: "legacy_materializer_reachable", Value: strconv.FormatBool(report.LegacyMaterializerReachable)},
		{Section: "materialization_capability", Name: "legacy_materializer_used", Value: strconv.FormatBool(report.LegacyMaterializerUsed)},
	}
	for index, field := range report.Fields {
		rows = append(rows, ExecutionInspectionRow{
			Section: "materialization_field",
			Name:    fmt.Sprintf("%03d", index+1),
			Value:   string(field.Status),
			Detail:  materializationCapabilityFieldDetail(field),
		})
	}
	return rows
}

func materializationCapabilityFieldDetail(field ProjectionMaterializationFieldCapability) string {
	parts := []string{
		"field=" + field.Index + "." + field.Field,
		"type=" + string(field.Type),
		"native=" + strconv.FormatBool(field.Status == ProjectionMaterializationCapabilityNative),
	}
	if field.Encoding != "" {
		parts = append(parts, "encoding="+string(field.Encoding))
	}
	if field.LookupKind != NativeProjectionLookupUnknown {
		parts = append(parts, "lookup="+string(field.LookupKind))
	}
	if field.Source != "" {
		parts = append(parts, "source="+field.Source)
	}
	if field.ReasonCode != ProjectionMaterializationReasonUnknown {
		parts = append(parts, "reason_code="+string(field.ReasonCode))
	}
	if field.Reason != "" {
		parts = append(parts, "reason="+field.Reason)
	}
	return strings.Join(parts, " ")
}

func filterDomainInspectionRows(translation qsbridge.QuantaFilterDomainTranslation) []ExecutionInspectionRow {
	if filterDomainTranslationEmpty(translation) {
		return nil
	}
	return []ExecutionInspectionRow{
		{Section: "filter_domain", Name: "translation_required", Value: strconv.FormatBool(translation.Required)},
		{Section: "filter_domain", Name: "source_domains", Value: strings.Join(translation.SourceDomains, ",")},
		{Section: "filter_domain", Name: "target_domain", Value: translation.TargetDomain},
		{Section: "filter_domain", Name: "strategies", Value: filterDomainStrategiesValue(translation.Strategies)},
	}
}

func filterDomainStrategiesValue(strategies []qsbridge.PhysicalStrategy) string {
	values := make([]string, 0, len(strategies))
	for _, strategy := range strategies {
		values = append(values, string(strategy))
	}
	return strings.Join(values, ",")
}

func filterDomainNormalizationInspectionRows(plan qsbridge.FilterDomainNormalizationPlan) []ExecutionInspectionRow {
	if !plan.Required() {
		return nil
	}
	rows := []ExecutionInspectionRow{
		{
			Section: "filter_domain_normalization",
			Name:    "request_count",
			Value:   strconv.Itoa(len(plan.Requests)),
			Detail:  "operation=" + string(plan.Operation),
		},
		{
			Section: "filter_domain_normalization",
			Name:    "expected_replacements",
			Value:   strconv.Itoa(len(plan.Requests)),
			Detail:  "source-domain leaves require target-domain candidates",
		},
	}
	for index, request := range plan.Requests {
		rows = append(rows, ExecutionInspectionRow{
			Section: "filter_domain_normalization",
			Name:    fmt.Sprintf("request_%03d", index+1),
			Value:   request.SourceDomain + "->" + request.TargetDomain,
			Detail: fmt.Sprintf(
				"path_len=%d strategy=%s replacement=expected",
				len(request.RelationshipPath),
				request.Strategy,
			),
		})
		if direction, ok := request.RelationshipVectorDirection(); ok {
			edge := request.RelationshipPath[0]
			rows = append(rows, ExecutionInspectionRow{
				Section: "filter_domain_normalization",
				Name:    fmt.Sprintf("vector_%03d", index+1),
				Value:   request.SourceDomain + "->" + request.TargetDomain,
				Detail: fmt.Sprintf(
					"source_leaf=pending source_candidates=%s edge=%s->%s direction=%s target=%s",
					request.SourceDomain,
					edge.Left.QualifiedName(),
					edge.Right.QualifiedName(),
					direction,
					request.TargetDomain,
				),
			})
		}
	}
	return rows
}

func filterDomainTranslationEmpty(translation qsbridge.QuantaFilterDomainTranslation) bool {
	return !translation.Required &&
		len(translation.SourceDomains) == 0 &&
		translation.TargetDomain == "" &&
		len(translation.Strategies) == 0
}

func executionShapeInspectionRows(shape ExecutionShapeInspection) []ExecutionInspectionRow {
	var rows []ExecutionInspectionRow
	if shape.GroupedAggregateTopNCandidate {
		rows = append(rows, ExecutionInspectionRow{
			Section: "execution_shape",
			Name:    "grouped_aggregate_topn_candidate",
			Value:   "true",
			Detail:  shape.GroupedAggregateTopNDetail,
		})
	}
	if shape.FoundsetFollowUpCandidate {
		rows = append(rows, ExecutionInspectionRow{
			Section: "execution_shape",
			Name:    "foundset_followup_candidate",
			Value:   "true",
			Detail:  shape.FoundsetFollowUpDetail,
		})
	}
	return rows
}

func relationshipVectorAdapterRows(plan RelationshipJoinPlan) []ExecutionInspectionRow {
	if !plan.NeedsRelationshipVectorExecution() {
		return nil
	}
	request := plan.VectorRequest("")
	rows := []ExecutionInspectionRow{
		{Section: "relationship_adapter", Name: "kind", Value: string(RelationshipJoinExecutionVector)},
		{Section: "relationship_adapter", Name: "edge_count", Value: strconv.Itoa(request.EdgeCount())},
	}
	if edge, ok := request.FirstEdge(); ok {
		rows = append(rows,
			ExecutionInspectionRow{Section: "relationship_adapter", Name: "first_edge", Value: edge.Left.QualifiedName() + " -> " + edge.Right.QualifiedName()},
			ExecutionInspectionRow{Section: "relationship_adapter", Name: "intent", Value: string(edge.Intent)},
			ExecutionInspectionRow{Section: "relationship_adapter", Name: "sql_kind", Value: string(edge.SQLKind)},
			ExecutionInspectionRow{Section: "relationship_adapter", Name: "encoding", Value: string(edge.EncodingKind)},
			ExecutionInspectionRow{Section: "relationship_adapter", Name: "capabilities", Value: relationshipCapabilitiesValue(edge.Capabilities)},
		)
	}
	rows = append(rows, relationshipVectorCoverageInspectionRows(request)...)
	return rows
}

func relationshipVectorCoverageInspectionRows(request RelationshipVectorJoinRequest) []ExecutionInspectionRow {
	reads := request.RelationshipVectorProjectionReads(nil)
	rows := make([]ExecutionInspectionRow, 0, len(reads)*6)
	for index, read := range reads {
		prefix := fmt.Sprintf("read_%03d.", index+1)
		coverage := read.CoveragePlan.Effective(read.ProjectionScope)
		detail := fmt.Sprintf(
			"input_domain=%s output_domain=%s translation=%s intent=%s edge=%s->%s",
			read.Input.Domain.Name(),
			read.OutputDomain.Name(),
			read.Translation.Direction,
			read.Intent,
			read.Edge.Left.QualifiedName(),
			read.Edge.Right.QualifiedName(),
		)
		rows = append(rows,
			ExecutionInspectionRow{Section: "relationship_vector_coverage", Name: prefix + "scope", Value: string(coverage.ProjectionScope), Detail: detail},
			ExecutionInspectionRow{Section: "relationship_vector_coverage", Name: prefix + "requested_rows", Value: strconv.Itoa(read.Input.CandidateCount())},
			ExecutionInspectionRow{Section: "relationship_vector_coverage", Name: prefix + "expected_status", Value: string(coverage.ExpectStatus)},
			ExecutionInspectionRow{Section: "relationship_vector_coverage", Name: prefix + "verify", Value: strconv.FormatBool(coverage.VerifyCoverage)},
			ExecutionInspectionRow{Section: "relationship_vector_coverage", Name: prefix + "recovery_policy", Value: string(coverage.RecoveryPolicy)},
		)
	}
	return rows
}

func executionJoinInspectionRows(joins []ExecutionJoinInspection) []ExecutionInspectionRow {
	rows := make([]ExecutionInspectionRow, 0, len(joins)*6)
	for index, join := range joins {
		prefix := fmt.Sprintf("%03d", index+1)
		detail := fmt.Sprintf("left=%s right=%s", join.Left, join.Right)
		rows = append(rows,
			ExecutionInspectionRow{Section: "join", Name: prefix + ".join_kind", Value: join.JoinKind, Detail: detail},
			ExecutionInspectionRow{Section: "join", Name: prefix + ".sql_kind", Value: string(join.SQLKind)},
			ExecutionInspectionRow{Section: "join", Name: prefix + ".encoding", Value: string(join.EncodingKind)},
			ExecutionInspectionRow{Section: "join", Name: prefix + ".capabilities", Value: relationshipCapabilitiesValue(join.Capabilities)},
			ExecutionInspectionRow{Section: "join", Name: prefix + ".execution_status", Value: string(join.ExecutionStatus)},
		)
	}
	return rows
}

// ResultChunk returns inspection rows as a protocol-neutral result chunk.
func (i ExecutionInspection) ResultChunk(sequence int, final bool) qsbridge.ResultChunk {
	return qsbridge.ExecutionInspectionRowsChunk(i.Rows(), sequence, final)
}

// Rows returns stable adapter-facing rows for SQL inspection metadata.
func (r SQLInspectionResult) Rows() []ExecutionInspectionRow {
	query := r.Request.Bound.Prepared.Query
	rows := []ExecutionInspectionRow{
		{Section: "sql", Name: "supported", Value: strconv.FormatBool(r.Supported())},
		{Section: "sql", Name: "kind", Value: string(r.Prepared.Kind)},
		{Section: "sql", Name: "result_columns", Value: strconv.Itoa(len(r.Request.ResultColumns))},
		{Section: "intermediate", Name: "fragments", Value: strconv.Itoa(len(r.Intermediate.Fragments))},
		{Section: "intermediate", Name: "projections", Value: strconv.Itoa(len(r.Intermediate.ProjectionFields))},
	}
	rows = append(rows, queryShapeInspectionRows(query)...)
	rows = append(rows, queryMembershipInspectionRows(query.Memberships)...)
	rows = append(rows, quantaFilterInspectionRows(r.Intermediate.Filter)...)
	if !r.Intermediate.Filter.Empty() {
		rows = append(rows,
			ExecutionInspectionRow{Section: "filter", Name: "planned", Value: "true"},
			ExecutionInspectionRow{Section: "filter", Name: "execution_capability", Value: strconv.FormatBool(r.FilterExecutionEnabled)},
		)
	}
	if !executionInspectionEmpty(r.Runtime) {
		rows = append(rows, r.Runtime.Rows()...)
	}
	rows = append(rows, inspectionDiagnosticRows(sqlOnlyInspectionDiagnostics(r.Diagnostics, r.Runtime.Diagnostics))...)
	return rows
}

func queryShapeInspectionRows(query qsbridge.QueryIR) []ExecutionInspectionRow {
	if query.Kind == "" {
		return nil
	}
	rows := []ExecutionInspectionRow{
		{Section: "query_shape", Name: "sources", Value: strconv.Itoa(len(query.Sources))},
		{Section: "query_shape", Name: "joins", Value: strconv.Itoa(len(query.Joins))},
		{Section: "query_shape", Name: "memberships", Value: strconv.Itoa(len(query.Memberships))},
		{Section: "query_shape", Name: "predicates", Value: strconv.Itoa(len(query.Predicates))},
		{Section: "query_shape", Name: "group_by", Value: strconv.Itoa(len(query.GroupBy))},
		{Section: "query_shape", Name: "aggregates", Value: strconv.Itoa(len(query.Aggregates))},
		{Section: "query_shape", Name: "having", Value: strconv.Itoa(len(query.Having))},
		{Section: "query_shape", Name: "order_by", Value: strconv.Itoa(len(query.OrderBy))},
		{Section: "query_shape", Name: "limit", Value: strconv.Itoa(query.Result.Limit)},
		{Section: "query_shape", Name: "offset", Value: strconv.Itoa(query.Result.Offset)},
		{Section: "query_shape", Name: "aggregate_functions", Value: aggregateFunctionSummary(query.Aggregates)},
		{Section: "query_shape", Name: "conditional_aggregates", Value: strconv.Itoa(conditionalAggregateCount(query.Aggregates))},
		{Section: "query_shape", Name: "arithmetic_aggregates", Value: strconv.Itoa(arithmeticAggregateCount(query.Aggregates))},
		{Section: "query_shape", Name: "distinct_aggregates", Value: strconv.Itoa(distinctAggregateCount(query.Aggregates))},
	}
	return rows
}

func queryMembershipInspectionRows(memberships []qsbridge.MembershipEdge) []ExecutionInspectionRow {
	rows := make([]ExecutionInspectionRow, 0, len(memberships)*6)
	for index, membership := range memberships {
		prefix := fmt.Sprintf("%03d", index+1)
		detail := fmt.Sprintf("left=%s right=%s", runtimeMembershipDisplay(membership.Left, membership.LeftTuple), runtimeMembershipDisplay(membership.Right, membership.RightTuple))
		rows = append(rows,
			ExecutionInspectionRow{Section: "membership", Name: prefix + ".kind", Value: string(membership.Kind), Detail: detail},
			ExecutionInspectionRow{Section: "membership", Name: prefix + ".direction", Value: string(membership.Direction)},
			ExecutionInspectionRow{Section: "membership", Name: prefix + ".encoding", Value: string(membership.Encoding.Kind)},
			ExecutionInspectionRow{Section: "membership", Name: prefix + ".capabilities", Value: relationshipCapabilitiesValue(membership.Encoding.Capabilities)},
			ExecutionInspectionRow{Section: "membership", Name: prefix + ".predicates", Value: strconv.Itoa(len(membership.Predicates))},
			ExecutionInspectionRow{Section: "membership", Name: prefix + ".supported", Value: strconv.FormatBool(membership.Supported())},
		)
	}
	return rows
}

func runtimeMembershipDisplay(fallback qsbridge.FieldRef, tuple []qsbridge.Expr) string {
	if len(tuple) == 0 {
		return fallback.QualifiedName()
	}
	parts := make([]string, 0, len(tuple))
	for _, expr := range tuple {
		parts = append(parts, runtimeMembershipExprDisplay(expr))
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func runtimeMembershipExprDisplay(expr qsbridge.Expr) string {
	switch typed := expr.(type) {
	case qsbridge.FieldExpr:
		return typed.Ref.QualifiedName()
	case *qsbridge.FieldExpr:
		if typed != nil {
			return typed.Ref.QualifiedName()
		}
	case qsbridge.LiteralExpr:
		return fmt.Sprint(typed.Value)
	case *qsbridge.LiteralExpr:
		if typed != nil {
			return fmt.Sprint(typed.Value)
		}
	}
	if expr == nil {
		return "<nil>"
	}
	return string(expr.ExpressionKind())
}

func aggregateFunctionSummary(aggregates []qsbridge.Aggregate) string {
	if len(aggregates) == 0 {
		return ""
	}
	counts := make(map[string]int)
	order := make([]string, 0, len(aggregates))
	for _, aggregate := range aggregates {
		name := strings.ToLower(aggregate.Function)
		if name == "" {
			name = "<unknown>"
		}
		if counts[name] == 0 {
			order = append(order, name)
		}
		counts[name]++
	}
	parts := make([]string, 0, len(order))
	for _, name := range order {
		parts = append(parts, fmt.Sprintf("%s=%d", name, counts[name]))
	}
	return strings.Join(parts, ",")
}

func conditionalAggregateCount(aggregates []qsbridge.Aggregate) int {
	count := 0
	for _, aggregate := range aggregates {
		if exprContainsSearchedCase(aggregate.Input) || exprContainsSearchedCase(aggregate.Filter) {
			count++
		}
	}
	return count
}

func arithmeticAggregateCount(aggregates []qsbridge.Aggregate) int {
	count := 0
	for _, aggregate := range aggregates {
		if exprContainsArithmetic(aggregate.Input) {
			count++
		}
	}
	return count
}

func distinctAggregateCount(aggregates []qsbridge.Aggregate) int {
	count := 0
	for _, aggregate := range aggregates {
		if aggregate.Mode == qsbridge.AggregateDistinct {
			count++
		}
	}
	return count
}

func exprContainsSearchedCase(expr qsbridge.Expr) bool {
	switch n := expr.(type) {
	case nil:
		return false
	case qsbridge.SearchedCaseExpr:
		return true
	case *qsbridge.SearchedCaseExpr:
		return true
	case qsbridge.BinaryExpr:
		return exprContainsSearchedCase(n.Left) || exprContainsSearchedCase(n.Right)
	case *qsbridge.BinaryExpr:
		return exprContainsSearchedCase(n.Left) || exprContainsSearchedCase(n.Right)
	case qsbridge.CallExpr:
		for _, arg := range n.Args {
			if exprContainsSearchedCase(arg) {
				return true
			}
		}
	case *qsbridge.CallExpr:
		for _, arg := range n.Args {
			if exprContainsSearchedCase(arg) {
				return true
			}
		}
	case qsbridge.ListExpr:
		for _, item := range n.Items {
			if exprContainsSearchedCase(item) {
				return true
			}
		}
	case *qsbridge.ListExpr:
		for _, item := range n.Items {
			if exprContainsSearchedCase(item) {
				return true
			}
		}
	}
	return false
}

func exprContainsArithmetic(expr qsbridge.Expr) bool {
	switch n := expr.(type) {
	case nil:
		return false
	case qsbridge.BinaryExpr:
		if arithmeticBinaryOp(n.Op) {
			return true
		}
		return exprContainsArithmetic(n.Left) || exprContainsArithmetic(n.Right)
	case *qsbridge.BinaryExpr:
		if arithmeticBinaryOp(n.Op) {
			return true
		}
		return exprContainsArithmetic(n.Left) || exprContainsArithmetic(n.Right)
	case qsbridge.SearchedCaseExpr:
		for _, when := range n.Whens {
			if exprContainsArithmetic(when.Condition) || exprContainsArithmetic(when.Result) {
				return true
			}
		}
		return exprContainsArithmetic(n.Else)
	case *qsbridge.SearchedCaseExpr:
		for _, when := range n.Whens {
			if exprContainsArithmetic(when.Condition) || exprContainsArithmetic(when.Result) {
				return true
			}
		}
		return exprContainsArithmetic(n.Else)
	case qsbridge.CallExpr:
		for _, arg := range n.Args {
			if exprContainsArithmetic(arg) {
				return true
			}
		}
	case *qsbridge.CallExpr:
		for _, arg := range n.Args {
			if exprContainsArithmetic(arg) {
				return true
			}
		}
	case qsbridge.ListExpr:
		for _, item := range n.Items {
			if exprContainsArithmetic(item) {
				return true
			}
		}
	case *qsbridge.ListExpr:
		for _, item := range n.Items {
			if exprContainsArithmetic(item) {
				return true
			}
		}
	}
	return false
}

func arithmeticBinaryOp(op qsbridge.BinaryOp) bool {
	switch op {
	case qsbridge.BinaryOpAdd, qsbridge.BinaryOpSubtract, qsbridge.BinaryOpMultiply, qsbridge.BinaryOpDivide:
		return true
	default:
		return false
	}
}

func executionInspectionEmpty(inspection ExecutionInspection) bool {
	return inspection.Route.Path == "" &&
		inspection.Route.Discovery == "" &&
		inspection.Route.Target == (RuntimeTarget{}) &&
		inspection.SelectedExecutor == "" &&
		inspection.RuntimeProfile == (RuntimeInspectionProfile{}) &&
		inspection.CallPlan.RootIndex == "" &&
		inspection.Shape == (ExecutionShapeInspection{}) &&
		filterDomainTranslationEmpty(inspection.FilterDomain) &&
		!inspection.FilterDomainPlan.Required() &&
		len(inspection.Joins) == 0 &&
		!inspection.JoinPlan.NeedsRelationshipVectorExecution() &&
		len(inspection.Diagnostics) == 0
}

func quantaFilterInspectionRows(filter qsbridge.QuantaFilterExpression) []ExecutionInspectionRow {
	if filter.Empty() {
		return nil
	}
	rows := []ExecutionInspectionRow{
		{Section: "intermediate", Name: "filter_nodes", Value: strconv.Itoa(quantaFilterNodeCount(filter))},
		{Section: "intermediate", Name: "filter_leaves", Value: strconv.Itoa(quantaFilterLeafCount(filter))},
		{Section: "intermediate", Name: "filter_depth", Value: strconv.Itoa(quantaFilterDepth(filter))},
	}
	rows = append(rows, quantaFilterTreeRows("root", filter)...)
	return rows
}

func quantaFilterTreeRows(name string, filter qsbridge.QuantaFilterExpression) []ExecutionInspectionRow {
	if filter.Empty() {
		return nil
	}
	row := ExecutionInspectionRow{
		Section: "filter",
		Name:    name,
		Value:   string(filter.Operation),
		Detail:  quantaFilterDetail(filter),
	}
	rows := []ExecutionInspectionRow{row}
	for index, child := range filter.Children {
		childName := fmt.Sprintf("%s.%d", name, index+1)
		rows = append(rows, quantaFilterTreeRows(childName, child)...)
	}
	return rows
}

func quantaFilterDetail(filter qsbridge.QuantaFilterExpression) string {
	if filter.Leaf() {
		return quantaFilterFragmentDetail(filter.Fragment)
	}
	return fmt.Sprintf("children=%d leaves=%d depth=%d", len(filter.Children), quantaFilterLeafCount(filter), quantaFilterDepth(filter))
}

func quantaFilterFragmentDetail(fragment qsbridge.QuantaQueryFragment) string {
	parts := []string{}
	if fragment.Index != "" || fragment.Field != "" {
		parts = append(parts, fragment.Index+"."+fragment.Field)
	}
	if fragment.Operation != "" {
		parts = append(parts, string(fragment.Operation))
	}
	if fragment.BSIOp != "" {
		parts = append(parts, string(fragment.BSIOp))
	}
	if fragment.Negate {
		parts = append(parts, "negate=true")
	}
	if fragment.NullCheck {
		parts = append(parts, "null_check=true")
	}
	return strings.Join(parts, " ")
}

func quantaFilterNodeCount(filter qsbridge.QuantaFilterExpression) int {
	if filter.Empty() {
		return 0
	}
	count := 1
	for _, child := range filter.Children {
		count += quantaFilterNodeCount(child)
	}
	return count
}

func quantaFilterLeafCount(filter qsbridge.QuantaFilterExpression) int {
	if filter.Empty() {
		return 0
	}
	if filter.Leaf() {
		return 1
	}
	count := 0
	for _, child := range filter.Children {
		count += quantaFilterLeafCount(child)
	}
	return count
}

func quantaFilterDepth(filter qsbridge.QuantaFilterExpression) int {
	if filter.Empty() {
		return 0
	}
	depth := 1
	for _, child := range filter.Children {
		childDepth := 1 + quantaFilterDepth(child)
		if childDepth > depth {
			depth = childDepth
		}
	}
	return depth
}

// ResultChunk returns SQL inspection rows as a protocol-neutral result chunk.
func (r SQLInspectionResult) ResultChunk(sequence int, final bool) qsbridge.ResultChunk {
	return qsbridge.ExecutionInspectionRowsChunk(r.Rows(), sequence, final)
}

func inspectionDiagnosticRows(diagnostics qsbridge.DiagnosticSet) []ExecutionInspectionRow {
	rows := make([]ExecutionInspectionRow, 0, len(diagnostics))
	for index, diagnostic := range diagnostics {
		rows = append(rows, ExecutionInspectionRow{
			Section: "diagnostic",
			Name:    fmt.Sprintf("%03d", index+1),
			Value:   string(diagnostic.Code),
			Detail:  diagnostic.Error(),
		})
	}
	return rows
}

func sqlOnlyInspectionDiagnostics(all qsbridge.DiagnosticSet, runtime qsbridge.DiagnosticSet) qsbridge.DiagnosticSet {
	remaining := diagnosticCodeCounts(runtime)
	filtered := make(qsbridge.DiagnosticSet, 0, len(all))
	for _, diagnostic := range all {
		key := string(diagnostic.Code) + "\x00" + diagnostic.Message
		if remaining[key] > 0 {
			remaining[key]--
			continue
		}
		filtered = append(filtered, diagnostic)
	}
	return filtered
}

func diagnosticCodeCounts(diagnostics qsbridge.DiagnosticSet) map[string]int {
	counts := make(map[string]int, len(diagnostics))
	for _, diagnostic := range diagnostics {
		counts[string(diagnostic.Code)+"\x00"+diagnostic.Message]++
	}
	return counts
}

func relationshipCapabilitiesValue(capabilities qsbridge.RelationshipCapabilities) string {
	values := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		values = append(values, string(capability))
	}
	return strings.Join(values, ",")
}

func runtimeTargetValue(target RuntimeTarget) string {
	parts := make([]string, 0, 3)
	if target.NodeID != "" {
		parts = append(parts, "node="+target.NodeID)
	}
	if target.Address != "" {
		parts = append(parts, "address="+target.Address)
	}
	if target.Local {
		parts = append(parts, "local=true")
	}
	return strings.Join(parts, " ")
}
