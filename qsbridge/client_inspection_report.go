package qsbridge

import "strconv"

// ClientInspectionRow describes one adapter-visible planning inspection row.
type ClientInspectionRow struct {
	Category string
	Name     string
	Value    string
	Detail   string
}

// ClientInspectionReportExchange is adapter-facing query inspection metadata.
type ClientInspectionReportExchange struct {
	Connection   ConnectionContext
	Report       InspectionReport
	Rows         []ClientInspectionRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// PrepareClientInspectionReport returns protocol-neutral rows for a planning inspection report.
func (s PlanningService) PrepareClientInspectionReport(connection ConnectionContext, report InspectionReport) ClientInspectionReportExchange {
	_ = s
	exchange := ClientInspectionReportExchange{
		Connection:  cloneConnectionContext(connection),
		Report:      cloneInspectionReport(report),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), report.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = inspectionReportRows(report)
	}
	exchange.Result = exchange.inspectionReportResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether inspection metadata can be returned.
func (e ClientInspectionReportExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts inspection diagnostics into protocol-facing errors.
func (e ClientInspectionReportExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking inspection error, if any.
func (e ClientInspectionReportExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientInspectionReportExchange) inspectionReportResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     inspectionReportResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.inspectionReportRows(),
		Final: true,
	})
}

func inspectionReportResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Category", Type: DataTypeString},
		{Name: "Name", Type: DataTypeString},
		{Name: "Value", Type: DataTypeString, Nullable: true},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientInspectionReportExchange) inspectionReportRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(row.Category),
			metadataStringCell(row.Name),
			metadataStringCell(row.Value),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}

func inspectionReportRows(report InspectionReport) []ClientInspectionRow {
	rows := []ClientInspectionRow{
		{Category: "summary", Name: "kind", Value: string(report.Query.Kind)},
		{Category: "summary", Name: "supported", Value: boolCacheValue(report.Supported)},
		{Category: "summary", Name: "capabilities", Value: joinPlanCapabilities(report.Capabilities)},
		{Category: "summary", Name: "predicates", Value: strconv.Itoa(report.Query.Predicates)},
		{Category: "summary", Name: "joins", Value: strconv.Itoa(report.Query.Joins)},
		{Category: "summary", Name: "memberships", Value: strconv.Itoa(report.Query.Memberships)},
		{Category: "summary", Name: "group_by", Value: strconv.Itoa(report.Query.GroupBy)},
		{Category: "summary", Name: "aggregates", Value: strconv.Itoa(report.Query.Aggregates)},
		{Category: "summary", Name: "order_by", Value: strconv.Itoa(report.Query.OrderBy)},
		{Category: "summary", Name: "functions", Value: strconv.Itoa(report.Query.Functions)},
		{Category: "summary", Name: "native_blockers", Value: strconv.Itoa(report.Query.NativeBlockers)},
	}
	for _, source := range report.Query.Sources {
		rows = append(rows, ClientInspectionRow{Category: "source", Name: source})
	}
	for _, field := range report.Query.Fields {
		rows = append(rows, ClientInspectionRow{Category: "field", Name: field})
	}
	for _, access := range report.Query.Access {
		rows = append(rows, ClientInspectionRow{
			Category: "access",
			Name:     string(access.Privilege),
			Value:    access.Table,
			Detail:   joinStringValues(access.Fields),
		})
	}
	for _, encoding := range report.Query.FieldEncodings {
		rows = append(rows, ClientInspectionRow{
			Category: "encoding",
			Name:     encoding.Field,
			Value:    string(encoding.Kind),
			Detail:   inspectionEncodingDetail(encoding),
		})
	}
	for _, join := range report.Query.JoinEdges {
		rows = append(rows, ClientInspectionRow{
			Category: "join",
			Name:     string(join.Kind),
			Value:    join.Left + "=" + join.Right,
			Detail:   inspectionJoinDetail(join),
		})
	}
	for _, membership := range report.Query.MembershipEdges {
		rows = append(rows, ClientInspectionRow{
			Category: "membership",
			Name:     string(membership.Kind),
			Value:    membership.Left + "=" + membership.Right,
			Detail:   inspectionMembershipDetail(membership),
		})
	}
	for _, usage := range report.Query.FunctionUsages {
		rows = append(rows, ClientInspectionRow{
			Category: "function",
			Name:     usage.Name,
			Value:    string(usage.Context),
			Detail:   inspectionFunctionUsageDetail(usage),
		})
	}
	for _, blocker := range report.Query.Blockers {
		rows = append(rows, ClientInspectionRow{
			Category: "blocker",
			Name:     string(nativeBlockerCode(blocker)),
			Value:    string(blocker.Capability),
			Detail:   inspectionNativeBlockerDetail(blocker),
		})
	}
	if report.Query.Mutation.Kind != "" {
		rows = append(rows, ClientInspectionRow{
			Category: "mutation",
			Name:     string(report.Query.Mutation.Kind),
			Value:    report.Query.Mutation.Target,
			Detail:   inspectionMutationDetail(report.Query.Mutation),
		})
	}
	for _, diagnostic := range report.Diagnostics {
		rows = append(rows, ClientInspectionRow{
			Category: "diagnostic",
			Name:     string(diagnostic.Code),
			Value:    string(diagnostic.Severity),
			Detail:   diagnostic.Message,
		})
	}
	return rows
}

func inspectionEncodingDetail(encoding FieldEncodingInspection) string {
	parts := []string{
		"multiplicity=" + string(encoding.Multiplicity),
		"rehydration=" + string(encoding.Rehydration),
		"lookup=" + boolCacheValue(encoding.RequiresLookup),
		"searchable=" + boolCacheValue(encoding.Searchable),
	}
	if encoding.LegacyName != "" {
		parts = append(parts, "legacy="+encoding.LegacyName)
	}
	if len(encoding.PredicateCapabilities) > 0 {
		parts = append(parts, "predicates="+joinPredicateCapabilities(encoding.PredicateCapabilities))
	}
	if len(encoding.ProjectionCapabilities) > 0 {
		parts = append(parts, "projections="+joinProjectionCapabilities(encoding.ProjectionCapabilities))
	}
	return joinStringValues(parts)
}

func inspectionJoinDetail(join JoinInspection) string {
	return joinStringValues([]string{
		"direction=" + string(join.Direction),
		"nulls=" + string(join.Nulls),
		"cardinality=" + join.Cardinality,
		"legal=" + boolCacheValue(join.Legal),
		"encoding=" + string(join.EncodingKind),
		"capabilities=" + joinRelationshipCapabilities(RelationshipCapabilities(join.Capabilities)),
	})
}

func inspectionMembershipDetail(membership MembershipInspection) string {
	return joinStringValues([]string{
		"direction=" + string(membership.Direction),
		"cardinality=" + membership.Cardinality,
		"legal=" + boolCacheValue(membership.Legal),
		"encoding=" + string(membership.EncodingKind),
		"capabilities=" + joinRelationshipCapabilities(RelationshipCapabilities(membership.Capabilities)),
	})
}

func inspectionFunctionUsageDetail(usage FunctionUsage) string {
	return joinStringValues([]string{
		"origin=" + string(usage.Origin),
		"placement=" + string(usage.Placement),
		"return=" + string(usage.ReturnType),
		"deterministic=" + boolCacheValue(usage.Deterministic),
	})
}

func nativeBlockerCode(blocker NativeBlocker) DiagnosticCode {
	if blocker.Code == "" {
		return DiagnosticNativeBlocker
	}
	return blocker.Code
}

func inspectionNativeBlockerDetail(blocker NativeBlocker) string {
	phase := blocker.Phase
	if phase == "" {
		phase = PhaseClassify
	}
	parts := []string{"phase=" + string(phase)}
	if blocker.Reason != "" {
		parts = append(parts, "reason="+blocker.Reason)
	}
	if !blocker.Span.Empty() {
		parts = append(parts, "span="+sourceSpanDetail(blocker.Span))
	}
	return joinStringValues(parts)
}

func sourceSpanDetail(span SourceSpan) string {
	return joinStringValues([]string{
		"start_line=" + strconv.Itoa(span.StartLine),
		"start_col=" + strconv.Itoa(span.StartCol),
		"end_line=" + strconv.Itoa(span.EndLine),
		"end_col=" + strconv.Itoa(span.EndCol),
	})
}

func inspectionMutationDetail(mutation MutationInspection) string {
	return joinStringValues([]string{
		"columns=" + joinStringValues(mutation.Columns),
		"rows=" + strconv.Itoa(mutation.Rows),
		"assignments=" + strconv.Itoa(mutation.Assignments),
		"predicates=" + strconv.Itoa(mutation.Predicates),
	})
}

func joinPredicateCapabilities(capabilities []PredicateCapability) string {
	values := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		values = append(values, string(capability))
	}
	return joinStringValues(values)
}

func joinProjectionCapabilities(capabilities []ProjectionCapability) string {
	values := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		values = append(values, string(capability))
	}
	return joinStringValues(values)
}
