package qsbridge

import "strconv"

// ClientExplainRow describes one adapter-visible structured explain row.
type ClientExplainRow struct {
	Section ExplainSection
	Name    string
	Value   string
	Detail  string
}

// ClientExplainSummaryRow describes aggregate structured explain metadata.
type ClientExplainSummaryRow struct {
	RowCount              int
	SelectedSectionCount  int
	AccessIntent          PhysicalAccessIntent
	Lifecycle             ClientPlanLifecycleKind
	LifecycleSteps        int
	LogicalCount          int
	PhysicalCount         int
	OptimizerCount        int
	OptimizerSummaryCount int
	DiagnosticCount       int
	FunctionCount         int
	NativeBlockerCount    int
	Supported             bool
}

// ClientExplainBundleExchange is adapter-facing structured explain metadata.
type ClientExplainBundleExchange struct {
	Connection   ConnectionContext
	Bundle       ExplainBundle
	Rows         []ClientExplainRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// PrepareClientExplainBundle returns protocol-neutral structured explain rows.
func (s PlanningService) PrepareClientExplainBundle(connection ConnectionContext, bundle ExplainBundle) ClientExplainBundleExchange {
	_ = s
	exchange := ClientExplainBundleExchange{
		Connection:  cloneConnectionContext(connection),
		Bundle:      cloneExplainBundle(bundle),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), bundle.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = explainBundleRows(bundle)
	}
	exchange.Result = exchange.explainBundleResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether structured explain metadata can be returned.
func (e ClientExplainBundleExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts structured explain diagnostics into protocol-facing errors.
func (e ClientExplainBundleExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking structured explain error, if any.
func (e ClientExplainBundleExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientExplainBundleExchange) explainBundleResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     explainBundleResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.explainBundleResultRows(),
		Final: true,
	})
}

func explainBundleResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Section", Type: DataTypeString},
		{Name: "Name", Type: DataTypeString},
		{Name: "Value", Type: DataTypeString, Nullable: true},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientExplainBundleExchange) explainBundleResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Section)),
			metadataStringCell(row.Name),
			metadataStringCell(row.Value),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}

func explainBundleRows(bundle ExplainBundle) []ClientExplainRow {
	rows := []ClientExplainRow{
		{Section: "summary", Name: "supported", Value: boolCacheValue(bundle.Supported)},
		{Section: "summary", Name: "access_intent", Value: string(bundle.AccessIntent)},
		{Section: "summary", Name: "lifecycle", Value: string(bundle.Lifecycle)},
		{Section: "summary", Name: "lifecycle_steps", Value: strconv.Itoa(bundle.LifecycleSteps)},
		{Section: "summary", Name: "sections", Value: joinExplainSections(bundle.Sections)},
	}
	for _, node := range bundle.Logical.Nodes {
		rows = append(rows, ClientExplainRow{
			Section: ExplainSectionLogical,
			Name:    string(node.Kind),
			Value:   node.Summary,
			Detail:  explainNodeDetail(node.ID, node.ParentID, node.Depth, node.Source),
		})
	}
	for _, node := range bundle.Physical.Nodes {
		rows = append(rows, ClientExplainRow{
			Section: ExplainSectionPhysical,
			Name:    string(node.Kind),
			Value:   node.Summary,
			Detail:  explainPhysicalNodeDetail(node),
		})
	}
	for _, rewrite := range bundle.Optimization.Rewrites {
		rows = append(rows, ClientExplainRow{
			Section: ExplainSectionOptimizer,
			Name:    string(rewrite.Rule),
			Value:   string(rewrite.Status),
			Detail:  explainRewriteDetail(rewrite),
		})
	}
	if bundle.Options.Includes(ExplainSectionOptimizerSummary) {
		rows = append(rows, ClientExplainRow{
			Section: ExplainSectionOptimizerSummary,
			Name:    "summary",
			Value:   boolCacheValue(bundle.OptimizationSummary.Supported),
			Detail:  explainOptimizerSummaryDetail(bundle.OptimizationSummary),
		})
	}
	for _, diagnostic := range bundle.Diagnostics {
		rows = append(rows, ClientExplainRow{
			Section: ExplainSectionDiagnostics,
			Name:    string(diagnostic.Code),
			Value:   string(diagnostic.Severity),
			Detail:  diagnostic.Message,
		})
	}
	for _, usage := range bundle.FunctionUsages {
		rows = append(rows, ClientExplainRow{
			Section: ExplainSectionFunctions,
			Name:    usage.Name,
			Value:   string(usage.Context),
			Detail:  inspectionFunctionUsageDetail(usage),
		})
	}
	for _, blocker := range bundle.NativeBlockers {
		rows = append(rows, ClientExplainRow{
			Section: ExplainSectionNativeBlockers,
			Name:    string(nativeBlockerCode(blocker)),
			Value:   string(blocker.Capability),
			Detail:  inspectionNativeBlockerDetail(blocker),
		})
	}
	return rows
}

func explainOptimizerSummaryDetail(summary OptimizationSummary) string {
	return joinStringValues([]string{
		"total=" + strconv.Itoa(summary.Total),
		"applied=" + strconv.Itoa(summary.Applied),
		"advisory=" + strconv.Itoa(summary.Advisory),
		"blocked=" + strconv.Itoa(summary.Blocked),
		"skipped=" + strconv.Itoa(summary.Skipped),
		"diagnostics=" + strconv.Itoa(summary.Diagnostics),
		"blocking=" + strconv.Itoa(summary.Blocking),
		"compatibility=" + strconv.Itoa(summary.Compatibility),
		"performance=" + strconv.Itoa(summary.Performance),
		"physical=" + strconv.Itoa(summary.Physical),
		"safety=" + strconv.Itoa(summary.Safety),
		"logical_impact=" + strconv.Itoa(summary.LogicalImpact),
		"physical_impact=" + strconv.Itoa(summary.PhysicalImpact),
		"diagnostic_only=" + strconv.Itoa(summary.DiagnosticOnly),
		"no_impact=" + strconv.Itoa(summary.NoImpact),
	})
}

func explainNodeDetail(id int, parentID int, depth int, source string) string {
	parts := []string{
		"id=" + strconv.Itoa(id),
		"parent=" + strconv.Itoa(parentID),
		"depth=" + strconv.Itoa(depth),
	}
	if source != "" {
		parts = append(parts, "source="+source)
	}
	return joinStringValues(parts)
}

func explainPhysicalNodeDetail(node PhysicalNodeExplanation) string {
	parts := []string{
		"id=" + strconv.Itoa(node.ID),
		"parent=" + strconv.Itoa(node.ParentID),
		"depth=" + strconv.Itoa(node.Depth),
	}
	if node.Source != "" {
		parts = append(parts, "source="+node.Source)
	}
	if len(node.Strategies) > 0 {
		parts = append(parts, "strategies="+joinPhysicalStrategies(node.Strategies))
	}
	return joinStringValues(parts)
}

func joinPhysicalStrategies(strategies []PhysicalStrategy) string {
	values := make([]string, 0, len(strategies))
	for _, strategy := range strategies {
		values = append(values, string(strategy))
	}
	return joinStringValues(values)
}

func explainRewriteDetail(rewrite RewriteRecord) string {
	return joinStringValues([]string{
		"category=" + string(rewrite.Category),
		"impact=" + string(rewrite.Impact),
		"reason=" + rewrite.Reason,
		"before=" + rewrite.Before,
		"after=" + rewrite.After,
		"capabilities=" + joinPlanCapabilities(rewrite.Capabilities),
		"diagnostics=" + joinDiagnosticCodes(diagnosticCodes(rewrite.Diagnostics)),
		"fields=" + joinStringValues(qualifiedFieldNames(rewrite.Fields)),
	})
}

func joinExplainSections(sections []ExplainSection) string {
	values := make([]string, 0, len(sections))
	for _, section := range sections {
		values = append(values, string(section))
	}
	return joinStringValues(values)
}

func cloneExplainBundle(bundle ExplainBundle) ExplainBundle {
	bundle.Sections = append([]ExplainSection(nil), bundle.Sections...)
	bundle.Logical = clonePlanExplanation(bundle.Logical)
	bundle.Physical = clonePhysicalPlanExplanation(bundle.Physical)
	bundle.Optimization = bundle.Optimization.Clone()
	bundle.Diagnostics = cloneDiagnosticSet(bundle.Diagnostics)
	bundle.FunctionUsages = append([]FunctionUsage(nil), bundle.FunctionUsages...)
	bundle.NativeBlockers = append([]NativeBlocker(nil), bundle.NativeBlockers...)
	return bundle
}

func summarizeClientExplainRows(bundle ExplainBundle, rows []ClientExplainRow) ClientExplainSummaryRow {
	summary := ClientExplainSummaryRow{
		RowCount:             len(rows),
		SelectedSectionCount: len(bundle.Sections),
		AccessIntent:         bundle.AccessIntent,
		Lifecycle:            bundle.Lifecycle,
		LifecycleSteps:       bundle.LifecycleSteps,
		Supported:            bundle.Supported,
	}
	for _, row := range rows {
		switch row.Section {
		case ExplainSectionLogical:
			summary.LogicalCount++
		case ExplainSectionPhysical:
			summary.PhysicalCount++
		case ExplainSectionOptimizer:
			summary.OptimizerCount++
		case ExplainSectionOptimizerSummary:
			summary.OptimizerSummaryCount++
		case ExplainSectionDiagnostics:
			summary.DiagnosticCount++
		case ExplainSectionFunctions:
			summary.FunctionCount++
		case ExplainSectionNativeBlockers:
			summary.NativeBlockerCount++
		}
	}
	return summary
}
