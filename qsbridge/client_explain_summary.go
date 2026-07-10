package qsbridge

// ClientExplainSummaryExchange is adapter-facing metadata for aggregate structured explain rows.
type ClientExplainSummaryExchange struct {
	Connection   ConnectionContext
	Bundle       ExplainBundle
	Row          ClientExplainSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// PrepareClientExplainSummary returns aggregate protocol-neutral structured explain metadata.
func (s PlanningService) PrepareClientExplainSummary(connection ConnectionContext, bundle ExplainBundle) ClientExplainSummaryExchange {
	_ = s
	exchange := ClientExplainSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Bundle:      cloneExplainBundle(bundle),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), bundle.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeClientExplainRows(bundle, explainBundleRows(bundle))
	}
	exchange.Result = exchange.explainSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether structured explain summary metadata can be returned.
func (e ClientExplainSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts structured explain summary diagnostics into protocol-facing errors.
func (e ClientExplainSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking structured explain summary error, if any.
func (e ClientExplainSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientExplainSummaryExchange) explainSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     explainSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{explainSummaryResultRow(e.Row)},
		Final: true,
	})
}

func explainSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Row_count", Type: DataTypeInt},
		{Name: "Selected_section_count", Type: DataTypeInt},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Logical_count", Type: DataTypeInt},
		{Name: "Physical_count", Type: DataTypeInt},
		{Name: "Optimizer_count", Type: DataTypeInt},
		{Name: "Optimizer_summary_count", Type: DataTypeInt},
		{Name: "Diagnostic_count", Type: DataTypeInt},
		{Name: "Function_count", Type: DataTypeInt},
		{Name: "Native_blocker_count", Type: DataTypeInt},
		{Name: "Supported", Type: DataTypeBool},
	}
}

func explainSummaryResultRow(row ClientExplainSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.RowCount),
		metadataIntCell(row.SelectedSectionCount),
		metadataStringCell(string(row.AccessIntent)),
		metadataStringCell(string(row.Lifecycle)),
		metadataIntCell(row.LifecycleSteps),
		metadataIntCell(row.LogicalCount),
		metadataIntCell(row.PhysicalCount),
		metadataIntCell(row.OptimizerCount),
		metadataIntCell(row.OptimizerSummaryCount),
		metadataIntCell(row.DiagnosticCount),
		metadataIntCell(row.FunctionCount),
		metadataIntCell(row.NativeBlockerCount),
		metadataBoolCell(row.Supported),
	}
}
