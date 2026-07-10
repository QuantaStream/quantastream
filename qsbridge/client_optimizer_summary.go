package qsbridge

// ClientOptimizerSummaryExchange is adapter-facing aggregate optimizer metadata.
type ClientOptimizerSummaryExchange struct {
	Connection   ConnectionContext
	Trace        OptimizationTrace
	Summary      OptimizationSummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// PrepareClientOptimizerSummary returns one protocol-neutral optimizer summary row.
func (s PlanningService) PrepareClientOptimizerSummary(connection ConnectionContext, trace OptimizationTrace) ClientOptimizerSummaryExchange {
	_ = s
	exchange := ClientOptimizerSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Trace:       trace.Clone(),
		Summary:     trace.Summary(),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), trace.Diagnostics),
	}
	exchange.Result = exchange.optimizerSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether optimizer summary metadata can be returned.
func (e ClientOptimizerSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts optimizer summary diagnostics into protocol-facing errors.
func (e ClientOptimizerSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking optimizer summary error, if any.
func (e ClientOptimizerSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientOptimizerSummaryExchange) optimizerSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     optimizerSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows: []ResultRow{{
			metadataBoolCell(e.Summary.Supported),
			metadataIntCell(e.Summary.Total),
			metadataIntCell(e.Summary.Applied),
			metadataIntCell(e.Summary.Advisory),
			metadataIntCell(e.Summary.Blocked),
			metadataIntCell(e.Summary.Skipped),
			metadataIntCell(e.Summary.Diagnostics),
			metadataIntCell(e.Summary.Blocking),
			metadataIntCell(e.Summary.Compatibility),
			metadataIntCell(e.Summary.Performance),
			metadataIntCell(e.Summary.Physical),
			metadataIntCell(e.Summary.Safety),
			metadataIntCell(e.Summary.LogicalImpact),
			metadataIntCell(e.Summary.PhysicalImpact),
			metadataIntCell(e.Summary.DiagnosticOnly),
			metadataIntCell(e.Summary.NoImpact),
		}},
		Final: true,
	})
}

func optimizerSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Total", Type: DataTypeInt},
		{Name: "Applied", Type: DataTypeInt},
		{Name: "Advisory", Type: DataTypeInt},
		{Name: "Blocked", Type: DataTypeInt},
		{Name: "Skipped", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeInt},
		{Name: "Blocking", Type: DataTypeInt},
		{Name: "Compatibility", Type: DataTypeInt},
		{Name: "Performance", Type: DataTypeInt},
		{Name: "Physical", Type: DataTypeInt},
		{Name: "Safety", Type: DataTypeInt},
		{Name: "Logical_impact", Type: DataTypeInt},
		{Name: "Physical_impact", Type: DataTypeInt},
		{Name: "Diagnostic_only", Type: DataTypeInt},
		{Name: "No_impact", Type: DataTypeInt},
	}
}
