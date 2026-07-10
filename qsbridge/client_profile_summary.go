package qsbridge

// ClientExecutionProfileSummaryExchange is adapter-facing metadata for aggregate execution profile rows.
type ClientExecutionProfileSummaryExchange struct {
	Connection   ConnectionContext
	Profile      ExecutionProfile
	Row          ClientExecutionProfileSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// PrepareClientExecutionProfileSummary returns aggregate protocol-neutral explain/profile metadata.
func (s PlanningService) PrepareClientExecutionProfileSummary(connection ConnectionContext, profile ExecutionProfile) ClientExecutionProfileSummaryExchange {
	_ = s
	exchange := ClientExecutionProfileSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Profile:     cloneExecutionProfile(profile),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), profile.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeExecutionProfileRows(profile, executionProfileRows(profile))
	}
	exchange.Result = exchange.executionProfileSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether profile summary metadata can be returned.
func (e ClientExecutionProfileSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts profile summary diagnostics into protocol-facing errors.
func (e ClientExecutionProfileSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking profile summary error, if any.
func (e ClientExecutionProfileSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientExecutionProfileSummaryExchange) executionProfileSummaryResult() ExecutionResult {
	result := ExecutionResult{
		RequestID:   e.Profile.RequestID,
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     executionProfileSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
		Profile:     cloneExecutionProfile(e.Profile),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{executionProfileSummaryResultRow(e.Row)},
		Final: true,
	})
}

func executionProfileSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Row_count", Type: DataTypeInt},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Explain_count", Type: DataTypeInt},
		{Name: "Timing_count", Type: DataTypeInt},
		{Name: "Counter_count", Type: DataTypeInt},
		{Name: "Diagnostic_count", Type: DataTypeInt},
		{Name: "Trace_explain", Type: DataTypeBool},
		{Name: "Include_profile", Type: DataTypeBool},
	}
}

func executionProfileSummaryResultRow(row ClientExecutionProfileSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.RowCount),
		metadataStringCell(string(row.AccessIntent)),
		metadataStringCell(string(row.Lifecycle)),
		metadataIntCell(row.LifecycleSteps),
		metadataIntCell(row.ExplainCount),
		metadataIntCell(row.TimingCount),
		metadataIntCell(row.CounterCount),
		metadataIntCell(row.DiagnosticCount),
		metadataBoolCell(row.TraceExplain),
		metadataBoolCell(row.IncludeProfile),
	}
}
