package qsbridge

// ClientDriverCompatibilitySummaryExchange is adapter-facing client-driver summary metadata.
type ClientDriverCompatibilitySummaryExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Row          ClientDriverCompatibilitySummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientDriverCompatibility returns aggregate client-driver target metadata.
func (s PlanningService) SummarizeClientDriverCompatibility(connection ConnectionContext, pattern string) ClientDriverCompatibilitySummaryExchange {
	_ = s
	exchange := ClientDriverCompatibilitySummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = SummarizeClientDriverCompatibility(filterClientDriverCompatibility(DefaultClientDriverCompatibility(), pattern))
	}
	exchange.Result = exchange.driverCompatibilitySummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether driver compatibility summary metadata can be returned.
func (e ClientDriverCompatibilitySummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts driver compatibility summary diagnostics into protocol-facing errors.
func (e ClientDriverCompatibilitySummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking driver compatibility summary error, if any.
func (e ClientDriverCompatibilitySummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientDriverCompatibilitySummaryExchange) driverCompatibilitySummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     driverCompatibilitySummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{driverCompatibilitySummaryRow(e.Row)},
		Final: true,
	})
}

func driverCompatibilitySummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Profile_count", Type: DataTypeInt},
		{Name: "Mysql_protocol_count", Type: DataTypeInt},
		{Name: "Go_protocol_count", Type: DataTypeInt},
		{Name: "Grpc_protocol_count", Type: DataTypeInt},
		{Name: "Boundary_only_count", Type: DataTypeInt},
		{Name: "Typed_api_count", Type: DataTypeInt},
		{Name: "Prepared_statement_count", Type: DataTypeInt},
		{Name: "Batch_execution_count", Type: DataTypeInt},
		{Name: "Streaming_result_count", Type: DataTypeInt},
		{Name: "Cancellation_count", Type: DataTypeInt},
		{Name: "Structured_explain_count", Type: DataTypeInt},
		{Name: "Plan_cache_policy_count", Type: DataTypeInt},
		{Name: "Password_auth_target_count", Type: DataTypeInt},
		{Name: "Token_auth_target_count", Type: DataTypeInt},
	}
}

func driverCompatibilitySummaryRow(row ClientDriverCompatibilitySummary) ResultRow {
	return ResultRow{
		metadataIntCell(row.ProfileCount),
		metadataIntCell(row.MySQLProtocolCount),
		metadataIntCell(row.GoProtocolCount),
		metadataIntCell(row.GRPCProtocolCount),
		metadataIntCell(row.BoundaryOnlyCount),
		metadataIntCell(row.TypedAPICount),
		metadataIntCell(row.PreparedStatementCount),
		metadataIntCell(row.BatchExecutionCount),
		metadataIntCell(row.StreamingResultCount),
		metadataIntCell(row.CancellationCount),
		metadataIntCell(row.StructuredExplainCount),
		metadataIntCell(row.PlanCachePolicyCount),
		metadataIntCell(row.PasswordAuthTargetCount),
		metadataIntCell(row.TokenAuthTargetCount),
	}
}
