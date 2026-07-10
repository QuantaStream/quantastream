package qsbridge

// ClientPreparedStatementStatusSummaryExchange is adapter-facing prepared handle summary metadata.
type ClientPreparedStatementStatusSummaryExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Row                 ClientPreparedStatementStatusSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientPreparedStatements returns aggregate prepared handle inventory metadata.
func (s PlanningService) SummarizeClientPreparedStatements(connection ConnectionContext, registry PreparedStatementRegistry) ClientPreparedStatementStatusSummaryExchange {
	_ = s
	exchange := ClientPreparedStatementStatusSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
		exchange.Result = exchange.preparedStatementStatusSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if registry == nil {
		exchange.ExchangeDiagnostics = mergeDiagnosticSets(exchange.ExchangeDiagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared statement registry is not configured"),
		})
	} else {
		exchange.Row = summarizePreparedStatementRows(preparedStatementRows(registry.List()))
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.preparedStatementStatusSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepared handle summary metadata can be returned.
func (e ClientPreparedStatementStatusSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientPreparedStatementStatusSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientPreparedStatementStatusSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientPreparedStatementStatusSummaryExchange) preparedStatementStatusSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedStatementStatusSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{preparedStatementStatusSummaryResultRow(e.Row)},
		Final: true,
	})
}

func preparedStatementStatusSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Statement_count", Type: DataTypeInt},
		{Name: "Named_statement_count", Type: DataTypeInt},
		{Name: "Supported_count", Type: DataTypeInt},
		{Name: "Unsupported_count", Type: DataTypeInt},
		{Name: "Read_intent_count", Type: DataTypeInt},
		{Name: "Write_intent_count", Type: DataTypeInt},
		{Name: "Select_lifecycle_count", Type: DataTypeInt},
		{Name: "Mutation_lifecycle_count", Type: DataTypeInt},
		{Name: "Parameter_count", Type: DataTypeInt},
		{Name: "Result_column_count", Type: DataTypeInt},
		{Name: "Diagnostic_count", Type: DataTypeInt},
		{Name: "Primary_placement_count", Type: DataTypeInt},
		{Name: "Local_placement_count", Type: DataTypeInt},
		{Name: "Follower_placement_count", Type: DataTypeInt},
		{Name: "Session_cache_count", Type: DataTypeInt},
	}
}

func preparedStatementStatusSummaryResultRow(row ClientPreparedStatementStatusSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.StatementCount),
		metadataIntCell(row.NamedStatementCount),
		metadataIntCell(row.SupportedCount),
		metadataIntCell(row.UnsupportedCount),
		metadataIntCell(row.ReadIntentCount),
		metadataIntCell(row.WriteIntentCount),
		metadataIntCell(row.SelectLifecycleCount),
		metadataIntCell(row.MutationLifecycleCount),
		metadataIntCell(row.ParameterCount),
		metadataIntCell(row.ResultColumnCount),
		metadataIntCell(row.DiagnosticCount),
		metadataIntCell(row.PrimaryPlacementCount),
		metadataIntCell(row.LocalPlacementCount),
		metadataIntCell(row.FollowerPlacementCount),
		metadataIntCell(row.SessionCacheCount),
	}
}
