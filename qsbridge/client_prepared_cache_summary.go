package qsbridge

// ClientPreparedPlanCacheSummaryExchange is adapter-facing prepared-plan cache summary metadata.
type ClientPreparedPlanCacheSummaryExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Row          ClientPreparedPlanCacheSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientPreparedPlanCache returns aggregate metadata for cached prepared plans.
func (s PlanningService) SummarizeClientPreparedPlanCache(connection ConnectionContext, cache PreparedPlanCacheInspector, pattern string) ClientPreparedPlanCacheSummaryExchange {
	_ = s
	exchange := ClientPreparedPlanCacheSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.preparedPlanCacheSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if cache == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared plan cache inspection is not configured"),
		})
		exchange.Result = exchange.preparedPlanCacheSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Row = summarizePreparedPlanCacheEntries(filterPreparedPlanCacheEntries(cache.ListPreparedPlanCacheEntries(), pattern))
	exchange.Result = exchange.preparedPlanCacheSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepared-plan cache summary metadata can be returned.
func (e ClientPreparedPlanCacheSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts prepared-plan cache diagnostics into protocol-facing errors.
func (e ClientPreparedPlanCacheSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking prepared-plan cache error, if any.
func (e ClientPreparedPlanCacheSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientPreparedPlanCacheSummaryExchange) preparedPlanCacheSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedPlanCacheSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{preparedPlanCacheSummaryResultRow(e.Row)},
		Final: true,
	})
}

func preparedPlanCacheSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Entry_count", Type: DataTypeInt},
		{Name: "Named_statement_count", Type: DataTypeInt},
		{Name: "Supported_count", Type: DataTypeInt},
		{Name: "Unsupported_count", Type: DataTypeInt},
		{Name: "Read_intent_count", Type: DataTypeInt},
		{Name: "Write_intent_count", Type: DataTypeInt},
		{Name: "Select_lifecycle_count", Type: DataTypeInt},
		{Name: "Mutation_lifecycle_count", Type: DataTypeInt},
		{Name: "Parameter_count", Type: DataTypeInt},
		{Name: "Result_column_count", Type: DataTypeInt},
		{Name: "Primary_placement_count", Type: DataTypeInt},
		{Name: "Local_placement_count", Type: DataTypeInt},
		{Name: "Follower_placement_count", Type: DataTypeInt},
		{Name: "Session_cache_count", Type: DataTypeInt},
		{Name: "Distinct_schema_count", Type: DataTypeInt},
		{Name: "Distinct_user_count", Type: DataTypeInt},
	}
}

func preparedPlanCacheSummaryResultRow(row ClientPreparedPlanCacheSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.EntryCount),
		metadataIntCell(row.NamedStatementCount),
		metadataIntCell(row.SupportedCount),
		metadataIntCell(row.UnsupportedCount),
		metadataIntCell(row.ReadIntentCount),
		metadataIntCell(row.WriteIntentCount),
		metadataIntCell(row.SelectLifecycleCount),
		metadataIntCell(row.MutationLifecycleCount),
		metadataIntCell(row.ParameterCount),
		metadataIntCell(row.ResultColumnCount),
		metadataIntCell(row.PrimaryPlacementCount),
		metadataIntCell(row.LocalPlacementCount),
		metadataIntCell(row.FollowerPlacementCount),
		metadataIntCell(row.SessionCacheCount),
		metadataIntCell(row.DistinctSchemaCount),
		metadataIntCell(row.DistinctUserCount),
	}
}
