package qsbridge

// ClientPreparedPlanCacheExchange is adapter-facing prepared-plan cache metadata.
type ClientPreparedPlanCacheExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Entries      []PreparedPlanCacheEntry
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ClientPreparedPlanCacheSummaryRow describes aggregate prepared-plan cache metadata.
type ClientPreparedPlanCacheSummaryRow struct {
	EntryCount             int
	NamedStatementCount    int
	SupportedCount         int
	UnsupportedCount       int
	ReadIntentCount        int
	WriteIntentCount       int
	SelectLifecycleCount   int
	MutationLifecycleCount int
	ParameterCount         int
	ResultColumnCount      int
	PrimaryPlacementCount  int
	LocalPlacementCount    int
	FollowerPlacementCount int
	SessionCacheCount      int
	DistinctSchemaCount    int
	DistinctUserCount      int
}

// ListClientPreparedPlanCache returns metadata for cached prepared plans.
func (s PlanningService) ListClientPreparedPlanCache(connection ConnectionContext, cache PreparedPlanCacheInspector, pattern string) ClientPreparedPlanCacheExchange {
	_ = s
	exchange := ClientPreparedPlanCacheExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.preparedPlanCacheResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if cache == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared plan cache inspection is not configured"),
		})
		exchange.Result = exchange.preparedPlanCacheResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Entries = filterPreparedPlanCacheEntries(cache.ListPreparedPlanCacheEntries(), pattern)
	exchange.Result = exchange.preparedPlanCacheResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepared-plan cache metadata can be returned.
func (e ClientPreparedPlanCacheExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts prepared-plan cache diagnostics into protocol-facing errors.
func (e ClientPreparedPlanCacheExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking prepared-plan cache error, if any.
func (e ClientPreparedPlanCacheExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientPreparedPlanCacheExchange) preparedPlanCacheResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedPlanCacheResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.preparedPlanCacheRows(),
		Final: true,
	})
}

func preparedPlanCacheResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Digest", Type: DataTypeString},
		{Name: "Statement_id", Type: DataTypeInt},
		{Name: "Statement_name", Type: DataTypeString, Nullable: true},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "Catalog_version", Type: DataTypeString, Nullable: true},
		{Name: "User", Type: DataTypeString, Nullable: true},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Parameters", Type: DataTypeInt},
		{Name: "Result_columns", Type: DataTypeInt},
		{Name: "Placement", Type: DataTypeString, Nullable: true},
		{Name: "Cache_scope", Type: DataTypeString, Nullable: true},
		{Name: "SQL", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPreparedPlanCacheExchange) preparedPlanCacheRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Entries))
	for _, entry := range e.Entries {
		rows = append(rows, ResultRow{
			metadataStringCell(entry.Key.Digest),
			metadataIntCell(int(entry.Handle.ID)),
			metadataStringCell(entry.Handle.Name),
			metadataStringCell(entry.Schema),
			metadataStringCell(string(entry.CatalogVersion)),
			metadataStringCell(string(entry.User)),
			metadataStringCell(string(entry.Kind)),
			metadataStringCell(string(entry.AccessIntent)),
			metadataStringCell(string(entry.Lifecycle)),
			metadataIntCell(entry.LifecycleSteps),
			metadataBoolCell(entry.Supported),
			metadataIntCell(entry.ParameterCount),
			metadataIntCell(entry.ResultColumnCount),
			metadataStringCell(string(entry.Scope.Placement)),
			metadataStringCell(string(entry.Scope.Cache)),
			metadataStringCell(entry.SQL),
		})
	}
	return rows
}

func filterPreparedPlanCacheEntries(entries []PreparedPlanCacheEntry, pattern string) []PreparedPlanCacheEntry {
	cloned := clonePreparedPlanCacheEntries(entries)
	sortPreparedPlanCacheEntries(cloned)
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloned
	}
	filtered := make([]PreparedPlanCacheEntry, 0, len(cloned))
	for _, entry := range cloned {
		if catalogFieldPatternMatch(pattern, entry.SQL) ||
			catalogFieldPatternMatch(pattern, entry.Schema) ||
			catalogFieldPatternMatch(pattern, string(entry.User)) ||
			catalogFieldPatternMatch(pattern, entry.Handle.Name) ||
			catalogFieldPatternMatch(pattern, entry.Key.Digest) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func summarizePreparedPlanCacheEntries(entries []PreparedPlanCacheEntry) ClientPreparedPlanCacheSummaryRow {
	summary := ClientPreparedPlanCacheSummaryRow{EntryCount: len(entries)}
	schemas := make(map[string]struct{})
	users := make(map[UserName]struct{})
	for _, entry := range entries {
		if entry.Handle.Name != "" {
			summary.NamedStatementCount++
		}
		if entry.Supported {
			summary.SupportedCount++
		} else {
			summary.UnsupportedCount++
		}
		switch entry.AccessIntent {
		case PhysicalAccessRead:
			summary.ReadIntentCount++
		case PhysicalAccessWrite:
			summary.WriteIntentCount++
		}
		switch entry.Lifecycle {
		case ClientPlanLifecycleSelect:
			summary.SelectLifecycleCount++
		case ClientPlanLifecycleMutation:
			summary.MutationLifecycleCount++
		}
		summary.ParameterCount += entry.ParameterCount
		summary.ResultColumnCount += entry.ResultColumnCount
		switch entry.Scope.Placement {
		case PlacementPrimary:
			summary.PrimaryPlacementCount++
		case PlacementLocal:
			summary.LocalPlacementCount++
		case PlacementFollower:
			summary.FollowerPlacementCount++
		}
		if entry.Scope.Cache == CacheSession {
			summary.SessionCacheCount++
		}
		if entry.Schema != "" {
			schemas[entry.Schema] = struct{}{}
		}
		if entry.User != "" {
			users[entry.User] = struct{}{}
		}
	}
	summary.DistinctSchemaCount = len(schemas)
	summary.DistinctUserCount = len(users)
	return summary
}
