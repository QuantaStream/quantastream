package qsbridge

// ClientPlanCacheKeyPart describes one component of a deterministic plan-cache key.
type ClientPlanCacheKeyPart struct {
	Part  string
	Name  string
	Value string
}

// ClientPlanCacheKeyExchange is client-facing plan-cache key metadata.
type ClientPlanCacheKeyExchange struct {
	Connection   ConnectionContext
	Key          PlanCacheKey
	Parts        []ClientPlanCacheKeyPart
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// InspectClientPlanCacheKey returns a deterministic breakdown of one plan-cache key.
func (s PlanningService) InspectClientPlanCacheKey(connection ConnectionContext, key PlanCacheKey) ClientPlanCacheKeyExchange {
	_ = s
	exchange := ClientPlanCacheKeyExchange{
		Connection:  cloneConnectionContext(connection),
		Key:         clonePlanCacheKey(key),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.planCacheKeyResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Parts = planCacheKeyParts(key)
	exchange.Result = exchange.planCacheKeyResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether plan-cache key metadata can be returned.
func (e ClientPlanCacheKeyExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts plan-cache key diagnostics into protocol-facing errors.
func (e ClientPlanCacheKeyExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking plan-cache key error, if any.
func (e ClientPlanCacheKeyExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientPlanCacheKeyExchange) planCacheKeyResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     planCacheKeyResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.planCacheKeyRows(),
		Final: true,
	})
}

func planCacheKeyResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Part", Type: DataTypeString},
		{Name: "Name", Type: DataTypeString, Nullable: true},
		{Name: "Value", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPlanCacheKeyExchange) planCacheKeyRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Parts))
	for _, part := range e.Parts {
		rows = append(rows, ResultRow{
			metadataStringCell(part.Part),
			metadataStringCell(part.Name),
			metadataStringCell(part.Value),
		})
	}
	return rows
}

func planCacheKeyParts(key PlanCacheKey) []ClientPlanCacheKeyPart {
	parts := []ClientPlanCacheKeyPart{
		{Part: "digest", Value: key.Digest},
		{Part: "sql", Value: key.SQL},
		{Part: "schema", Value: key.Schema},
		{Part: "catalog_version", Value: string(key.CatalogVersion)},
		{Part: "user", Value: string(key.User)},
	}
	for _, role := range sortedRoleNames(key.Roles) {
		parts = append(parts, ClientPlanCacheKeyPart{Part: "role", Value: string(role)})
	}
	for _, mode := range sortedSQLModes(key.SQLModes) {
		parts = append(parts, ClientPlanCacheKeyPart{Part: "sql_mode", Value: string(mode)})
	}
	parts = append(parts, ClientPlanCacheKeyPart{Part: "time_zone", Value: key.TimeZone})
	for _, name := range sortedVariableNames(key.Variables) {
		parts = append(parts, ClientPlanCacheKeyPart{Part: "variable", Name: name, Value: key.Variables[name]})
	}
	parts = append(parts, ClientPlanCacheKeyPart{Part: "shards_all", Value: boolCacheValue(key.Scope.Shards.All)})
	for _, shard := range sortedShardIDs(key.Scope.Shards.Shards) {
		parts = append(parts, ClientPlanCacheKeyPart{Part: "shard", Value: string(shard)})
	}
	for _, replica := range sortedReplicaIDs(key.Scope.Replicas) {
		parts = append(parts, ClientPlanCacheKeyPart{Part: "replica", Value: string(replica)})
	}
	parts = append(parts,
		ClientPlanCacheKeyPart{Part: "routing", Value: string(key.Scope.Routing)},
		ClientPlanCacheKeyPart{Part: "placement", Value: string(key.Scope.Placement)},
		ClientPlanCacheKeyPart{Part: "cache_scope", Value: string(key.Scope.Cache)},
	)
	return parts
}

func clonePlanCacheKey(key PlanCacheKey) PlanCacheKey {
	key.Roles = append([]RoleName(nil), key.Roles...)
	key.SQLModes = append([]SQLMode(nil), key.SQLModes...)
	key.Variables = cloneStringMap(key.Variables)
	key.Scope = clonePhysicalScope(key.Scope)
	return key
}
