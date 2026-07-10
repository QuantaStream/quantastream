package qsbridge

import "sort"

// ClientMetadataStoreSummaryExchange is adapter-facing metadata-store boundary information.
type ClientMetadataStoreSummaryExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Profiles     []MetadataStoreProfile
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientMetadataStoreSummary returns metadata persistence/cache boundaries.
func (s PlanningService) ListClientMetadataStoreSummary(connection ConnectionContext, pattern string) ClientMetadataStoreSummaryExchange {
	_ = s
	exchange := ClientMetadataStoreSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Profiles = filterMetadataStoreProfiles(DefaultMetadataStoreProfiles(), pattern)
	}
	exchange.Result = exchange.metadataStoreSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether metadata-store summary rows can be returned.
func (e ClientMetadataStoreSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts metadata-store diagnostics into protocol-facing errors.
func (e ClientMetadataStoreSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking metadata-store summary error, if any.
func (e ClientMetadataStoreSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientMetadataStoreSummaryExchange) metadataStoreSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     metadataStoreSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.metadataStoreSummaryRows(),
		Final: true,
	})
}

func metadataStoreSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Name", Type: DataTypeString},
		{Name: "Domain", Type: DataTypeString},
		{Name: "Backend", Type: DataTypeString, Nullable: true},
		{Name: "Cache_required", Type: DataTypeBool},
		{Name: "Cache_scope", Type: DataTypeString, Nullable: true},
		{Name: "Invalidation", Type: DataTypeString, Nullable: true},
		{Name: "Mutable", Type: DataTypeBool},
		{Name: "Distributed", Type: DataTypeBool},
		{Name: "Node_local_copy", Type: DataTypeBool},
		{Name: "Adapter_owned", Type: DataTypeBool},
		{Name: "Runtime_owned", Type: DataTypeBool},
		{Name: "Consistency_note", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientMetadataStoreSummaryExchange) metadataStoreSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Profiles))
	for _, profile := range e.Profiles {
		rows = append(rows, ResultRow{
			metadataStringCell(profile.Name),
			metadataStringCell(string(profile.Domain)),
			metadataStringCell(string(profile.Backend)),
			metadataBoolCell(profile.CacheRequired),
			metadataStringCell(string(profile.CacheScope)),
			metadataStringCell(string(profile.Invalidation)),
			metadataBoolCell(profile.Mutable),
			metadataBoolCell(profile.Distributed),
			metadataBoolCell(profile.NodeLocalCopy),
			metadataBoolCell(profile.AdapterOwned),
			metadataBoolCell(profile.RuntimeOwned),
			metadataStringCell(profile.ConsistencyNote),
		})
	}
	return rows
}

func filterMetadataStoreProfiles(profiles []MetadataStoreProfile, pattern string) []MetadataStoreProfile {
	cloned := cloneMetadataStoreProfiles(profiles)
	sort.SliceStable(cloned, func(i, j int) bool {
		if cloned[i].Domain != cloned[j].Domain {
			return cloned[i].Domain < cloned[j].Domain
		}
		return cloned[i].Name < cloned[j].Name
	})
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloned
	}
	filtered := make([]MetadataStoreProfile, 0, len(cloned))
	for _, profile := range cloned {
		if metadataStoreProfileMatchesPattern(profile, pattern) {
			filtered = append(filtered, profile)
		}
	}
	return filtered
}

func metadataStoreProfileMatchesPattern(profile MetadataStoreProfile, pattern string) bool {
	return catalogFieldPatternMatch(pattern, profile.Name) ||
		catalogFieldPatternMatch(pattern, string(profile.Domain)) ||
		catalogFieldPatternMatch(pattern, string(profile.Backend)) ||
		catalogFieldPatternMatch(pattern, profile.ConsistencyNote)
}
