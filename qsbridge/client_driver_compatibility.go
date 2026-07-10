package qsbridge

import "sort"

// ClientDriverCompatibilityExchange is adapter-facing client-driver target metadata.
type ClientDriverCompatibilityExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Profiles     []ClientDriverCompatibility
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientDriverCompatibility returns intended driver compatibility targets.
func (s PlanningService) ListClientDriverCompatibility(connection ConnectionContext, pattern string) ClientDriverCompatibilityExchange {
	_ = s
	exchange := ClientDriverCompatibilityExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Profiles = filterClientDriverCompatibility(DefaultClientDriverCompatibility(), pattern)
	}
	exchange.Result = exchange.driverCompatibilityResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether driver compatibility metadata can be returned.
func (e ClientDriverCompatibilityExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts driver compatibility diagnostics into protocol-facing errors.
func (e ClientDriverCompatibilityExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking driver compatibility error, if any.
func (e ClientDriverCompatibilityExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientDriverCompatibilityExchange) driverCompatibilityResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     driverCompatibilityResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.driverCompatibilityRows(),
		Final: true,
	})
}

func driverCompatibilityResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Name", Type: DataTypeString},
		{Name: "Ecosystem", Type: DataTypeString},
		{Name: "Protocol", Type: DataTypeString},
		{Name: "Status", Type: DataTypeString},
		{Name: "Drivers", Type: DataTypeString, Nullable: true},
		{Name: "Capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Auth_plugins", Type: DataTypeString, Nullable: true},
		{Name: "Notes", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientDriverCompatibilityExchange) driverCompatibilityRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Profiles))
	for _, profile := range e.Profiles {
		rows = append(rows, ResultRow{
			metadataStringCell(profile.Name),
			metadataStringCell(string(profile.Ecosystem)),
			metadataStringCell(string(profile.Protocol)),
			metadataStringCell(string(profile.Status)),
			metadataStringCell(joinStringValues(profile.Drivers)),
			metadataStringCell(joinProtocolCapabilities(profile.Capabilities)),
			metadataStringCell(joinAuthenticationPlugins(profile.AuthPlugins)),
			metadataStringCell(profile.Notes),
		})
	}
	return rows
}

func filterClientDriverCompatibility(profiles []ClientDriverCompatibility, pattern string) []ClientDriverCompatibility {
	cloned := cloneClientDriverCompatibility(profiles)
	sort.SliceStable(cloned, func(i, j int) bool {
		if cloned[i].Ecosystem != cloned[j].Ecosystem {
			return cloned[i].Ecosystem < cloned[j].Ecosystem
		}
		return cloned[i].Name < cloned[j].Name
	})
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloned
	}
	filtered := make([]ClientDriverCompatibility, 0, len(cloned))
	for _, profile := range cloned {
		if driverCompatibilityMatchesPattern(profile, pattern) {
			filtered = append(filtered, profile)
		}
	}
	return filtered
}

func driverCompatibilityMatchesPattern(profile ClientDriverCompatibility, pattern string) bool {
	if catalogFieldPatternMatch(pattern, profile.Name) ||
		catalogFieldPatternMatch(pattern, string(profile.Ecosystem)) ||
		catalogFieldPatternMatch(pattern, string(profile.Protocol)) {
		return true
	}
	for _, driver := range profile.Drivers {
		if catalogFieldPatternMatch(pattern, driver) {
			return true
		}
	}
	return false
}

func joinAuthenticationPlugins(plugins []AuthenticationPlugin) string {
	values := make([]string, 0, len(plugins))
	for _, plugin := range plugins {
		values = append(values, string(plugin))
	}
	return joinStringValues(values)
}
