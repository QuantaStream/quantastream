package qsbridge

import "sort"

// ClientSessionVariable describes one adapter-visible session variable.
type ClientSessionVariable struct {
	Name  string
	Value string
}

// ClientSessionVariablesSummaryRow describes aggregate SHOW VARIABLES-style session metadata.
type ClientSessionVariablesSummaryRow struct {
	VariableCount        int
	BuiltInCount         int
	AdapterVariableCount int
	EmptyValueCount      int
	NumericValueCount    int
	SelectedSchemaCount  int
	SQLModeCount         int
	TimeZoneCount        int
}

// ClientSessionVariablesExchange is adapter-facing metadata for session variable introspection.
type ClientSessionVariablesExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Variables    []ClientSessionVariable
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientSessionVariables returns SHOW VARIABLES-style metadata for the current session.
func (s PlanningService) ListClientSessionVariables(connection ConnectionContext, pattern string) ClientSessionVariablesExchange {
	_ = s
	exchange := ClientSessionVariablesExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.sessionVariablesResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Variables = filterClientSessionVariables(sessionVariables(connection.Session), pattern)
	exchange.Result = exchange.sessionVariablesResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether session variable metadata can be returned.
func (e ClientSessionVariablesExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts session variable diagnostics into protocol-facing errors.
func (e ClientSessionVariablesExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking session variable error, if any.
func (e ClientSessionVariablesExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientSessionVariablesExchange) sessionVariablesResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     sessionVariablesResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.sessionVariableRows(),
		Final: true,
	})
}

func sessionVariablesResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Variable_name", Type: DataTypeString},
		{Name: "Value", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientSessionVariablesExchange) sessionVariableRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Variables))
	for _, variable := range e.Variables {
		rows = append(rows, ResultRow{
			metadataStringCell(variable.Name),
			metadataStringCell(variable.Value),
		})
	}
	return rows
}

func sessionVariables(session SessionContext) []ClientSessionVariable {
	values := map[string]string{
		"database":  session.CurrentSchema,
		"sql_mode":  joinSQLModes(session.SQLModes),
		"time_zone": session.TimeZone,
	}
	for name, value := range session.Variables {
		values[name] = value
	}

	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	variables := make([]ClientSessionVariable, 0, len(names))
	for _, name := range names {
		variables = append(variables, ClientSessionVariable{Name: name, Value: values[name]})
	}
	return variables
}

func filterClientSessionVariables(variables []ClientSessionVariable, pattern string) []ClientSessionVariable {
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloneClientSessionVariables(variables)
	}
	filtered := make([]ClientSessionVariable, 0, len(variables))
	for _, variable := range variables {
		if catalogFieldPatternMatch(pattern, variable.Name) {
			filtered = append(filtered, variable)
		}
	}
	return filtered
}

func cloneClientSessionVariables(variables []ClientSessionVariable) []ClientSessionVariable {
	if len(variables) == 0 {
		return nil
	}
	return append([]ClientSessionVariable(nil), variables...)
}

func summarizeClientSessionVariables(variables []ClientSessionVariable) ClientSessionVariablesSummaryRow {
	summary := ClientSessionVariablesSummaryRow{VariableCount: len(variables)}
	for _, variable := range variables {
		switch variable.Name {
		case "database":
			summary.BuiltInCount++
			if variable.Value != "" {
				summary.SelectedSchemaCount++
			}
		case "sql_mode":
			summary.BuiltInCount++
			if variable.Value != "" {
				summary.SQLModeCount++
			}
		case "time_zone":
			summary.BuiltInCount++
			if variable.Value != "" {
				summary.TimeZoneCount++
			}
		default:
			summary.AdapterVariableCount++
		}
		if variable.Value == "" {
			summary.EmptyValueCount++
		}
		if sessionVariableNumericValue(variable.Value) {
			summary.NumericValueCount++
		}
	}
	return summary
}

func sessionVariableNumericValue(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func joinSQLModes(modes []SQLMode) string {
	if len(modes) == 0 {
		return ""
	}
	values := make([]string, 0, len(modes))
	for _, mode := range modes {
		values = append(values, string(mode))
	}
	sort.Strings(values)
	joined := ""
	for i, value := range values {
		if i > 0 {
			joined += ","
		}
		joined += value
	}
	return joined
}
