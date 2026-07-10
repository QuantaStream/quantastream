package qsbridge

import (
	"sort"
	"strings"
)

// ClientStatusVariable describes one adapter-visible status value.
type ClientStatusVariable struct {
	Name  string
	Value string
}

// ClientStatusVariablesSummaryRow describes aggregate SHOW STATUS-style metadata.
type ClientStatusVariablesSummaryRow struct {
	VariableCount         int
	EmptyValueCount       int
	NumericValueCount     int
	CommandStatusCount    int
	ThreadStatusCount     int
	ConnectionStatusCount int
}

// ClientStatusVariablesExchange is adapter-facing metadata for status introspection.
type ClientStatusVariablesExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Variables    []ClientStatusVariable
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientStatusVariables returns SHOW STATUS-style metadata supplied by the adapter.
func (s PlanningService) ListClientStatusVariables(connection ConnectionContext, variables []ClientStatusVariable, pattern string) ClientStatusVariablesExchange {
	_ = s
	exchange := ClientStatusVariablesExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.statusVariablesResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Variables = filterClientStatusVariables(variables, pattern)
	exchange.Result = exchange.statusVariablesResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether status variable metadata can be returned.
func (e ClientStatusVariablesExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts status variable diagnostics into protocol-facing errors.
func (e ClientStatusVariablesExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking status variable error, if any.
func (e ClientStatusVariablesExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientStatusVariablesExchange) statusVariablesResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     statusVariablesResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.statusVariableRows(),
		Final: true,
	})
}

func statusVariablesResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Variable_name", Type: DataTypeString},
		{Name: "Value", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientStatusVariablesExchange) statusVariableRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Variables))
	for _, variable := range e.Variables {
		rows = append(rows, ResultRow{
			metadataStringCell(variable.Name),
			metadataStringCell(variable.Value),
		})
	}
	return rows
}

func filterClientStatusVariables(variables []ClientStatusVariable, pattern string) []ClientStatusVariable {
	cloned := cloneClientStatusVariables(variables)
	sort.Slice(cloned, func(i, j int) bool {
		return cloned[i].Name < cloned[j].Name
	})
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloned
	}
	filtered := make([]ClientStatusVariable, 0, len(cloned))
	for _, variable := range cloned {
		if catalogFieldPatternMatch(pattern, variable.Name) {
			filtered = append(filtered, variable)
		}
	}
	return filtered
}

func cloneClientStatusVariables(variables []ClientStatusVariable) []ClientStatusVariable {
	if len(variables) == 0 {
		return nil
	}
	return append([]ClientStatusVariable(nil), variables...)
}

func summarizeClientStatusVariables(variables []ClientStatusVariable) ClientStatusVariablesSummaryRow {
	summary := ClientStatusVariablesSummaryRow{VariableCount: len(variables)}
	for _, variable := range variables {
		if variable.Value == "" {
			summary.EmptyValueCount++
		}
		if statusVariableNumericValue(variable.Value) {
			summary.NumericValueCount++
		}
		if strings.HasPrefix(variable.Name, "Com_") {
			summary.CommandStatusCount++
		}
		if strings.HasPrefix(variable.Name, "Threads_") {
			summary.ThreadStatusCount++
		}
		if strings.HasPrefix(variable.Name, "Connections") || strings.HasPrefix(variable.Name, "Connection_") {
			summary.ConnectionStatusCount++
		}
	}
	return summary
}

func statusVariableNumericValue(value string) bool {
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
