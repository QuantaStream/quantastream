package qsbridge

// ClientStatisticsExchange is adapter-facing metadata for a protocol statistics command.
type ClientStatisticsExchange struct {
	Connection  ConnectionContext
	Variables   []ClientStatusVariable
	Summary     string
	Diagnostics DiagnosticSet
}

// ClientStatisticsSummaryRow describes aggregate protocol statistics metadata.
type ClientStatisticsSummaryRow struct {
	VariableCount        int
	SummaryLength        int
	EmptyValueCount      int
	NumericValueCount    int
	CommandVariableCount int
	ThreadVariableCount  int
	ConnectionCount      int
}

// PrepareClientStatistics formats adapter-supplied status values for a statistics command.
func (s PlanningService) PrepareClientStatistics(connection ConnectionContext, variables []ClientStatusVariable) ClientStatisticsExchange {
	_ = s
	exchange := ClientStatisticsExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	exchange.Variables = filterClientStatusVariables(variables, "")
	exchange.Summary = clientStatisticsSummary(exchange.Variables)
	return exchange
}

// Supported reports whether statistics metadata can be returned.
func (e ClientStatisticsExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts statistics diagnostics into protocol-facing errors.
func (e ClientStatisticsExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking statistics error, if any.
func (e ClientStatisticsExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func clientStatisticsSummary(variables []ClientStatusVariable) string {
	summary := ""
	for i, variable := range variables {
		if i > 0 {
			summary += "  "
		}
		summary += variable.Name + ": " + variable.Value
	}
	return summary
}

func summarizeClientStatistics(variables []ClientStatusVariable, summaryText string) ClientStatisticsSummaryRow {
	summary := ClientStatisticsSummaryRow{
		VariableCount: len(variables),
		SummaryLength: len(summaryText),
	}
	for _, variable := range variables {
		if variable.Value == "" {
			summary.EmptyValueCount++
		}
		if statusVariableNumericValue(variable.Value) {
			summary.NumericValueCount++
		}
		switch {
		case len(variable.Name) >= len("Com_") && variable.Name[:len("Com_")] == "Com_":
			summary.CommandVariableCount++
		case len(variable.Name) >= len("Threads_") && variable.Name[:len("Threads_")] == "Threads_":
			summary.ThreadVariableCount++
		case variable.Name == "Connections":
			summary.ConnectionCount++
		}
	}
	return summary
}
