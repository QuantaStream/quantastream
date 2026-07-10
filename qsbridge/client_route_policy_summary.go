package qsbridge

// ClientRoutePolicySummaryExchange is adapter-facing aggregate route policy metadata.
type ClientRoutePolicySummaryExchange struct {
	Connection   ConnectionContext
	Rows         []RoutePolicySummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientRoutePolicies returns aggregate route policy metadata.
func (s PlanningService) SummarizeClientRoutePolicies(connection ConnectionContext) ClientRoutePolicySummaryExchange {
	_ = s
	exchange := ClientRoutePolicySummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []RoutePolicySummary{DefaultRoutePolicySummary()}
	}
	exchange.Result = exchange.routePolicySummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether route policy summary metadata can be returned.
func (e ClientRoutePolicySummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts route policy summary diagnostics into protocol-facing errors.
func (e ClientRoutePolicySummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking route policy summary error, if any.
func (e ClientRoutePolicySummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientRoutePolicySummaryExchange) routePolicySummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     routePolicySummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.routePolicySummaryRows(),
		Final: true,
	})
}

func routePolicySummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Policies", Type: DataTypeInt},
		{Name: "Default_policies", Type: DataTypeInt},
		{Name: "Native_allowed", Type: DataTypeInt},
		{Name: "Fallback_allowed", Type: DataTypeInt},
		{Name: "Rejects_unsupported", Type: DataTypeInt},
		{Name: "Native_routing_enabled", Type: DataTypeInt},
		{Name: "Native_routing_disabled", Type: DataTypeInt},
	}
}

func (e ClientRoutePolicySummaryExchange) routePolicySummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.PolicyCount),
			metadataIntCell(row.DefaultCount),
			metadataIntCell(row.NativeAllowedCount),
			metadataIntCell(row.FallbackAllowedCount),
			metadataIntCell(row.RejectsUnsupportedCount),
			metadataIntCell(row.NativeRoutingEnabledCount),
			metadataIntCell(row.NativeRoutingDisabledCount),
		})
	}
	return rows
}
