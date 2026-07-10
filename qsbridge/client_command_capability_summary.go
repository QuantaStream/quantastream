package qsbridge

// ClientCommandCapabilitySummaryExchange is adapter-facing command capability summary metadata.
type ClientCommandCapabilitySummaryExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Row                 ClientCommandCapabilitySummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientCommandCapabilities returns aggregate non-SQL command capability metadata.
func (s PlanningService) SummarizeClientCommandCapabilities(connection ConnectionContext) ClientCommandCapabilitySummaryExchange {
	_ = s
	exchange := ClientCommandCapabilitySummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeCommandCapabilityRows(commandCapabilityRows(connection.Protocol))
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.commandCapabilitySummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether command capability summary metadata can be returned.
func (e ClientCommandCapabilitySummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientCommandCapabilitySummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientCommandCapabilitySummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientCommandCapabilitySummaryExchange) commandCapabilitySummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     commandCapabilitySummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{commandCapabilitySummaryResultRow(e.Row)},
		Final: true,
	})
}

func commandCapabilitySummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Command_count", Type: DataTypeInt},
		{Name: "Supported_count", Type: DataTypeInt},
		{Name: "Unsupported_count", Type: DataTypeInt},
		{Name: "Requires_payload_count", Type: DataTypeInt},
		{Name: "Session_action_count", Type: DataTypeInt},
		{Name: "Closes_connection_count", Type: DataTypeInt},
		{Name: "Statement_result_capability_count", Type: DataTypeInt},
		{Name: "Session_action_capability_count", Type: DataTypeInt},
		{Name: "All_supported", Type: DataTypeBool},
	}
}

func commandCapabilitySummaryResultRow(row ClientCommandCapabilitySummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.CommandCount),
		metadataIntCell(row.SupportedCount),
		metadataIntCell(row.UnsupportedCount),
		metadataIntCell(row.RequiresPayloadCount),
		metadataIntCell(row.SessionActionCount),
		metadataIntCell(row.ClosesConnectionCount),
		metadataIntCell(row.StatementResultCapabilityCount),
		metadataIntCell(row.SessionActionCapabilityCount),
		metadataBoolCell(row.AllSupported),
	}
}
