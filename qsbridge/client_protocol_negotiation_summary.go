package qsbridge

// ClientProtocolNegotiationSummaryExchange is adapter-facing metadata for aggregate protocol negotiation metadata.
type ClientProtocolNegotiationSummaryExchange struct {
	Connection          ConnectionContext
	Negotiation         ProtocolNegotiation
	Row                 ClientProtocolNegotiationSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	Diagnostics         DiagnosticSet
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientProtocolExecutionNegotiation validates and summarizes one requested protocol execution shape.
func (s PlanningService) SummarizeClientProtocolExecutionNegotiation(connection ConnectionContext, mode ProtocolExecutionMode, options ExecutionOptions) ClientProtocolNegotiationSummaryExchange {
	_ = s
	negotiation := connection.Protocol.NegotiateExecution(mode, options)
	exchange := ClientProtocolNegotiationSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Negotiation:         cloneProtocolNegotiation(negotiation),
		Diagnostics:         cloneDiagnosticSet(negotiation.Diagnostics),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeClientProtocolNegotiations([]ClientProtocolNegotiationRow{protocolNegotiationRow(negotiation)})
	}
	exchange.Result = exchange.protocolNegotiationSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether protocol negotiation summary metadata can be returned.
func (e ClientProtocolNegotiationSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientProtocolNegotiationSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientProtocolNegotiationSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientProtocolNegotiationSummaryExchange) protocolNegotiationSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     protocolNegotiationSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{protocolNegotiationSummaryResultRow(e.Row)},
		Final: true,
	})
}

func protocolNegotiationSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Negotiation_count", Type: DataTypeInt},
		{Name: "Supported_count", Type: DataTypeInt},
		{Name: "Unsupported_count", Type: DataTypeInt},
		{Name: "Capability_count", Type: DataTypeInt},
		{Name: "Diagnostic_count", Type: DataTypeInt},
		{Name: "Streaming_requested_count", Type: DataTypeInt},
		{Name: "Cursor_requested_count", Type: DataTypeInt},
		{Name: "Cancelable_count", Type: DataTypeInt},
		{Name: "Explain_count", Type: DataTypeInt},
		{Name: "Profile_count", Type: DataTypeInt},
	}
}

func protocolNegotiationSummaryResultRow(row ClientProtocolNegotiationSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.NegotiationCount),
		metadataIntCell(row.SupportedCount),
		metadataIntCell(row.UnsupportedCount),
		metadataIntCell(row.CapabilityCount),
		metadataIntCell(row.DiagnosticCount),
		metadataIntCell(row.StreamingRequestedCount),
		metadataIntCell(row.CursorRequestedCount),
		metadataIntCell(row.CancelableCount),
		metadataIntCell(row.ExplainCount),
		metadataIntCell(row.ProfileCount),
	}
}
