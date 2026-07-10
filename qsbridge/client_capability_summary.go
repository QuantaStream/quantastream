package qsbridge

// ClientCapabilitySummaryExchange is adapter-facing connection capability summary metadata.
type ClientCapabilitySummaryExchange struct {
	Connection   ConnectionContext
	Row          ClientCapabilitySummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientConnectionCapabilities returns aggregate protocol and client capability metadata.
func (s PlanningService) SummarizeClientConnectionCapabilities(connection ConnectionContext) ClientCapabilitySummaryExchange {
	_ = s
	exchange := ClientCapabilitySummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeClientVisibleCapabilities(connectionCapabilities(connection))
	}
	exchange.Result = exchange.capabilitySummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether capability summary metadata can be returned.
func (e ClientCapabilitySummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts capability summary diagnostics into protocol-facing errors.
func (e ClientCapabilitySummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking capability summary error, if any.
func (e ClientCapabilitySummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCapabilitySummaryExchange) capabilitySummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     capabilitySummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{capabilitySummaryResultRow(e.Row)},
		Final: true,
	})
}

func capabilitySummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Capability_count", Type: DataTypeInt},
		{Name: "Protocol_capability_count", Type: DataTypeInt},
		{Name: "Client_capability_count", Type: DataTypeInt},
		{Name: "Advertised_count", Type: DataTypeInt},
		{Name: "Prepared_count", Type: DataTypeInt},
		{Name: "Batch_count", Type: DataTypeInt},
		{Name: "Streaming_count", Type: DataTypeInt},
		{Name: "Cancellation_count", Type: DataTypeInt},
		{Name: "Explain_count", Type: DataTypeInt},
		{Name: "Profile_count", Type: DataTypeInt},
		{Name: "Session_action_count", Type: DataTypeInt},
		{Name: "Tls_count", Type: DataTypeInt},
		{Name: "Compression_count", Type: DataTypeInt},
		{Name: "Session_tracking_count", Type: DataTypeInt},
	}
}

func capabilitySummaryResultRow(row ClientCapabilitySummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.CapabilityCount),
		metadataIntCell(row.ProtocolCapabilityCount),
		metadataIntCell(row.ClientCapabilityCount),
		metadataIntCell(row.AdvertisedCount),
		metadataIntCell(row.PreparedCount),
		metadataIntCell(row.BatchCount),
		metadataIntCell(row.StreamingCount),
		metadataIntCell(row.CancellationCount),
		metadataIntCell(row.ExplainCount),
		metadataIntCell(row.ProfileCount),
		metadataIntCell(row.SessionActionCount),
		metadataIntCell(row.TLSCount),
		metadataIntCell(row.CompressionCount),
		metadataIntCell(row.SessionTrackingCount),
	}
}
