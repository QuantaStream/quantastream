package qsbridge

// ClientCancellationProfileSummaryExchange is adapter-facing cancellation profile summary metadata.
type ClientCancellationProfileSummaryExchange struct {
	Connection   ConnectionContext
	Row          CancellationProfileSummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientCancellationProfiles returns aggregate cancellation capability metadata.
func (s PlanningService) SummarizeClientCancellationProfiles(connection ConnectionContext) ClientCancellationProfileSummaryExchange {
	_ = s
	exchange := ClientCancellationProfileSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = DefaultCancellationProfileSummary()
	}
	exchange.Result = exchange.cancellationProfileSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether cancellation profile summary metadata can be returned.
func (e ClientCancellationProfileSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts cancellation profile summary diagnostics into protocol-facing errors.
func (e ClientCancellationProfileSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking cancellation profile summary error, if any.
func (e ClientCancellationProfileSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCancellationProfileSummaryExchange) cancellationProfileSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     cancellationProfileSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{cancellationProfileSummaryRow(e.Row)},
		Final: true,
	})
}

func cancellationProfileSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Profile_count", Type: DataTypeInt},
		{Name: "Requires_request_id_count", Type: DataTypeInt},
		{Name: "Requires_registry_count", Type: DataTypeInt},
		{Name: "Client_initiated_count", Type: DataTypeInt},
		{Name: "Timeout_driven_count", Type: DataTypeInt},
		{Name: "Shutdown_driven_count", Type: DataTypeInt},
		{Name: "Force_allowed_count", Type: DataTypeInt},
	}
}

func cancellationProfileSummaryRow(row CancellationProfileSummary) ResultRow {
	return ResultRow{
		metadataIntCell(row.ProfileCount),
		metadataIntCell(row.RequiresRequestIDCount),
		metadataIntCell(row.RequiresRegistryCount),
		metadataIntCell(row.ClientInitiatedCount),
		metadataIntCell(row.TimeoutDrivenCount),
		metadataIntCell(row.ShutdownDrivenCount),
		metadataIntCell(row.ForceAllowedCount),
	}
}
