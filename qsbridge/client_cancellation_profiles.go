package qsbridge

// ClientCancellationProfileExchange is adapter-facing cancellation capability metadata.
type ClientCancellationProfileExchange struct {
	Connection   ConnectionContext
	Rows         []CancellationProfile
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientCancellationProfiles returns cancellation capability metadata.
func (s PlanningService) ListClientCancellationProfiles(connection ConnectionContext) ClientCancellationProfileExchange {
	_ = s
	exchange := ClientCancellationProfileExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = DefaultCancellationProfiles()
	}
	exchange.Result = exchange.cancellationProfileResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether cancellation capability metadata can be returned.
func (e ClientCancellationProfileExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts cancellation profile diagnostics into protocol-facing errors.
func (e ClientCancellationProfileExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking cancellation profile error, if any.
func (e ClientCancellationProfileExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCancellationProfileExchange) cancellationProfileResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     cancellationProfileResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.cancellationProfileRows(),
		Final: true,
	})
}

func cancellationProfileResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Reason", Type: DataTypeString},
		{Name: "Requires_request_id", Type: DataTypeBool},
		{Name: "Requires_registry", Type: DataTypeBool},
		{Name: "Client_initiated", Type: DataTypeBool},
		{Name: "Timeout_driven", Type: DataTypeBool},
		{Name: "Shutdown_driven", Type: DataTypeBool},
		{Name: "Force_allowed", Type: DataTypeBool},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientCancellationProfileExchange) cancellationProfileRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Reason)),
			metadataBoolCell(row.RequiresRequestID),
			metadataBoolCell(row.RequiresRegistry),
			metadataBoolCell(row.ClientInitiated),
			metadataBoolCell(row.TimeoutDriven),
			metadataBoolCell(row.ShutdownDriven),
			metadataBoolCell(row.ForceAllowed),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}
