package qsbridge

// ClientDispatchTargetExchange is adapter-facing dispatch target metadata.
type ClientDispatchTargetExchange struct {
	Connection   ConnectionContext
	Rows         []DispatchTargetProfile
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientDispatchTargets returns dispatch target boundary metadata.
func (s PlanningService) ListClientDispatchTargets(connection ConnectionContext) ClientDispatchTargetExchange {
	_ = s
	exchange := ClientDispatchTargetExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = DefaultDispatchTargetProfiles()
	}
	exchange.Result = exchange.dispatchTargetResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether dispatch target metadata can be returned.
func (e ClientDispatchTargetExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts dispatch target diagnostics into protocol-facing errors.
func (e ClientDispatchTargetExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking dispatch target error, if any.
func (e ClientDispatchTargetExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientDispatchTargetExchange) dispatchTargetResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     dispatchTargetResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.dispatchTargetRows(),
		Final: true,
	})
}

func dispatchTargetResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Target", Type: DataTypeString},
		{Name: "Handoff", Type: DataTypeString},
		{Name: "Runtime_owned", Type: DataTypeBool},
		{Name: "Requires_executor", Type: DataTypeBool},
		{Name: "Configurable", Type: DataTypeBool},
		{Name: "Terminal", Type: DataTypeBool},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientDispatchTargetExchange) dispatchTargetRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Target)),
			metadataStringCell(string(row.Handoff)),
			metadataBoolCell(row.RuntimeOwned),
			metadataBoolCell(row.RequiresExecutor),
			metadataBoolCell(row.Configurable),
			metadataBoolCell(row.Terminal),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}
