package qsbridge

// ClientAdapterReadinessGateSummaryExchange is aggregate release gate metadata.
type ClientAdapterReadinessGateSummaryExchange struct {
	Connection   ConnectionContext
	Surface      AdapterSurfaceKind
	Rows         []AdapterReadinessGateSummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientAdapterReadinessGates returns aggregate release gate metadata.
func (s PlanningService) SummarizeClientAdapterReadinessGates(connection ConnectionContext, surface AdapterSurfaceKind) ClientAdapterReadinessGateSummaryExchange {
	_ = s
	exchange := ClientAdapterReadinessGateSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Surface:     surface,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = AdapterReadinessGateSummariesForSurface(surface)
	}
	exchange.Result = exchange.adapterReadinessGateSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter readiness gate summary metadata can be returned.
func (e ClientAdapterReadinessGateSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter readiness gate summary diagnostics into errors.
func (e ClientAdapterReadinessGateSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking gate summary error, if any.
func (e ClientAdapterReadinessGateSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterReadinessGateSummaryExchange) adapterReadinessGateSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterReadinessGateSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterReadinessGateSummaryRows(),
		Final: true,
	})
}

func adapterReadinessGateSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surface", Type: DataTypeString},
		{Name: "Gates", Type: DataTypeInt},
		{Name: "Ready", Type: DataTypeInt},
		{Name: "Runtime_blocking", Type: DataTypeInt},
		{Name: "Blockers", Type: DataTypeInt},
		{Name: "Next_gate", Type: DataTypeString, Nullable: true},
		{Name: "Next_gate_order", Type: DataTypeInt},
		{Name: "Contracts_ready", Type: DataTypeBool},
		{Name: "Metadata_ready", Type: DataTypeBool},
		{Name: "Runtime_ready", Type: DataTypeBool},
	}
}

func (e ClientAdapterReadinessGateSummaryExchange) adapterReadinessGateSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Surface)),
			metadataIntCell(row.GateCount),
			metadataIntCell(row.ReadyCount),
			metadataIntCell(row.RuntimeBlockCount),
			metadataIntCell(row.BlockerCount),
			metadataStringCell(string(row.NextGate)),
			metadataIntCell(row.NextGateOrder),
			metadataBoolCell(row.ContractsReady),
			metadataBoolCell(row.MetadataReady),
			metadataBoolCell(row.RuntimeReady),
		})
	}
	return rows
}
