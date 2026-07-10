package qsbridge

// ClientAdapterReadinessSummaryExchange is adapter-facing aggregate adapter readiness metadata.
type ClientAdapterReadinessSummaryExchange struct {
	Connection   ConnectionContext
	Rows         []AdapterReadinessSummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientAdapterReadiness returns aggregate readiness across adapter surfaces.
func (s PlanningService) SummarizeClientAdapterReadiness(connection ConnectionContext) ClientAdapterReadinessSummaryExchange {
	_ = s
	exchange := ClientAdapterReadinessSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []AdapterReadinessSummary{DefaultAdapterReadinessSummary()}
	}
	exchange.Result = exchange.adapterReadinessSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter readiness summary metadata can be returned.
func (e ClientAdapterReadinessSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter readiness summary diagnostics into protocol-facing errors.
func (e ClientAdapterReadinessSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking adapter readiness summary error, if any.
func (e ClientAdapterReadinessSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterReadinessSummaryExchange) adapterReadinessSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterReadinessSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterReadinessSummaryRows(),
		Final: true,
	})
}

func adapterReadinessSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surfaces", Type: DataTypeInt},
		{Name: "Metadata_ready", Type: DataTypeInt},
		{Name: "Runtime_ready", Type: DataTypeInt},
		{Name: "Client_facing", Type: DataTypeInt},
		{Name: "Control_plane", Type: DataTypeInt},
		{Name: "Embedded", Type: DataTypeInt},
		{Name: "Internal", Type: DataTypeInt},
		{Name: "Contracts", Type: DataTypeInt},
		{Name: "Deferred_contracts", Type: DataTypeInt},
		{Name: "Adapter_owned_contracts", Type: DataTypeInt},
		{Name: "Runtime_owned_contracts", Type: DataTypeInt},
		{Name: "Qsbridge_contracts", Type: DataTypeInt},
		{Name: "Phases", Type: DataTypeInt},
		{Name: "Deferred_phases", Type: DataTypeInt},
		{Name: "Runtime_blocking_phases", Type: DataTypeInt},
	}
}

func (e ClientAdapterReadinessSummaryExchange) adapterReadinessSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.SurfaceCount),
			metadataIntCell(row.MetadataReadyCount),
			metadataIntCell(row.RuntimeReadyCount),
			metadataIntCell(row.ClientFacingCount),
			metadataIntCell(row.ControlPlaneCount),
			metadataIntCell(row.EmbeddedCount),
			metadataIntCell(row.InternalCount),
			metadataIntCell(row.ContractCount),
			metadataIntCell(row.DeferredContracts),
			metadataIntCell(row.AdapterOwnedContracts),
			metadataIntCell(row.RuntimeOwnedContracts),
			metadataIntCell(row.QSBridgeContracts),
			metadataIntCell(row.PhaseCount),
			metadataIntCell(row.DeferredPhases),
			metadataIntCell(row.RuntimeBlockingPhases),
		})
	}
	return rows
}
