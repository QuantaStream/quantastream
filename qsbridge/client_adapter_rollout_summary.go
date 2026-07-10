package qsbridge

// ClientAdapterRolloutSummaryExchange is adapter-facing aggregate rollout metadata.
type ClientAdapterRolloutSummaryExchange struct {
	Connection   ConnectionContext
	Surface      AdapterSurfaceKind
	Rows         []AdapterRolloutSummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientAdapterRollout returns aggregate rollout readiness by adapter surface.
func (s PlanningService) SummarizeClientAdapterRollout(connection ConnectionContext, surface AdapterSurfaceKind) ClientAdapterRolloutSummaryExchange {
	_ = s
	exchange := ClientAdapterRolloutSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Surface:     surface,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = AdapterRolloutSummariesForSurface(surface)
	}
	exchange.Result = exchange.adapterRolloutSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter rollout summary metadata can be returned.
func (e ClientAdapterRolloutSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter rollout summary diagnostics into protocol-facing errors.
func (e ClientAdapterRolloutSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking adapter rollout summary error, if any.
func (e ClientAdapterRolloutSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterRolloutSummaryExchange) adapterRolloutSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterRolloutSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterRolloutSummaryRows(),
		Final: true,
	})
}

func adapterRolloutSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surface", Type: DataTypeString},
		{Name: "Phases", Type: DataTypeInt},
		{Name: "Metadata_only", Type: DataTypeInt},
		{Name: "Boundary_only", Type: DataTypeInt},
		{Name: "Deferred", Type: DataTypeInt},
		{Name: "Blocks_runtime", Type: DataTypeInt},
		{Name: "Qsbridge_owned", Type: DataTypeInt},
		{Name: "Adapter_owned", Type: DataTypeInt},
		{Name: "Runtime_owned", Type: DataTypeInt},
	}
}

func (e ClientAdapterRolloutSummaryExchange) adapterRolloutSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Surface)),
			metadataIntCell(row.PhaseCount),
			metadataIntCell(row.MetadataOnlyCount),
			metadataIntCell(row.BoundaryOnlyCount),
			metadataIntCell(row.DeferredCount),
			metadataIntCell(row.BlocksRuntime),
			metadataIntCell(row.QSBridgeOwnedCount),
			metadataIntCell(row.AdapterOwnedCount),
			metadataIntCell(row.RuntimeOwnedCount),
		})
	}
	return rows
}
