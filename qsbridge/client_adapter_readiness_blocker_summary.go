package qsbridge

// ClientAdapterReadinessBlockerSummaryExchange is aggregate blocker metadata.
type ClientAdapterReadinessBlockerSummaryExchange struct {
	Connection   ConnectionContext
	Surface      AdapterSurfaceKind
	Rows         []AdapterReadinessBlockerSummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientAdapterReadinessBlockers returns aggregate blocker metadata.
func (s PlanningService) SummarizeClientAdapterReadinessBlockers(connection ConnectionContext, surface AdapterSurfaceKind) ClientAdapterReadinessBlockerSummaryExchange {
	_ = s
	exchange := ClientAdapterReadinessBlockerSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Surface:     surface,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = AdapterReadinessBlockerSummariesForSurface(surface)
	}
	exchange.Result = exchange.adapterReadinessBlockerSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter readiness blocker summary metadata can be returned.
func (e ClientAdapterReadinessBlockerSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter readiness blocker summary diagnostics into errors.
func (e ClientAdapterReadinessBlockerSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking blocker summary error, if any.
func (e ClientAdapterReadinessBlockerSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterReadinessBlockerSummaryExchange) adapterReadinessBlockerSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterReadinessBlockerSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterReadinessBlockerSummaryRows(),
		Final: true,
	})
}

func adapterReadinessBlockerSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surface", Type: DataTypeString},
		{Name: "Blockers", Type: DataTypeInt},
		{Name: "Contract_blockers", Type: DataTypeInt},
		{Name: "Rollout_blockers", Type: DataTypeInt},
		{Name: "Deferred", Type: DataTypeInt},
		{Name: "Boundary_only", Type: DataTypeInt},
		{Name: "Runtime_blocking", Type: DataTypeInt},
		{Name: "Adapter_owned", Type: DataTypeInt},
		{Name: "Runtime_owned", Type: DataTypeInt},
		{Name: "Qsbridge_owned", Type: DataTypeInt},
	}
}

func (e ClientAdapterReadinessBlockerSummaryExchange) adapterReadinessBlockerSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Surface)),
			metadataIntCell(row.BlockerCount),
			metadataIntCell(row.ContractBlockers),
			metadataIntCell(row.RolloutBlockers),
			metadataIntCell(row.DeferredCount),
			metadataIntCell(row.BoundaryOnlyCount),
			metadataIntCell(row.RuntimeBlockingCount),
			metadataIntCell(row.AdapterOwnedCount),
			metadataIntCell(row.RuntimeOwnedCount),
			metadataIntCell(row.QSBridgeOwnedCount),
		})
	}
	return rows
}
