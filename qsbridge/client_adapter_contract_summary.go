package qsbridge

// ClientAdapterContractSummaryExchange is adapter-facing aggregate adapter contract metadata.
type ClientAdapterContractSummaryExchange struct {
	Connection   ConnectionContext
	Surface      AdapterSurfaceKind
	Rows         []AdapterContractSummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientAdapterContracts returns aggregate contract readiness by adapter surface.
func (s PlanningService) SummarizeClientAdapterContracts(connection ConnectionContext, surface AdapterSurfaceKind) ClientAdapterContractSummaryExchange {
	_ = s
	exchange := ClientAdapterContractSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Surface:     surface,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = AdapterContractSummariesForSurface(surface)
	}
	exchange.Result = exchange.adapterContractSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter contract summary metadata can be returned.
func (e ClientAdapterContractSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter contract summary diagnostics into protocol-facing errors.
func (e ClientAdapterContractSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking adapter contract summary error, if any.
func (e ClientAdapterContractSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterContractSummaryExchange) adapterContractSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterContractSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterContractSummaryRows(),
		Final: true,
	})
}

func adapterContractSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surface", Type: DataTypeString},
		{Name: "Contracts", Type: DataTypeInt},
		{Name: "Required", Type: DataTypeInt},
		{Name: "Metadata_only", Type: DataTypeInt},
		{Name: "Boundary_only", Type: DataTypeInt},
		{Name: "Deferred", Type: DataTypeInt},
		{Name: "Adapter_owned", Type: DataTypeInt},
		{Name: "Runtime_owned", Type: DataTypeInt},
		{Name: "Qsbridge_owned", Type: DataTypeInt},
	}
}

func (e ClientAdapterContractSummaryExchange) adapterContractSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Surface)),
			metadataIntCell(row.ContractCount),
			metadataIntCell(row.RequiredCount),
			metadataIntCell(row.MetadataOnlyCount),
			metadataIntCell(row.BoundaryOnlyCount),
			metadataIntCell(row.DeferredCount),
			metadataIntCell(row.AdapterOwnedCount),
			metadataIntCell(row.RuntimeOwnedCount),
			metadataIntCell(row.QSBridgeOwnedCount),
		})
	}
	return rows
}
