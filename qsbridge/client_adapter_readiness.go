package qsbridge

// ClientAdapterReadinessExchange is adapter-facing aggregate readiness metadata.
type ClientAdapterReadinessExchange struct {
	Connection   ConnectionContext
	Surface      AdapterSurfaceKind
	Rows         []AdapterReadinessReport
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientAdapterReadiness returns readiness reports for adapter surfaces.
func (s PlanningService) ListClientAdapterReadiness(connection ConnectionContext, surface AdapterSurfaceKind) ClientAdapterReadinessExchange {
	_ = s
	exchange := ClientAdapterReadinessExchange{
		Connection:  cloneConnectionContext(connection),
		Surface:     surface,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = AdapterReadinessReportsForSurface(surface)
	}
	exchange.Result = exchange.adapterReadinessResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter readiness metadata can be returned.
func (e ClientAdapterReadinessExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter readiness diagnostics into protocol-facing errors.
func (e ClientAdapterReadinessExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking adapter readiness error, if any.
func (e ClientAdapterReadinessExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterReadinessExchange) adapterReadinessResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterReadinessResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterReadinessRows(),
		Final: true,
	})
}

func adapterReadinessResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surface", Type: DataTypeString},
		{Name: "Audience", Type: DataTypeString},
		{Name: "Protocol", Type: DataTypeString, Nullable: true},
		{Name: "Transport", Type: DataTypeString},
		{Name: "Placement", Type: DataTypeString},
		{Name: "Client_facing", Type: DataTypeBool},
		{Name: "Control_plane", Type: DataTypeBool},
		{Name: "Embedded", Type: DataTypeBool},
		{Name: "Internal", Type: DataTypeBool},
		{Name: "Metadata_ready", Type: DataTypeBool},
		{Name: "Runtime_ready", Type: DataTypeBool},
		{Name: "Next_phase", Type: DataTypeString, Nullable: true},
		{Name: "Contracts", Type: DataTypeInt},
		{Name: "Deferred_contracts", Type: DataTypeInt},
		{Name: "Adapter_owned_contracts", Type: DataTypeInt},
		{Name: "Runtime_owned_contracts", Type: DataTypeInt},
		{Name: "Qsbridge_contracts", Type: DataTypeInt},
		{Name: "Phases", Type: DataTypeInt},
		{Name: "Deferred_phases", Type: DataTypeInt},
		{Name: "Runtime_blocking_phases", Type: DataTypeInt},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientAdapterReadinessExchange) adapterReadinessRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Surface)),
			metadataStringCell(string(row.Audience)),
			metadataStringCell(string(row.Protocol)),
			metadataStringCell(string(row.Transport)),
			metadataStringCell(string(row.Placement)),
			metadataBoolCell(row.ClientFacing),
			metadataBoolCell(row.ControlPlane),
			metadataBoolCell(row.Embedded),
			metadataBoolCell(row.Internal),
			metadataBoolCell(row.MetadataReady),
			metadataBoolCell(row.RuntimeReady),
			metadataStringCell(string(row.NextPhase)),
			metadataIntCell(row.ContractCount),
			metadataIntCell(row.DeferredContracts),
			metadataIntCell(row.AdapterOwnedContracts),
			metadataIntCell(row.RuntimeOwnedContracts),
			metadataIntCell(row.QSBridgeContracts),
			metadataIntCell(row.PhaseCount),
			metadataIntCell(row.DeferredPhases),
			metadataIntCell(row.RuntimeBlockingPhases),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}
