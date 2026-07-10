package qsbridge

// ClientAdapterContractExchange is adapter-facing metadata for adapter implementation contracts.
type ClientAdapterContractExchange struct {
	Connection   ConnectionContext
	Surface      AdapterSurfaceKind
	Contracts    []AdapterContract
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientAdapterContracts returns implementation contracts for adapter surfaces.
func (s PlanningService) ListClientAdapterContracts(connection ConnectionContext, surface AdapterSurfaceKind) ClientAdapterContractExchange {
	_ = s
	exchange := ClientAdapterContractExchange{
		Connection:  cloneConnectionContext(connection),
		Surface:     surface,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Contracts = AdapterContractsForSurface(surface)
	}
	exchange.Result = exchange.adapterContractResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter contract metadata can be returned.
func (e ClientAdapterContractExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter contract diagnostics into protocol-facing errors.
func (e ClientAdapterContractExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking adapter contract error, if any.
func (e ClientAdapterContractExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterContractExchange) adapterContractResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterContractResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterContractRows(),
		Final: true,
	})
}

func adapterContractResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surface", Type: DataTypeString},
		{Name: "Concern", Type: DataTypeString},
		{Name: "Layer", Type: DataTypeString},
		{Name: "Status", Type: DataTypeString},
		{Name: "Owner", Type: DataTypeString},
		{Name: "Required", Type: DataTypeBool},
		{Name: "Adapter_owned", Type: DataTypeBool},
		{Name: "Runtime_owned", Type: DataTypeBool},
		{Name: "Metadata_name", Type: DataTypeString, Nullable: true},
		{Name: "Implementation", Type: DataTypeString, Nullable: true},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientAdapterContractExchange) adapterContractRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Contracts))
	for _, contract := range e.Contracts {
		rows = append(rows, ResultRow{
			metadataStringCell(string(contract.Surface)),
			metadataStringCell(string(contract.Concern)),
			metadataStringCell(string(contract.Layer)),
			metadataStringCell(string(contract.Status)),
			metadataStringCell(string(contract.Owner)),
			metadataBoolCell(contract.Required),
			metadataBoolCell(contract.AdapterOwned),
			metadataBoolCell(contract.RuntimeOwned),
			metadataStringCell(contract.MetadataName),
			metadataStringCell(contract.Implementation),
			metadataStringCell(contract.Detail),
		})
	}
	return rows
}
