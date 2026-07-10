package qsbridge

// ClientAdapterReadinessGateExchange is adapter-facing release gate metadata.
type ClientAdapterReadinessGateExchange struct {
	Connection   ConnectionContext
	Surface      AdapterSurfaceKind
	Rows         []AdapterReadinessGate
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientAdapterReadinessGates returns adapter release-readiness gates.
func (s PlanningService) ListClientAdapterReadinessGates(connection ConnectionContext, surface AdapterSurfaceKind) ClientAdapterReadinessGateExchange {
	_ = s
	exchange := ClientAdapterReadinessGateExchange{
		Connection:  cloneConnectionContext(connection),
		Surface:     surface,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = AdapterReadinessGatesForSurface(surface)
	}
	exchange.Result = exchange.adapterReadinessGateResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter readiness gate metadata can be returned.
func (e ClientAdapterReadinessGateExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter readiness gate diagnostics into protocol-facing errors.
func (e ClientAdapterReadinessGateExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking readiness gate error, if any.
func (e ClientAdapterReadinessGateExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterReadinessGateExchange) adapterReadinessGateResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterReadinessGateResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterReadinessGateRows(),
		Final: true,
	})
}

func adapterReadinessGateResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surface", Type: DataTypeString},
		{Name: "Gate", Type: DataTypeString},
		{Name: "Gate_order", Type: DataTypeInt},
		{Name: "Status", Type: DataTypeString},
		{Name: "Owner", Type: DataTypeString, Nullable: true},
		{Name: "Ready", Type: DataTypeBool},
		{Name: "Blocks_runtime", Type: DataTypeBool},
		{Name: "Blockers", Type: DataTypeInt},
		{Name: "Next", Type: DataTypeBool},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientAdapterReadinessGateExchange) adapterReadinessGateRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Surface)),
			metadataStringCell(string(row.Gate)),
			metadataIntCell(row.Order),
			metadataStringCell(string(row.Status)),
			metadataStringCell(string(row.Owner)),
			metadataBoolCell(row.Ready),
			metadataBoolCell(row.BlocksRuntime),
			metadataIntCell(row.BlockerCount),
			metadataBoolCell(row.Next),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}
