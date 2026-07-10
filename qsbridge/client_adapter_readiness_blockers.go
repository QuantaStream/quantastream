package qsbridge

// ClientAdapterReadinessBlockerExchange is adapter-facing blocker metadata.
type ClientAdapterReadinessBlockerExchange struct {
	Connection   ConnectionContext
	Surface      AdapterSurfaceKind
	Rows         []AdapterReadinessBlocker
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientAdapterReadinessBlockers returns blockers for adapter runtime readiness.
func (s PlanningService) ListClientAdapterReadinessBlockers(connection ConnectionContext, surface AdapterSurfaceKind) ClientAdapterReadinessBlockerExchange {
	_ = s
	exchange := ClientAdapterReadinessBlockerExchange{
		Connection:  cloneConnectionContext(connection),
		Surface:     surface,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = AdapterReadinessBlockersForSurface(surface)
	}
	exchange.Result = exchange.adapterReadinessBlockerResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter readiness blocker metadata can be returned.
func (e ClientAdapterReadinessBlockerExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter readiness blocker diagnostics into protocol-facing errors.
func (e ClientAdapterReadinessBlockerExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking adapter readiness blocker error, if any.
func (e ClientAdapterReadinessBlockerExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterReadinessBlockerExchange) adapterReadinessBlockerResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterReadinessBlockerResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterReadinessBlockerRows(),
		Final: true,
	})
}

func adapterReadinessBlockerResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surface", Type: DataTypeString},
		{Name: "Source", Type: DataTypeString},
		{Name: "Phase", Type: DataTypeString, Nullable: true},
		{Name: "Concern", Type: DataTypeString, Nullable: true},
		{Name: "Status", Type: DataTypeString},
		{Name: "Owner", Type: DataTypeString},
		{Name: "Blocks_runtime", Type: DataTypeBool},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientAdapterReadinessBlockerExchange) adapterReadinessBlockerRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Surface)),
			metadataStringCell(string(row.Source)),
			metadataStringCell(string(row.Phase)),
			metadataStringCell(string(row.Concern)),
			metadataStringCell(string(row.Status)),
			metadataStringCell(string(row.Owner)),
			metadataBoolCell(row.BlocksRuntime),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}
