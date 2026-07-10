package qsbridge

// ClientAdapterReadinessNextActionExchange is adapter-facing next-action metadata.
type ClientAdapterReadinessNextActionExchange struct {
	Connection   ConnectionContext
	Surface      AdapterSurfaceKind
	Rows         []AdapterReadinessNextAction
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientAdapterReadinessNextActions returns the next readiness gate per surface.
func (s PlanningService) ListClientAdapterReadinessNextActions(connection ConnectionContext, surface AdapterSurfaceKind) ClientAdapterReadinessNextActionExchange {
	_ = s
	exchange := ClientAdapterReadinessNextActionExchange{
		Connection:  cloneConnectionContext(connection),
		Surface:     surface,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = AdapterReadinessNextActionsForSurface(surface)
	}
	exchange.Result = exchange.adapterReadinessNextActionResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter readiness next-action metadata can be returned.
func (e ClientAdapterReadinessNextActionExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter readiness next-action diagnostics into errors.
func (e ClientAdapterReadinessNextActionExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking next-action error, if any.
func (e ClientAdapterReadinessNextActionExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterReadinessNextActionExchange) adapterReadinessNextActionResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterReadinessNextActionResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterReadinessNextActionRows(),
		Final: true,
	})
}

func adapterReadinessNextActionResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surface", Type: DataTypeString},
		{Name: "Gate", Type: DataTypeString},
		{Name: "Gate_order", Type: DataTypeInt},
		{Name: "Status", Type: DataTypeString},
		{Name: "Owner", Type: DataTypeString, Nullable: true},
		{Name: "Blocks_runtime", Type: DataTypeBool},
		{Name: "Blockers", Type: DataTypeInt},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientAdapterReadinessNextActionExchange) adapterReadinessNextActionRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Surface)),
			metadataStringCell(string(row.Gate)),
			metadataIntCell(row.Order),
			metadataStringCell(string(row.Status)),
			metadataStringCell(string(row.Owner)),
			metadataBoolCell(row.BlocksRuntime),
			metadataIntCell(row.BlockerCount),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}
