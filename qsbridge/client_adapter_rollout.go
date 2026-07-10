package qsbridge

// ClientAdapterRolloutExchange is adapter-facing metadata for adapter rollout phases.
type ClientAdapterRolloutExchange struct {
	Connection   ConnectionContext
	Surface      AdapterSurfaceKind
	Steps        []AdapterRolloutStep
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientAdapterRollout returns ordered rollout phases for adapter surfaces.
func (s PlanningService) ListClientAdapterRollout(connection ConnectionContext, surface AdapterSurfaceKind) ClientAdapterRolloutExchange {
	_ = s
	exchange := ClientAdapterRolloutExchange{
		Connection:  cloneConnectionContext(connection),
		Surface:     surface,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Steps = AdapterRolloutStepsForSurface(surface)
	}
	exchange.Result = exchange.adapterRolloutResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter rollout metadata can be returned.
func (e ClientAdapterRolloutExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter rollout diagnostics into protocol-facing errors.
func (e ClientAdapterRolloutExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking adapter rollout error, if any.
func (e ClientAdapterRolloutExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterRolloutExchange) adapterRolloutResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterRolloutResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterRolloutRows(),
		Final: true,
	})
}

func adapterRolloutResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surface", Type: DataTypeString},
		{Name: "Phase", Type: DataTypeString},
		{Name: "Phase_order", Type: DataTypeInt},
		{Name: "Status", Type: DataTypeString},
		{Name: "Owner", Type: DataTypeString},
		{Name: "Requires", Type: DataTypeString, Nullable: true},
		{Name: "Blocks_runtime", Type: DataTypeBool},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientAdapterRolloutExchange) adapterRolloutRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Steps))
	for _, step := range e.Steps {
		rows = append(rows, ResultRow{
			metadataStringCell(string(step.Surface)),
			metadataStringCell(string(step.Phase)),
			metadataIntCell(step.Order),
			metadataStringCell(string(step.Status)),
			metadataStringCell(string(step.Owner)),
			metadataStringCell(joinAdapterContractConcerns(step.Requires)),
			metadataBoolCell(step.BlocksRuntime),
			metadataStringCell(step.Detail),
		})
	}
	return rows
}

func joinAdapterContractConcerns(concerns []AdapterContractConcern) string {
	if len(concerns) == 0 {
		return ""
	}
	values := make([]string, 0, len(concerns))
	for _, concern := range concerns {
		values = append(values, string(concern))
	}
	return joinStringValues(values)
}
