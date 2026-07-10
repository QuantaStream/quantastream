package qsbridge

// ClientRoutePolicyExchange is adapter-facing route policy metadata.
type ClientRoutePolicyExchange struct {
	Connection   ConnectionContext
	Rows         []RoutePolicyProfile
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientRoutePolicies returns named route policy profiles.
func (s PlanningService) ListClientRoutePolicies(connection ConnectionContext) ClientRoutePolicyExchange {
	_ = s
	exchange := ClientRoutePolicyExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = DefaultRoutePolicyProfiles()
	}
	exchange.Result = exchange.routePolicyResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether route policy metadata can be returned.
func (e ClientRoutePolicyExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts route policy diagnostics into protocol-facing errors.
func (e ClientRoutePolicyExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking route policy error, if any.
func (e ClientRoutePolicyExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientRoutePolicyExchange) routePolicyResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     routePolicyResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.routePolicyRows(),
		Final: true,
	})
}

func routePolicyResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Name", Type: DataTypeString},
		{Name: "Mode", Type: DataTypeString},
		{Name: "Native_route_gate", Type: DataTypeString, Nullable: true},
		{Name: "Default", Type: DataTypeBool},
		{Name: "Native_allowed", Type: DataTypeBool},
		{Name: "Fallback_allowed", Type: DataTypeBool},
		{Name: "Rejects_unsupported", Type: DataTypeBool},
		{Name: "Native_routing_enabled", Type: DataTypeBool},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientRoutePolicyExchange) routePolicyRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(row.Name),
			metadataStringCell(string(row.Policy.Mode)),
			metadataStringCell(string(row.Policy.NativeRouting)),
			metadataBoolCell(row.Default),
			metadataBoolCell(row.NativeAllowed),
			metadataBoolCell(row.FallbackAllowed),
			metadataBoolCell(row.RejectsUnsupported),
			metadataBoolCell(row.NativeRoutingEnabled),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}
