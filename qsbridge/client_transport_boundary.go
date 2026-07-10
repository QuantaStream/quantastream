package qsbridge

// ClientTransportBoundaryExchange is adapter-facing transport and placement metadata.
type ClientTransportBoundaryExchange struct {
	Connection   ConnectionContext
	Role         TransportRole
	Boundaries   []TransportBoundary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientTransportBoundaries returns protocol, transport, and placement boundaries.
func (s PlanningService) ListClientTransportBoundaries(connection ConnectionContext, role TransportRole) ClientTransportBoundaryExchange {
	_ = s
	exchange := ClientTransportBoundaryExchange{
		Connection:  cloneConnectionContext(connection),
		Role:        role,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Boundaries = TransportBoundariesForRole(role)
	}
	exchange.Result = exchange.transportBoundaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether transport boundary metadata can be returned.
func (e ClientTransportBoundaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts transport boundary diagnostics into protocol-facing errors.
func (e ClientTransportBoundaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking transport boundary error, if any.
func (e ClientTransportBoundaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientTransportBoundaryExchange) transportBoundaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     transportBoundaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.transportBoundaryRows(),
		Final: true,
	})
}

func transportBoundaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Role", Type: DataTypeString},
		{Name: "Kind", Type: DataTypeString},
		{Name: "Protocol", Type: DataTypeString, Nullable: true},
		{Name: "Owner", Type: DataTypeString},
		{Name: "Placement", Type: DataTypeString},
		{Name: "Networked", Type: DataTypeBool},
		{Name: "Port_independent", Type: DataTypeBool},
		{Name: "Metadata_only", Type: DataTypeBool},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientTransportBoundaryExchange) transportBoundaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Boundaries))
	for _, boundary := range e.Boundaries {
		rows = append(rows, ResultRow{
			metadataStringCell(string(boundary.Role)),
			metadataStringCell(string(boundary.Kind)),
			metadataStringCell(string(boundary.Protocol)),
			metadataStringCell(string(boundary.Owner)),
			metadataStringCell(string(boundary.Placement)),
			metadataBoolCell(boundary.Networked),
			metadataBoolCell(boundary.PortIndependent),
			metadataBoolCell(boundary.MetadataOnly),
			metadataStringCell(boundary.Detail),
		})
	}
	return rows
}
