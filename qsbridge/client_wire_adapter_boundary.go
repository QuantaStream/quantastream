package qsbridge

// ClientWireAdapterBoundaryExchange is adapter-facing wire/server boundary metadata.
type ClientWireAdapterBoundaryExchange struct {
	Connection   ConnectionContext
	Protocol     ProtocolKind
	Boundaries   []WireAdapterBoundary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientWireAdapterBoundaries returns protocol/server responsibility boundaries.
func (s PlanningService) ListClientWireAdapterBoundaries(connection ConnectionContext, protocol ProtocolKind) ClientWireAdapterBoundaryExchange {
	_ = s
	if protocol == ProtocolUnknown {
		protocol = connection.Protocol.Kind
	}
	exchange := ClientWireAdapterBoundaryExchange{
		Connection:  cloneConnectionContext(connection),
		Protocol:    protocol,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Boundaries = WireAdapterBoundariesForProtocol(protocol)
	}
	exchange.Result = exchange.wireAdapterBoundaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether wire-adapter boundary metadata can be returned.
func (e ClientWireAdapterBoundaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts wire-adapter boundary diagnostics into protocol-facing errors.
func (e ClientWireAdapterBoundaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking wire-adapter boundary error, if any.
func (e ClientWireAdapterBoundaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientWireAdapterBoundaryExchange) wireAdapterBoundaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     wireAdapterBoundaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.wireAdapterBoundaryRows(),
		Final: true,
	})
}

func wireAdapterBoundaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Concern", Type: DataTypeString},
		{Name: "Protocol", Type: DataTypeString, Nullable: true},
		{Name: "Owner", Type: DataTypeString},
		{Name: "Permanent", Type: DataTypeBool},
		{Name: "Metadata_only", Type: DataTypeBool},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientWireAdapterBoundaryExchange) wireAdapterBoundaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Boundaries))
	for _, boundary := range e.Boundaries {
		rows = append(rows, ResultRow{
			metadataStringCell(string(boundary.Concern)),
			metadataStringCell(string(boundary.Protocol)),
			metadataStringCell(string(boundary.Owner)),
			metadataBoolCell(boundary.Permanent),
			metadataBoolCell(boundary.MetadataOnly),
			metadataStringCell(boundary.Detail),
		})
	}
	return rows
}
