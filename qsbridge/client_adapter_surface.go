package qsbridge

// ClientAdapterSurfaceExchange is adapter-facing metadata for named adapter surfaces.
type ClientAdapterSurfaceExchange struct {
	Connection   ConnectionContext
	Audience     AdapterSurfaceAudience
	Surfaces     []AdapterSurface
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientAdapterSurfaces returns named adapter surfaces for diagnostics and refactor tooling.
func (s PlanningService) ListClientAdapterSurfaces(connection ConnectionContext, audience AdapterSurfaceAudience) ClientAdapterSurfaceExchange {
	_ = s
	exchange := ClientAdapterSurfaceExchange{
		Connection:  cloneConnectionContext(connection),
		Audience:    audience,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Surfaces = AdapterSurfacesForAudience(audience)
	}
	exchange.Result = exchange.adapterSurfaceResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter surface metadata can be returned.
func (e ClientAdapterSurfaceExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter surface diagnostics into protocol-facing errors.
func (e ClientAdapterSurfaceExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking adapter surface error, if any.
func (e ClientAdapterSurfaceExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterSurfaceExchange) adapterSurfaceResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterSurfaceResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.adapterSurfaceRows(),
		Final: true,
	})
}

func adapterSurfaceResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Kind", Type: DataTypeString},
		{Name: "Audience", Type: DataTypeString},
		{Name: "Protocol", Type: DataTypeString, Nullable: true},
		{Name: "Transport", Type: DataTypeString},
		{Name: "Placement", Type: DataTypeString},
		{Name: "Owner", Type: DataTypeString},
		{Name: "Client_facing", Type: DataTypeBool},
		{Name: "Control_plane", Type: DataTypeBool},
		{Name: "Embedded", Type: DataTypeBool},
		{Name: "Internal", Type: DataTypeBool},
		{Name: "Uses_qsbridge_metadata", Type: DataTypeBool},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientAdapterSurfaceExchange) adapterSurfaceRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Surfaces))
	for _, surface := range e.Surfaces {
		rows = append(rows, ResultRow{
			metadataStringCell(string(surface.Kind)),
			metadataStringCell(string(surface.Audience)),
			metadataStringCell(string(surface.Protocol)),
			metadataStringCell(string(surface.Transport)),
			metadataStringCell(string(surface.Placement)),
			metadataStringCell(string(surface.Owner)),
			metadataBoolCell(surface.ClientFacing),
			metadataBoolCell(surface.ControlPlane),
			metadataBoolCell(surface.Embedded),
			metadataBoolCell(surface.Internal),
			metadataBoolCell(surface.UsesQSBridgeMetadata),
			metadataStringCell(surface.Detail),
		})
	}
	return rows
}
