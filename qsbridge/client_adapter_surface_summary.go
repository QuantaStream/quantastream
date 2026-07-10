package qsbridge

// ClientAdapterSurfaceSummaryExchange is adapter-facing metadata for aggregate adapter surface metadata.
type ClientAdapterSurfaceSummaryExchange struct {
	Connection   ConnectionContext
	Audience     AdapterSurfaceAudience
	Row          AdapterSurfaceSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientAdapterSurfaces returns aggregate adapter-surface metadata for diagnostics and refactor tooling.
func (s PlanningService) SummarizeClientAdapterSurfaces(connection ConnectionContext, audience AdapterSurfaceAudience) ClientAdapterSurfaceSummaryExchange {
	_ = s
	exchange := ClientAdapterSurfaceSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Audience:    audience,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeAdapterSurfaces(AdapterSurfacesForAudience(audience))
	}
	exchange.Result = exchange.adapterSurfaceSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether adapter surface summary metadata can be returned.
func (e ClientAdapterSurfaceSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts adapter surface summary diagnostics into protocol-facing errors.
func (e ClientAdapterSurfaceSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking adapter surface summary error, if any.
func (e ClientAdapterSurfaceSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAdapterSurfaceSummaryExchange) adapterSurfaceSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     adapterSurfaceSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{adapterSurfaceSummaryResultRow(e.Row)},
		Final: true,
	})
}

func adapterSurfaceSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Surface_count", Type: DataTypeInt},
		{Name: "Client_facing_count", Type: DataTypeInt},
		{Name: "Control_plane_count", Type: DataTypeInt},
		{Name: "Embedded_count", Type: DataTypeInt},
		{Name: "Internal_count", Type: DataTypeInt},
		{Name: "Uses_qsbridge_count", Type: DataTypeInt},
		{Name: "Mysql_protocol_count", Type: DataTypeInt},
		{Name: "Grpc_protocol_count", Type: DataTypeInt},
		{Name: "In_process_transport_count", Type: DataTypeInt},
	}
}

func adapterSurfaceSummaryResultRow(row AdapterSurfaceSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.SurfaceCount),
		metadataIntCell(row.ClientFacingCount),
		metadataIntCell(row.ControlPlaneCount),
		metadataIntCell(row.EmbeddedCount),
		metadataIntCell(row.InternalCount),
		metadataIntCell(row.UsesQSBridgeCount),
		metadataIntCell(row.MySQLProtocolCount),
		metadataIntCell(row.GRPCProtocolCount),
		metadataIntCell(row.InProcessTransportCount),
	}
}
