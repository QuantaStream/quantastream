package qsbridge

// ClientReadinessDetailExchange is adapter-facing readiness detail metadata.
type ClientReadinessDetailExchange struct {
	Connection   ConnectionContext
	Report       ReadinessReport
	Rows         []ReadinessDetailRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientReadinessDetails returns normalized compatibility and SQL feature readiness rows.
func (s PlanningService) ListClientReadinessDetails(connection ConnectionContext) ClientReadinessDetailExchange {
	report := s.ReadinessReport()
	exchange := ClientReadinessDetailExchange{
		Connection:  cloneConnectionContext(connection),
		Report:      report.Clone(),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), report.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = cloneReadinessDetailRows(report.Details)
	}
	exchange.Result = exchange.readinessDetailResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether readiness detail metadata can be returned.
func (e ClientReadinessDetailExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts readiness detail diagnostics into protocol-facing errors.
func (e ClientReadinessDetailExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking readiness detail error, if any.
func (e ClientReadinessDetailExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientReadinessDetailExchange) readinessDetailResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     readinessDetailResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.readinessDetailResultRows(),
		Final: true,
	})
}

func readinessDetailResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Scope", Type: DataTypeString},
		{Name: "Name", Type: DataTypeString},
		{Name: "Category", Type: DataTypeString, Nullable: true},
		{Name: "Status", Type: DataTypeString},
		{Name: "Runtime_owned", Type: DataTypeBool},
		{Name: "Adapter_owned", Type: DataTypeBool},
		{Name: "Description", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientReadinessDetailExchange) readinessDetailResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(row.Scope),
			metadataStringCell(row.Name),
			metadataStringCell(row.Category),
			metadataStringCell(string(row.Status)),
			metadataBoolCell(row.RuntimeOwned),
			metadataBoolCell(row.AdapterOwned),
			metadataStringCell(row.Description),
		})
	}
	return rows
}
