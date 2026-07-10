package qsbridge

// ClientReadinessExchange is adapter-facing scaffold readiness metadata.
type ClientReadinessExchange struct {
	Connection   ConnectionContext
	Report       ReadinessReport
	Rows         []ReadinessSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientReadiness returns protocol-neutral rows for scaffold readiness.
func (s PlanningService) ListClientReadiness(connection ConnectionContext) ClientReadinessExchange {
	report := s.ReadinessReport()
	exchange := ClientReadinessExchange{
		Connection:  cloneConnectionContext(connection),
		Report:      report.Clone(),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), report.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = cloneReadinessRows(report.Rows)
	}
	exchange.Result = exchange.readinessResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether readiness metadata can be returned.
func (e ClientReadinessExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts readiness diagnostics into protocol-facing errors.
func (e ClientReadinessExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking readiness error, if any.
func (e ClientReadinessExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientReadinessExchange) readinessResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     readinessResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.readinessResultRows(),
		Final: true,
	})
}

func readinessResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Scope", Type: DataTypeString},
		{Name: "Name", Type: DataTypeString},
		{Name: "Status", Type: DataTypeString},
		{Name: "Count", Type: DataTypeInt},
	}
}

func (e ClientReadinessExchange) readinessResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(row.Scope),
			metadataStringCell(row.Name),
			metadataStringCell(string(row.Status)),
			metadataIntCell(row.Count),
		})
	}
	return rows
}
