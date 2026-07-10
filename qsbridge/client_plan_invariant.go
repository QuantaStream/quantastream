package qsbridge

// ClientPlanInvariantExchange is adapter-facing prepared-plan invariant metadata.
type ClientPlanInvariantExchange struct {
	Connection   ConnectionContext
	Report       PlanInvariantReport
	Rows         []PlanInvariantCheck
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientPreparedPlanInvariants returns consistency rows for a prepared plan.
func (s PlanningService) ListClientPreparedPlanInvariants(connection ConnectionContext, prepared PreparedPlan) ClientPlanInvariantExchange {
	_ = s
	report := prepared.PlanInvariants()
	exchange := ClientPlanInvariantExchange{
		Connection:  cloneConnectionContext(connection),
		Report:      report.Clone(),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), report.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = clonePlanInvariantChecks(report.Checks)
	}
	exchange.Result = exchange.planInvariantResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepared-plan invariant metadata can be returned and all invariants pass.
func (e ClientPlanInvariantExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts invariant diagnostics into protocol-facing errors.
func (e ClientPlanInvariantExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking invariant error, if any.
func (e ClientPlanInvariantExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientPlanInvariantExchange) planInvariantResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     planInvariantResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.planInvariantResultRows(),
		Final: true,
	})
}

func planInvariantResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Invariant", Type: DataTypeString},
		{Name: "Status", Type: DataTypeString},
		{Name: "Diagnostic", Type: DataTypeString, Nullable: true},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPlanInvariantExchange) planInvariantResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(row.Name),
			metadataStringCell(string(row.Status)),
			metadataStringCell(string(row.Diagnostic)),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}
