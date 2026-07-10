package qsbridge

// ClientSQLFeatureCoverageExchange is adapter-facing prepared-plan SQL feature coverage metadata.
type ClientSQLFeatureCoverageExchange struct {
	Connection   ConnectionContext
	Report       SQLFeatureCoverageReport
	Rows         []SQLFeatureCoverage
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientPreparedSQLFeatureCoverage returns SQL feature coverage rows for a prepared plan.
func (s PlanningService) ListClientPreparedSQLFeatureCoverage(connection ConnectionContext, prepared PreparedPlan) ClientSQLFeatureCoverageExchange {
	report := prepared.SQLFeatureCoverage(s.SQLFeatureMatrix())
	exchange := ClientSQLFeatureCoverageExchange{
		Connection:  cloneConnectionContext(connection),
		Report:      cloneSQLFeatureCoverageReport(report),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), report.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = cloneSQLFeatureCoverage(report.Coverage)
	}
	exchange.Result = exchange.sqlFeatureCoverageResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether SQL feature coverage metadata can be returned.
func (e ClientSQLFeatureCoverageExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts SQL feature coverage diagnostics into protocol-facing errors.
func (e ClientSQLFeatureCoverageExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking SQL feature coverage error, if any.
func (e ClientSQLFeatureCoverageExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientSQLFeatureCoverageExchange) sqlFeatureCoverageResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     sqlFeatureCoverageResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.sqlFeatureCoverageResultRows(),
		Final: true,
	})
}

func sqlFeatureCoverageResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Feature", Type: DataTypeString},
		{Name: "Category", Type: DataTypeString},
		{Name: "Status", Type: DataTypeString},
		{Name: "Present", Type: DataTypeBool},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientSQLFeatureCoverageExchange) sqlFeatureCoverageResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, coverage := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(coverage.Feature.Name),
			metadataStringCell(string(coverage.Feature.Category)),
			metadataStringCell(string(coverage.Feature.Status)),
			metadataBoolCell(coverage.Present),
			metadataBoolCell(coverage.Supported),
			metadataStringCell(joinPlanCapabilities(coverage.Capabilities)),
			metadataStringCell(joinDiagnosticCodes(coverage.Diagnostics)),
			metadataStringCell(coverage.Detail),
		})
	}
	return rows
}

func cloneSQLFeatureCoverageReport(report SQLFeatureCoverageReport) SQLFeatureCoverageReport {
	return SQLFeatureCoverageReport{
		Prepared:    clonePreparedPlan(report.Prepared),
		Matrix:      report.Matrix.Clone(),
		Coverage:    cloneSQLFeatureCoverage(report.Coverage),
		Diagnostics: cloneDiagnosticSet(report.Diagnostics),
	}
}

func cloneSQLFeatureCoverage(items []SQLFeatureCoverage) []SQLFeatureCoverage {
	if len(items) == 0 {
		return nil
	}
	cloned := make([]SQLFeatureCoverage, 0, len(items))
	for _, item := range items {
		item.Feature = cloneSQLFeature(item.Feature)
		item.Capabilities = append([]PlanCapability(nil), item.Capabilities...)
		item.Diagnostics = append([]DiagnosticCode(nil), item.Diagnostics...)
		cloned = append(cloned, item)
	}
	return cloned
}
