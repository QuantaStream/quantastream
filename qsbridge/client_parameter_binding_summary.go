package qsbridge

// ClientParameterBindingSummaryExchange is adapter-facing parameter binding summary metadata.
type ClientParameterBindingSummaryExchange struct {
	Connection          ConnectionContext
	Prepared            PreparedPlan
	Values              []ParameterValue
	Bindings            ParameterBindingSet
	Row                 ClientParameterBindingSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientParameterBindings validates supplied values and returns aggregate binding metadata.
func (s PlanningService) SummarizeClientParameterBindings(connection ConnectionContext, prepared PreparedPlan, values ...ParameterValue) ClientParameterBindingSummaryExchange {
	_ = s
	bindings := BindParameterValues(prepared.Parameters, values...)
	exchange := ClientParameterBindingSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Prepared:            clonePreparedPlan(prepared),
		Values:              append([]ParameterValue(nil), values...),
		Bindings:            cloneParameterBindingSet(bindings),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeParameterBindingRows(parameterBindingRows(prepared.Parameters, values))
	}
	exchange.Result = exchange.parameterBindingSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether parameter binding summary metadata can be returned.
func (e ClientParameterBindingSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientParameterBindingSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientParameterBindingSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientParameterBindingSummaryExchange) parameterBindingSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     parameterBindingSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{parameterBindingSummaryResultRow(e.Row)},
		Final: true,
	})
}

func parameterBindingSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Parameter_count", Type: DataTypeInt},
		{Name: "Required_count", Type: DataTypeInt},
		{Name: "Named_count", Type: DataTypeInt},
		{Name: "Positional_count", Type: DataTypeInt},
		{Name: "Nullable_count", Type: DataTypeInt},
		{Name: "Present_count", Type: DataTypeInt},
		{Name: "Bound_count", Type: DataTypeInt},
		{Name: "Missing_count", Type: DataTypeInt},
		{Name: "Extra_count", Type: DataTypeInt},
		{Name: "Type_mismatch_count", Type: DataTypeInt},
		{Name: "Null_not_allowed_count", Type: DataTypeInt},
	}
}

func parameterBindingSummaryResultRow(row ClientParameterBindingSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.ParameterCount),
		metadataIntCell(row.RequiredCount),
		metadataIntCell(row.NamedCount),
		metadataIntCell(row.PositionalCount),
		metadataIntCell(row.NullableCount),
		metadataIntCell(row.PresentCount),
		metadataIntCell(row.BoundCount),
		metadataIntCell(row.MissingCount),
		metadataIntCell(row.ExtraCount),
		metadataIntCell(row.TypeMismatchCount),
		metadataIntCell(row.NullNotAllowedCount),
	}
}
