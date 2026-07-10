package qsbridge

// ClientPreparedMetadataSummaryExchange is adapter-facing metadata for aggregate prepared metadata.
type ClientPreparedMetadataSummaryExchange struct {
	Connection   ConnectionContext
	Prepared     PreparedPlan
	Row          ClientPreparedMetadataSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientPreparedMetadata returns aggregate prepared parameter and result-column metadata.
func (s PlanningService) SummarizeClientPreparedMetadata(connection ConnectionContext, prepared PreparedPlan) ClientPreparedMetadataSummaryExchange {
	_ = s
	description := prepared.Description()
	exchange := ClientPreparedMetadataSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Prepared:    clonePreparedPlan(prepared),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), description.Diagnostics),
	}
	if connection.Supported() && description.SupportedForPrepare() {
		exchange.Row = summarizePreparedMetadataRows(preparedMetadataRows(description, connection.Protocol))
	}
	exchange.Result = exchange.preparedMetadataSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepare metadata summary can be returned.
func (e ClientPreparedMetadataSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts prepare metadata summary diagnostics into protocol-facing errors.
func (e ClientPreparedMetadataSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking prepare metadata summary error, if any.
func (e ClientPreparedMetadataSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientPreparedMetadataSummaryExchange) preparedMetadataSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedMetadataSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{preparedMetadataSummaryResultRow(e.Row)},
		Final: true,
	})
}

func preparedMetadataSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Row_count", Type: DataTypeInt},
		{Name: "Parameter_count", Type: DataTypeInt},
		{Name: "Result_column_count", Type: DataTypeInt},
		{Name: "Read_intent_count", Type: DataTypeInt},
		{Name: "Write_intent_count", Type: DataTypeInt},
		{Name: "Select_lifecycle_count", Type: DataTypeInt},
		{Name: "Mutation_lifecycle_count", Type: DataTypeInt},
		{Name: "Nullable_count", Type: DataTypeInt},
		{Name: "Source_count", Type: DataTypeInt},
		{Name: "Flagged_count", Type: DataTypeInt},
	}
}

func preparedMetadataSummaryResultRow(row ClientPreparedMetadataSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.RowCount),
		metadataIntCell(row.ParameterCount),
		metadataIntCell(row.ResultColumnCount),
		metadataIntCell(row.ReadIntentCount),
		metadataIntCell(row.WriteIntentCount),
		metadataIntCell(row.SelectLifecycleCount),
		metadataIntCell(row.MutationLifecycleCount),
		metadataIntCell(row.NullableCount),
		metadataIntCell(row.SourceCount),
		metadataIntCell(row.FlaggedCount),
	}
}
