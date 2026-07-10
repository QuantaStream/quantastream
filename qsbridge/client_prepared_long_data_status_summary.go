package qsbridge

// ClientPreparedLongDataStatusSummaryExchange is adapter-facing long-data inventory summary metadata.
type ClientPreparedLongDataStatusSummaryExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Row                 ClientPreparedLongDataStatusSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientPreparedLongDataStatus returns aggregate prepared long-data inventory metadata.
func (s PlanningService) SummarizeClientPreparedLongDataStatus(connection ConnectionContext, registry PreparedLongDataRegistry) ClientPreparedLongDataStatusSummaryExchange {
	_ = s
	exchange := ClientPreparedLongDataStatusSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
		exchange.Result = exchange.preparedLongDataStatusSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if registry == nil {
		exchange.ExchangeDiagnostics = mergeDiagnosticSets(exchange.ExchangeDiagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared long-data registry is not configured"),
		})
	} else {
		exchange.Row = summarizePreparedLongDataRows(preparedLongDataRows(registry.List()))
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.preparedLongDataStatusSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepared long-data inventory summary metadata can be returned.
func (e ClientPreparedLongDataStatusSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientPreparedLongDataStatusSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientPreparedLongDataStatusSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientPreparedLongDataStatusSummaryExchange) preparedLongDataStatusSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedLongDataStatusSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{preparedLongDataStatusSummaryResultRow(e.Row)},
		Final: true,
	})
}

func preparedLongDataStatusSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "State_count", Type: DataTypeInt},
		{Name: "Named_statement_count", Type: DataTypeInt},
		{Name: "Final_state_count", Type: DataTypeInt},
		{Name: "String_kind_count", Type: DataTypeInt},
		{Name: "Total_chunks", Type: DataTypeInt},
		{Name: "Total_bytes", Type: DataTypeInt},
		{Name: "Largest_state_bytes", Type: DataTypeInt},
		{Name: "Distinct_statement_count", Type: DataTypeInt},
	}
}

func preparedLongDataStatusSummaryResultRow(row ClientPreparedLongDataStatusSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.StateCount),
		metadataIntCell(row.NamedStatementCount),
		metadataIntCell(row.FinalStateCount),
		metadataIntCell(row.StringKindCount),
		metadataIntCell(int(row.TotalChunks)),
		metadataIntCell(int(row.TotalBytes)),
		metadataIntCell(int(row.LargestStateBytes)),
		metadataIntCell(row.DistinctStatementCount),
	}
}
