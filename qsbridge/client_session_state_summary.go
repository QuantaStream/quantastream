package qsbridge

// ClientSessionStateSummaryExchange is adapter-facing metadata for aggregate session-state changes.
type ClientSessionStateSummaryExchange struct {
	Connection   ConnectionContext
	Response     ProtocolStatementResponse
	Row          ClientSessionStateSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientSessionStateChanges returns aggregate protocol-neutral metadata for response session actions.
func (s PlanningService) SummarizeClientSessionStateChanges(connection ConnectionContext, response ProtocolStatementResponse) ClientSessionStateSummaryExchange {
	_ = s
	exchange := ClientSessionStateSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Response:    cloneProtocolStatementResponse(response),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, response.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.sessionStateSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, validateClientSessionActions(connection.Protocol, response.SessionActions))
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Row = summarizeSessionStateChanges(sessionStateChanges(response.SessionActions))
	}
	exchange.Result = exchange.sessionStateSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether session-state summary metadata can be returned.
func (e ClientSessionStateSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts session-state summary diagnostics into protocol-facing errors.
func (e ClientSessionStateSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking session-state summary error, if any.
func (e ClientSessionStateSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientSessionStateSummaryExchange) sessionStateSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     sessionStateSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{sessionStateSummaryResultRow(e.Row)},
		Final: true,
	})
}

func sessionStateSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Change_count", Type: DataTypeInt},
		{Name: "Schema_change_count", Type: DataTypeInt},
		{Name: "Variable_change_count", Type: DataTypeInt},
		{Name: "Transaction_count", Type: DataTypeInt},
		{Name: "Reset_connection_count", Type: DataTypeInt},
		{Name: "Change_user_count", Type: DataTypeInt},
	}
}

func sessionStateSummaryResultRow(row ClientSessionStateSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.ChangeCount),
		metadataIntCell(row.SchemaChangeCount),
		metadataIntCell(row.VariableChangeCount),
		metadataIntCell(row.TransactionCount),
		metadataIntCell(row.ResetConnectionCount),
		metadataIntCell(row.ChangeUserCount),
	}
}
