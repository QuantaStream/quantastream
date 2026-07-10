package qsbridge

// ClientPreparedResetSummaryRow describes one prepared statement reset exchange.
type ClientPreparedResetSummaryRow struct {
	StatementID     PreparedStatementID
	StatementName   string
	Reset           bool
	ClearedLongData bool
	ResponseKind    ClientResponseKind
	Status          string
	Supported       bool
	DiagnosticCodes []DiagnosticCode
}

// ClientPreparedResetSummaryExchange is adapter-facing prepared reset metadata.
type ClientPreparedResetSummaryExchange struct {
	Connection          ConnectionContext
	Reset               ClientPreparedResetExchange
	Rows                []ClientPreparedResetSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientPreparedReset returns row metadata for one prepared reset exchange.
func (s PlanningService) SummarizeClientPreparedReset(connection ConnectionContext, reset ClientPreparedResetExchange) ClientPreparedResetSummaryExchange {
	_ = s
	exchange := ClientPreparedResetSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Reset:               cloneClientPreparedResetExchange(reset),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientPreparedResetSummaryRow{preparedResetSummaryRow(reset)}
	}
	exchange.Result = exchange.preparedResetSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepared reset summary metadata can be returned.
func (e ClientPreparedResetSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientPreparedResetSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientPreparedResetSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientPreparedResetSummaryExchange) preparedResetSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedResetSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.preparedResetSummaryRows(),
		Final: true,
	})
}

func preparedResetSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Statement_id", Type: DataTypeInt},
		{Name: "Statement_name", Type: DataTypeString, Nullable: true},
		{Name: "Reset", Type: DataTypeBool},
		{Name: "Cleared_long_data", Type: DataTypeBool},
		{Name: "Response_kind", Type: DataTypeString, Nullable: true},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPreparedResetSummaryExchange) preparedResetSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(int(row.StatementID)),
			metadataStringCell(row.StatementName),
			metadataBoolCell(row.Reset),
			metadataBoolCell(row.ClearedLongData),
			metadataStringCell(string(row.ResponseKind)),
			metadataStringCell(row.Status),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func preparedResetSummaryRow(reset ClientPreparedResetExchange) ClientPreparedResetSummaryRow {
	return ClientPreparedResetSummaryRow{
		StatementID:     reset.Handle.ID,
		StatementName:   reset.Handle.Name,
		Reset:           reset.Reset,
		ClearedLongData: reset.ClearedLongData,
		ResponseKind:    reset.Response.Kind,
		Status:          reset.Response.StatementResponse.Status,
		Supported:       reset.Supported(),
		DiagnosticCodes: reset.Diagnostics.Codes(),
	}
}

func cloneClientPreparedResetExchange(exchange ClientPreparedResetExchange) ClientPreparedResetExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Prepared = clonePreparedPlan(exchange.Prepared)
	exchange.Response = cloneClientResponseItem(exchange.Response)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}

func cloneClientResponseItem(item ClientResponseItem) ClientResponseItem {
	item.Outcome.Diagnostics = cloneDiagnosticSet(item.Outcome.Diagnostics)
	item.Result = cloneExecutionResult(item.Result)
	item.Schema = cloneProtocolResultSchema(item.Schema)
	item.StatementResponse = cloneProtocolStatementResponse(item.StatementResponse)
	item.Errors = cloneProtocolErrors(item.Errors)
	item.Flags = append([]ClientResponseFlag(nil), item.Flags...)
	return item
}

func cloneProtocolErrors(errors []ProtocolError) []ProtocolError {
	if len(errors) == 0 {
		return nil
	}
	cloned := make([]ProtocolError, 0, len(errors))
	for _, err := range errors {
		err.Diagnostic = cloneDiagnostic(err.Diagnostic)
		cloned = append(cloned, err)
	}
	return cloned
}
