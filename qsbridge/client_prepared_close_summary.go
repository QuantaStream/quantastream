package qsbridge

// ClientPreparedCloseSummaryRow describes one prepared statement close exchange.
type ClientPreparedCloseSummaryRow struct {
	StatementID     PreparedStatementID
	StatementName   string
	Closed          bool
	ResponseKind    ClientResponseKind
	Status          string
	Supported       bool
	DiagnosticCodes []DiagnosticCode
}

// ClientPreparedCloseSummaryExchange is adapter-facing prepared close metadata.
type ClientPreparedCloseSummaryExchange struct {
	Connection          ConnectionContext
	Close               ClientPreparedCloseExchange
	Rows                []ClientPreparedCloseSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientPreparedClose returns row metadata for one prepared close exchange.
func (s PlanningService) SummarizeClientPreparedClose(connection ConnectionContext, close ClientPreparedCloseExchange) ClientPreparedCloseSummaryExchange {
	_ = s
	exchange := ClientPreparedCloseSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Close:               cloneClientPreparedCloseExchange(close),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientPreparedCloseSummaryRow{preparedCloseSummaryRow(close)}
	}
	exchange.Result = exchange.preparedCloseSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepared close summary metadata can be returned.
func (e ClientPreparedCloseSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientPreparedCloseSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientPreparedCloseSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientPreparedCloseSummaryExchange) preparedCloseSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedCloseSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.preparedCloseSummaryRows(),
		Final: true,
	})
}

func preparedCloseSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Statement_id", Type: DataTypeInt},
		{Name: "Statement_name", Type: DataTypeString, Nullable: true},
		{Name: "Closed", Type: DataTypeBool},
		{Name: "Response_kind", Type: DataTypeString, Nullable: true},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPreparedCloseSummaryExchange) preparedCloseSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(int(row.StatementID)),
			metadataStringCell(row.StatementName),
			metadataBoolCell(row.Closed),
			metadataStringCell(string(row.ResponseKind)),
			metadataStringCell(row.Status),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func preparedCloseSummaryRow(close ClientPreparedCloseExchange) ClientPreparedCloseSummaryRow {
	return ClientPreparedCloseSummaryRow{
		StatementID:     close.Request.Handle.ID,
		StatementName:   close.Request.Handle.Name,
		Closed:          close.Closed,
		ResponseKind:    close.Response.Kind,
		Status:          close.Response.StatementResponse.Status,
		Supported:       close.Supported(),
		DiagnosticCodes: close.Diagnostics.Codes(),
	}
}

func cloneClientPreparedCloseExchange(exchange ClientPreparedCloseExchange) ClientPreparedCloseExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Request = clonePreparedStatementCloseRequest(exchange.Request)
	exchange.Response = cloneClientResponseItem(exchange.Response)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}
