package qsbridge

// ClientTransactionSummaryRow describes one transaction metadata exchange.
type ClientTransactionSummaryRow struct {
	Action          SessionActionKind
	Status          string
	Supported       bool
	Applied         bool
	SessionActions  int
	WarningCount    int
	StatusFlags     []ProtocolStatusFlag
	DiagnosticCodes []DiagnosticCode
}

// ClientTransactionSummaryExchange is adapter-facing transaction response metadata.
type ClientTransactionSummaryExchange struct {
	Connection          ConnectionContext
	Transaction         ClientTransactionExchange
	Rows                []ClientTransactionSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientTransaction returns row metadata for one transaction exchange.
func (s PlanningService) SummarizeClientTransaction(connection ConnectionContext, transaction ClientTransactionExchange) ClientTransactionSummaryExchange {
	_ = s
	exchange := ClientTransactionSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Transaction:         cloneClientTransactionExchange(transaction),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientTransactionSummaryRow{transactionSummaryRow(transaction)}
	}
	exchange.Result = exchange.transactionSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether transaction summary metadata can be returned.
func (e ClientTransactionSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientTransactionSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientTransactionSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientTransactionSummaryExchange) transactionSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     transactionSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.transactionSummaryRows(),
		Final: true,
	})
}

func transactionSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Action", Type: DataTypeString, Nullable: true},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Applied", Type: DataTypeBool},
		{Name: "Session_actions", Type: DataTypeInt},
		{Name: "Warnings", Type: DataTypeInt},
		{Name: "Status_flags", Type: DataTypeString, Nullable: true},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientTransactionSummaryExchange) transactionSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Action)),
			metadataStringCell(row.Status),
			metadataBoolCell(row.Supported),
			metadataBoolCell(row.Applied),
			metadataIntCell(row.SessionActions),
			metadataIntCell(row.WarningCount),
			metadataStringCell(joinProtocolStatusFlags(row.StatusFlags)),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func transactionSummaryRow(transaction ClientTransactionExchange) ClientTransactionSummaryRow {
	return ClientTransactionSummaryRow{
		Action:          transaction.Action.Kind,
		Status:          transaction.Response.Status,
		Supported:       transaction.Supported(),
		Applied:         transaction.Session.Applied,
		SessionActions:  len(transaction.Response.SessionActions),
		WarningCount:    int(transaction.Response.Warnings),
		StatusFlags:     append([]ProtocolStatusFlag(nil), transaction.Response.Flags...),
		DiagnosticCodes: transaction.Diagnostics.Codes(),
	}
}

func cloneClientTransactionExchange(exchange ClientTransactionExchange) ClientTransactionExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Session = cloneClientSessionActionExchange(exchange.Session)
	exchange.Response = cloneProtocolStatementResponse(exchange.Response)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}

func cloneClientSessionActionExchange(exchange ClientSessionActionExchange) ClientSessionActionExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Transition = cloneSessionTransition(exchange.Transition)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}
