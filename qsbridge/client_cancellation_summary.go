package qsbridge

// ClientCancellationSummaryRow describes one cancellation exchange for adapters.
type ClientCancellationSummaryRow struct {
	RequestID       ExecutionRequestID
	Kind            ExecutionRequestKind
	PreviousStatus  ExecutionStatus
	CancelReason    CancellationReason
	Force           bool
	Recorded        bool
	Supported       bool
	ResultStatus    ExecutionStatus
	BatchStatus     ExecutionStatus
	DiagnosticCodes []DiagnosticCode
	Message         string
}

// ClientCancellationSummaryExchange is adapter-facing cancellation result metadata.
type ClientCancellationSummaryExchange struct {
	Connection          ConnectionContext
	Cancellation        ClientCancellationExchange
	Rows                []ClientCancellationSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientCancellation returns row metadata for one cancellation exchange.
func (s PlanningService) SummarizeClientCancellation(connection ConnectionContext, cancellation ClientCancellationExchange) ClientCancellationSummaryExchange {
	_ = s
	exchange := ClientCancellationSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Cancellation:        cloneClientCancellationExchange(cancellation),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientCancellationSummaryRow{cancellationSummaryRow(cancellation)}
	}
	exchange.Result = exchange.cancellationSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether cancellation summary metadata can be returned.
func (e ClientCancellationSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientCancellationSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientCancellationSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientCancellationSummaryExchange) cancellationSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     cancellationSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.cancellationSummaryRows(),
		Final: true,
	})
}

func cancellationSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Request_id", Type: DataTypeString},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Previous_status", Type: DataTypeString, Nullable: true},
		{Name: "Cancel_reason", Type: DataTypeString, Nullable: true},
		{Name: "Force", Type: DataTypeBool},
		{Name: "Recorded", Type: DataTypeBool},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Result_status", Type: DataTypeString, Nullable: true},
		{Name: "Batch_status", Type: DataTypeString, Nullable: true},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "Message", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientCancellationSummaryExchange) cancellationSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.PreviousStatus)),
			metadataStringCell(string(row.CancelReason)),
			metadataBoolCell(row.Force),
			metadataBoolCell(row.Recorded),
			metadataBoolCell(row.Supported),
			metadataStringCell(string(row.ResultStatus)),
			metadataStringCell(string(row.BatchStatus)),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
			metadataStringCell(row.Message),
		})
	}
	return rows
}

func cancellationSummaryRow(cancellation ClientCancellationExchange) ClientCancellationSummaryRow {
	return ClientCancellationSummaryRow{
		RequestID:       cancellation.Cancellation.RequestID,
		Kind:            cancellation.Record.Kind,
		PreviousStatus:  cancellation.Record.Status,
		CancelReason:    cancellation.Cancellation.Reason,
		Force:           cancellation.Cancellation.Force,
		Recorded:        cancellation.Record.ID != "",
		Supported:       cancellation.Supported(),
		ResultStatus:    cancellation.Result.Status,
		BatchStatus:     cancellation.BatchResult.Status,
		DiagnosticCodes: cancellation.Diagnostics.Codes(),
		Message:         cancellation.Cancellation.Message,
	}
}

func cloneClientCancellationExchange(exchange ClientCancellationExchange) ClientCancellationExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Record = cloneExecutionRecord(exchange.Record)
	exchange.Cancellation = cloneCancellationRequest(exchange.Cancellation)
	exchange.Result = cloneExecutionResult(exchange.Result)
	exchange.BatchResult = cloneBatchExecutionResult(exchange.BatchResult)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}
