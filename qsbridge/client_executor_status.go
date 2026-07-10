package qsbridge

// ClientExecutorStatusRow describes one configured executor boundary.
type ClientExecutorStatusRow struct {
	Target        DispatchTarget
	Configured    bool
	SingleRequest bool
	BatchRequest  bool
	Detail        string
}

// ClientExecutorStatusSummaryRow describes aggregate executor boundary status.
type ClientExecutorStatusSummaryRow struct {
	ExecutorCount      int
	ConfiguredCount    int
	MissingCount       int
	SingleRequestCount int
	BatchRequestCount  int
	AllConfigured      bool
	AllSingleRequest   bool
	AllBatchRequest    bool
}

// ClientExecutorStatusExchange is adapter-facing executor boundary metadata.
type ClientExecutorStatusExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Rows                []ClientExecutorStatusRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientExecutorStatus returns non-executing status rows for executor boundaries.
func (s PlanningService) ListClientExecutorStatus(connection ConnectionContext, dispatcher ExecutionDispatcher) ClientExecutorStatusExchange {
	_ = s
	exchange := ClientExecutorStatusExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = executorStatusRows(dispatcher)
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.executorStatusResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether executor status metadata can be returned.
func (e ClientExecutorStatusExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientExecutorStatusExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientExecutorStatusExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientExecutorStatusExchange) executorStatusResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     executorStatusResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.executorStatusResultRows(),
		Final: true,
	})
}

func executorStatusResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Executor", Type: DataTypeString},
		{Name: "Configured", Type: DataTypeBool},
		{Name: "Single_request", Type: DataTypeBool},
		{Name: "Batch_request", Type: DataTypeBool},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientExecutorStatusExchange) executorStatusResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Target)),
			metadataBoolCell(row.Configured),
			metadataBoolCell(row.SingleRequest),
			metadataBoolCell(row.BatchRequest),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}

func executorStatusRows(dispatcher ExecutionDispatcher) []ClientExecutorStatusRow {
	return []ClientExecutorStatusRow{
		{
			Target:        DispatchTargetNative,
			Configured:    dispatcher.Native != nil,
			SingleRequest: dispatcher.Native != nil,
			BatchRequest:  dispatcher.Native != nil,
			Detail:        executorStatusDetail(DispatchTargetNative, dispatcher.Native != nil),
		},
		{
			Target:        DispatchTargetLegacy,
			Configured:    dispatcher.Legacy != nil,
			SingleRequest: dispatcher.Legacy != nil,
			BatchRequest:  dispatcher.Legacy != nil,
			Detail:        executorStatusDetail(DispatchTargetLegacy, dispatcher.Legacy != nil),
		},
	}
}

func summarizeExecutorStatusRows(rows []ClientExecutorStatusRow) ClientExecutorStatusSummaryRow {
	summary := ClientExecutorStatusSummaryRow{
		ExecutorCount:    len(rows),
		AllConfigured:    len(rows) > 0,
		AllSingleRequest: len(rows) > 0,
		AllBatchRequest:  len(rows) > 0,
	}
	for _, row := range rows {
		if row.Configured {
			summary.ConfiguredCount++
		} else {
			summary.MissingCount++
			summary.AllConfigured = false
		}
		if row.SingleRequest {
			summary.SingleRequestCount++
		} else {
			summary.AllSingleRequest = false
		}
		if row.BatchRequest {
			summary.BatchRequestCount++
		} else {
			summary.AllBatchRequest = false
		}
	}
	return summary
}

func executorStatusDetail(target DispatchTarget, configured bool) string {
	if configured {
		return string(target) + " is configured"
	}
	return string(target) + " is not configured"
}
