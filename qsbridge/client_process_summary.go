package qsbridge

import "sort"

// ClientProcessListSummaryExchange is adapter-facing metadata for aggregate process-list metadata.
type ClientProcessListSummaryExchange struct {
	Connection   ConnectionContext
	Row          ClientProcessListSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientExecutionProcesses returns aggregate process-list style metadata for registered executions.
func (s PlanningService) SummarizeClientExecutionProcesses(connection ConnectionContext, registry ExecutionRegistry) ClientProcessListSummaryExchange {
	_ = s
	exchange := ClientProcessListSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.processListSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if registry == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "execution registry is not configured"),
		})
		exchange.Result = exchange.processListSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	records := cloneExecutionRecords(registry.List())
	sort.Slice(records, func(i, j int) bool {
		return records[i].ID < records[j].ID
	})
	exchange.Row = summarizeClientExecutionProcesses(records)
	exchange.Result = exchange.processListSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether process-list summary metadata can be returned.
func (e ClientProcessListSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts process-list summary diagnostics into protocol-facing errors.
func (e ClientProcessListSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking process-list summary error, if any.
func (e ClientProcessListSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientProcessListSummaryExchange) processListSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     processListSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{processListSummaryResultRow(e.Row)},
		Final: true,
	})
}

func processListSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Process_count", Type: DataTypeInt},
		{Name: "Single_request_count", Type: DataTypeInt},
		{Name: "Batch_request_count", Type: DataTypeInt},
		{Name: "Pending_count", Type: DataTypeInt},
		{Name: "Streaming_count", Type: DataTypeInt},
		{Name: "Complete_count", Type: DataTypeInt},
		{Name: "Failed_count", Type: DataTypeInt},
		{Name: "Cancel_requested_count", Type: DataTypeInt},
		{Name: "Cancelable_count", Type: DataTypeInt},
	}
}

func processListSummaryResultRow(row ClientProcessListSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.ProcessCount),
		metadataIntCell(row.SingleRequestCount),
		metadataIntCell(row.BatchRequestCount),
		metadataIntCell(row.PendingCount),
		metadataIntCell(row.StreamingCount),
		metadataIntCell(row.CompleteCount),
		metadataIntCell(row.FailedCount),
		metadataIntCell(row.CancelRequestedCount),
		metadataIntCell(row.CancelableCount),
	}
}
