package qsbridge

import "sort"

// ClientProcessListExchange is adapter-facing metadata for in-flight requests.
type ClientProcessListExchange struct {
	Connection   ConnectionContext
	Records      []ExecutionRecord
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ClientProcessListSummaryRow describes aggregate process-list metadata.
type ClientProcessListSummaryRow struct {
	ProcessCount         int
	SingleRequestCount   int
	BatchRequestCount    int
	PendingCount         int
	StreamingCount       int
	CompleteCount        int
	FailedCount          int
	CancelRequestedCount int
	CancelableCount      int
}

// ListClientExecutionProcesses returns process-list style metadata for registered executions.
func (s PlanningService) ListClientExecutionProcesses(connection ConnectionContext, registry ExecutionRegistry) ClientProcessListExchange {
	_ = s
	exchange := ClientProcessListExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.processListResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if registry == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "execution registry is not configured"),
		})
		exchange.Result = exchange.processListResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Records = cloneExecutionRecords(registry.List())
	sort.Slice(exchange.Records, func(i, j int) bool {
		return exchange.Records[i].ID < exchange.Records[j].ID
	})
	exchange.Result = exchange.processListResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether process-list metadata can be returned.
func (e ClientProcessListExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts process-list diagnostics into protocol-facing errors.
func (e ClientProcessListExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking process-list error, if any.
func (e ClientProcessListExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientProcessListExchange) processListResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     processListResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.processListRows(),
		Final: true,
	})
}

func processListResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Id", Type: DataTypeString},
		{Name: "Kind", Type: DataTypeString},
		{Name: "Status", Type: DataTypeString},
		{Name: "User", Type: DataTypeString},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "Cancelable", Type: DataTypeBool},
		{Name: "SQL", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientProcessListExchange) processListRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Records))
	for _, record := range e.Records {
		rows = append(rows, ResultRow{
			metadataStringCell(string(record.ID)),
			metadataStringCell(string(record.Kind)),
			metadataStringCell(string(record.Status)),
			metadataStringCell(string(record.Session.User)),
			metadataStringCell(record.Session.CurrentSchema),
			metadataBoolCell(record.Options.Cancelable),
			metadataStringCell(executionRecordSQL(record)),
		})
	}
	return rows
}

func executionRecordSQL(record ExecutionRecord) string {
	switch record.Kind {
	case ExecutionRequestBatch:
		return record.Batch.Prepared.SQL
	default:
		return record.Request.Bound.Prepared.SQL
	}
}

func cloneExecutionRecords(records []ExecutionRecord) []ExecutionRecord {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]ExecutionRecord, 0, len(records))
	for _, record := range records {
		cloned = append(cloned, cloneExecutionRecord(record))
	}
	return cloned
}

func summarizeClientExecutionProcesses(records []ExecutionRecord) ClientProcessListSummaryRow {
	summary := ClientProcessListSummaryRow{ProcessCount: len(records)}
	for _, record := range records {
		switch record.Kind {
		case ExecutionRequestBatch:
			summary.BatchRequestCount++
		default:
			summary.SingleRequestCount++
		}
		switch record.Status {
		case ExecutionPending:
			summary.PendingCount++
		case ExecutionStreaming:
			summary.StreamingCount++
		case ExecutionComplete:
			summary.CompleteCount++
		case ExecutionFailed:
			summary.FailedCount++
		case ExecutionCancelRequested:
			summary.CancelRequestedCount++
		}
		if record.Options.Cancelable {
			summary.CancelableCount++
		}
	}
	return summary
}
