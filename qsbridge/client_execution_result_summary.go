package qsbridge

// ClientExecutionResultSummaryRow describes one single execution result envelope.
type ClientExecutionResultSummaryRow struct {
	RequestID      ExecutionRequestID
	Kind           ResultKind
	Status         ExecutionStatus
	AccessIntent   PhysicalAccessIntent
	Lifecycle      ClientPlanLifecycleKind
	LifecycleSteps int
	Complete       bool
	RowsReturned   uint64
	AffectedRows   uint64
	ResultColumns  int
	ResultChunks   int
	SessionActions int
	Cursor         CursorID
	CursorState    CursorState
	Canceled       bool
	Profiled       bool
	Diagnostics    []DiagnosticCode
}

// ClientExecutionResultSummaryExchange is adapter-facing single-result metadata.
type ClientExecutionResultSummaryExchange struct {
	Connection   ConnectionContext
	Execution    ExecutionResult
	Rows         []ClientExecutionResultSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientExecutionResultSummary returns compact metadata for one execution result.
func (s PlanningService) ListClientExecutionResultSummary(connection ConnectionContext, execution ExecutionResult) ClientExecutionResultSummaryExchange {
	_ = s
	exchange := ClientExecutionResultSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Execution:   cloneExecutionResult(execution),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, execution.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientExecutionResultSummaryRow{executionResultSummaryRow(execution)}
	}
	exchange.Result = exchange.executionResultSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether execution result summary metadata can be returned.
func (e ClientExecutionResultSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts execution result summary diagnostics into protocol-facing errors.
func (e ClientExecutionResultSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking execution result summary error, if any.
func (e ClientExecutionResultSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientExecutionResultSummaryExchange) executionResultSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     executionResultSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.executionResultSummaryRows(),
		Final: true,
	})
}

func executionResultSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Complete", Type: DataTypeBool},
		{Name: "Rows_returned", Type: DataTypeInt},
		{Name: "Affected_rows", Type: DataTypeInt},
		{Name: "Result_columns", Type: DataTypeInt},
		{Name: "Result_chunks", Type: DataTypeInt},
		{Name: "Session_actions", Type: DataTypeInt},
		{Name: "Cursor", Type: DataTypeString, Nullable: true},
		{Name: "Cursor_state", Type: DataTypeString, Nullable: true},
		{Name: "Canceled", Type: DataTypeBool},
		{Name: "Profiled", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientExecutionResultSummaryExchange) executionResultSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.Status)),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataBoolCell(row.Complete),
			metadataIntCell(int(row.RowsReturned)),
			metadataIntCell(int(row.AffectedRows)),
			metadataIntCell(row.ResultColumns),
			metadataIntCell(row.ResultChunks),
			metadataIntCell(row.SessionActions),
			metadataStringCell(string(row.Cursor)),
			metadataStringCell(string(row.CursorState)),
			metadataBoolCell(row.Canceled),
			metadataBoolCell(row.Profiled),
			metadataStringCell(joinDiagnosticCodes(row.Diagnostics)),
		})
	}
	return rows
}

func executionResultSummaryRow(execution ExecutionResult) ClientExecutionResultSummaryRow {
	return ClientExecutionResultSummaryRow{
		RequestID:      execution.RequestID,
		Kind:           execution.Kind,
		Status:         execution.Status,
		AccessIntent:   execution.Profile.AccessIntent,
		Lifecycle:      execution.Profile.Lifecycle,
		LifecycleSteps: execution.Profile.LifecycleSteps,
		Complete:       execution.Complete,
		RowsReturned:   execution.RowsReturned,
		AffectedRows:   execution.Statement.AffectedRows,
		ResultColumns:  len(execution.Columns),
		ResultChunks:   len(execution.Chunks),
		SessionActions: len(execution.SessionActions),
		Cursor:         execution.Cursor.ID,
		CursorState:    execution.Cursor.State,
		Canceled:       execution.Status == ExecutionCanceled || execution.Cancellation.Supported(),
		Profiled:       !execution.Profile.Empty(),
		Diagnostics:    execution.Diagnostics.Codes(),
	}
}
