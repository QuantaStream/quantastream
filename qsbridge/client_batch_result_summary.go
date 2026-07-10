package qsbridge

// ClientBatchResultSummaryRow describes one item in a batch result envelope.
type ClientBatchResultSummaryRow struct {
	Item           int
	RequestID      ExecutionRequestID
	Kind           ResultKind
	Status         ExecutionStatus
	AccessIntent   PhysicalAccessIntent
	Lifecycle      ClientPlanLifecycleKind
	LifecycleSteps int
	Complete       bool
	RowsReturned   uint64
	AffectedRows   uint64
	Diagnostics    []DiagnosticCode
	SessionActions int
}

// ClientBatchResultSummaryExchange is adapter-facing batch result metadata.
type ClientBatchResultSummaryExchange struct {
	Connection   ConnectionContext
	Batch        BatchExecutionResult
	Rows         []ClientBatchResultSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientBatchResultSummary returns compact metadata for a batch result envelope.
func (s PlanningService) ListClientBatchResultSummary(connection ConnectionContext, batch BatchExecutionResult) ClientBatchResultSummaryExchange {
	_ = s
	exchange := ClientBatchResultSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Batch:       cloneBatchExecutionResult(batch),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, batch.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = batchResultSummaryRows(batch)
	}
	exchange.Result = exchange.batchResultSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch result summary metadata can be returned.
func (e ClientBatchResultSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch result summary diagnostics into protocol-facing errors.
func (e ClientBatchResultSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch result summary error, if any.
func (e ClientBatchResultSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientBatchResultSummaryExchange) batchResultSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchResultSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.batchResultSummaryRows(),
		Final: true,
	})
}

func batchResultSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Item", Type: DataTypeInt},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Complete", Type: DataTypeBool},
		{Name: "Rows_returned", Type: DataTypeInt},
		{Name: "Affected_rows", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "Session_actions", Type: DataTypeInt},
	}
}

func (e ClientBatchResultSummaryExchange) batchResultSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Item),
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.Status)),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataBoolCell(row.Complete),
			metadataIntCell(int(row.RowsReturned)),
			metadataIntCell(int(row.AffectedRows)),
			metadataStringCell(joinDiagnosticCodes(row.Diagnostics)),
			metadataIntCell(row.SessionActions),
		})
	}
	return rows
}

func batchResultSummaryRows(batch BatchExecutionResult) []ClientBatchResultSummaryRow {
	if len(batch.Items) == 0 {
		return nil
	}
	rows := make([]ClientBatchResultSummaryRow, 0, len(batch.Items))
	for i, item := range batch.Items {
		rows = append(rows, ClientBatchResultSummaryRow{
			Item:           i,
			RequestID:      item.RequestID,
			Kind:           item.Kind,
			Status:         item.Status,
			AccessIntent:   item.Profile.AccessIntent,
			Lifecycle:      item.Profile.Lifecycle,
			LifecycleSteps: item.Profile.LifecycleSteps,
			Complete:       item.Complete,
			RowsReturned:   item.RowsReturned,
			AffectedRows:   item.Statement.AffectedRows,
			Diagnostics:    item.Diagnostics.Codes(),
			SessionActions: len(item.SessionActions),
		})
	}
	return rows
}

func cloneBatchExecutionResult(result BatchExecutionResult) BatchExecutionResult {
	items := result.Items
	result.Items = make([]ExecutionResult, 0, len(items))
	for _, item := range items {
		result.Items = append(result.Items, cloneExecutionResult(item))
	}
	result.Diagnostics = cloneDiagnosticSet(result.Diagnostics)
	result.Cancellation = cloneCancellationRequest(result.Cancellation)
	result.SessionActions = cloneSessionActions(result.SessionActions)
	return result
}
