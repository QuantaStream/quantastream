package qsbridge

// ClientBatchResultChunkSummaryRow describes one batch item result chunk.
type ClientBatchResultChunkSummaryRow struct {
	Item            int
	BatchID         ExecutionRequestID
	RequestID       ExecutionRequestID
	Chunk           int
	Sequence        int
	Rows            int
	Final           bool
	ResultStatus    ExecutionStatus
	ResultKind      ResultKind
	RowsReturned    uint64
	Cursor          CursorID
	CursorState     CursorState
	DiagnosticCodes []DiagnosticCode
}

// ClientBatchResultChunkSummaryExchange is adapter-facing batch chunk metadata.
type ClientBatchResultChunkSummaryExchange struct {
	Connection   ConnectionContext
	Batch        BatchExecutionResult
	Rows         []ClientBatchResultChunkSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientBatchResultChunkSummary returns chunk metadata rows for every batch item.
func (s PlanningService) ListClientBatchResultChunkSummary(connection ConnectionContext, batch BatchExecutionResult) ClientBatchResultChunkSummaryExchange {
	_ = s
	exchange := ClientBatchResultChunkSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Batch:       cloneBatchExecutionResult(batch),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, batch.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = batchResultChunkSummaryRows(batch)
	}
	exchange.Result = exchange.batchResultChunkSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch result chunk summary metadata can be returned.
func (e ClientBatchResultChunkSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch result chunk summary diagnostics into protocol-facing errors.
func (e ClientBatchResultChunkSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch result chunk summary error, if any.
func (e ClientBatchResultChunkSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientBatchResultChunkSummaryExchange) batchResultChunkSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchResultChunkSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.batchResultChunkSummaryResultRows(),
		Final: true,
	})
}

func batchResultChunkSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Item", Type: DataTypeInt},
		{Name: "Batch_id", Type: DataTypeString, Nullable: true},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Chunk", Type: DataTypeInt},
		{Name: "Sequence", Type: DataTypeInt},
		{Name: "Rows", Type: DataTypeInt},
		{Name: "Final", Type: DataTypeBool},
		{Name: "Result_status", Type: DataTypeString, Nullable: true},
		{Name: "Result_kind", Type: DataTypeString, Nullable: true},
		{Name: "Rows_returned", Type: DataTypeInt},
		{Name: "Cursor", Type: DataTypeString, Nullable: true},
		{Name: "Cursor_state", Type: DataTypeString, Nullable: true},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientBatchResultChunkSummaryExchange) batchResultChunkSummaryResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Item),
			metadataStringCell(string(row.BatchID)),
			metadataStringCell(string(row.RequestID)),
			metadataIntCell(row.Chunk),
			metadataIntCell(row.Sequence),
			metadataIntCell(row.Rows),
			metadataBoolCell(row.Final),
			metadataStringCell(string(row.ResultStatus)),
			metadataStringCell(string(row.ResultKind)),
			metadataIntCell(int(row.RowsReturned)),
			metadataStringCell(string(row.Cursor)),
			metadataStringCell(string(row.CursorState)),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func batchResultChunkSummaryRows(batch BatchExecutionResult) []ClientBatchResultChunkSummaryRow {
	if len(batch.Items) == 0 {
		return nil
	}
	rows := make([]ClientBatchResultChunkSummaryRow, 0, len(batch.Items))
	for itemIndex, item := range batch.Items {
		for _, chunk := range resultChunkSummaryRows(item) {
			rows = append(rows, ClientBatchResultChunkSummaryRow{
				Item:            itemIndex,
				BatchID:         batch.RequestID,
				RequestID:       chunk.RequestID,
				Chunk:           chunk.Chunk,
				Sequence:        chunk.Sequence,
				Rows:            chunk.Rows,
				Final:           chunk.Final,
				ResultStatus:    chunk.ResultStatus,
				ResultKind:      chunk.ResultKind,
				RowsReturned:    chunk.RowsReturned,
				Cursor:          chunk.Cursor,
				CursorState:     chunk.CursorState,
				DiagnosticCodes: append([]DiagnosticCode(nil), chunk.DiagnosticCodes...),
			})
		}
	}
	return rows
}
