package qsbridge

// ClientResultChunkSummaryRow describes one result chunk without exposing row values.
type ClientResultChunkSummaryRow struct {
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

// ClientResultChunkSummaryExchange is adapter-facing result chunk metadata.
type ClientResultChunkSummaryExchange struct {
	Connection   ConnectionContext
	Execution    ExecutionResult
	Rows         []ClientResultChunkSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientResultChunkSummary returns one metadata row per execution result chunk.
func (s PlanningService) ListClientResultChunkSummary(connection ConnectionContext, execution ExecutionResult) ClientResultChunkSummaryExchange {
	_ = s
	exchange := ClientResultChunkSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Execution:   cloneExecutionResult(execution),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, execution.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = resultChunkSummaryRows(execution)
	}
	exchange.Result = exchange.resultChunkSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether result chunk summary metadata can be returned.
func (e ClientResultChunkSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts result chunk summary diagnostics into protocol-facing errors.
func (e ClientResultChunkSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking result chunk summary error, if any.
func (e ClientResultChunkSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientResultChunkSummaryExchange) resultChunkSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     resultChunkSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.resultChunkSummaryResultRows(),
		Final: true,
	})
}

func resultChunkSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
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

func (e ClientResultChunkSummaryExchange) resultChunkSummaryResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
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

func resultChunkSummaryRows(execution ExecutionResult) []ClientResultChunkSummaryRow {
	if len(execution.Chunks) == 0 {
		return nil
	}
	rows := make([]ClientResultChunkSummaryRow, 0, len(execution.Chunks))
	for i, chunk := range execution.Chunks {
		rows = append(rows, ClientResultChunkSummaryRow{
			RequestID:       execution.RequestID,
			Chunk:           i,
			Sequence:        chunk.Sequence,
			Rows:            countNonNilResultRows(chunk.Rows),
			Final:           chunk.Final,
			ResultStatus:    execution.Status,
			ResultKind:      execution.Kind,
			RowsReturned:    execution.RowsReturned,
			Cursor:          execution.Cursor.ID,
			CursorState:     execution.Cursor.State,
			DiagnosticCodes: execution.Diagnostics.Codes(),
		})
	}
	return rows
}

func countNonNilResultRows(rows []ResultRow) int {
	count := 0
	for _, row := range rows {
		if row != nil {
			count++
		}
	}
	return count
}
