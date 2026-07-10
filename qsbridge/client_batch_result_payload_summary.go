package qsbridge

// ClientBatchResultPayloadSummaryRow describes observed batch item payload shape.
type ClientBatchResultPayloadSummaryRow struct {
	Item         int
	BatchID      ExecutionRequestID
	RequestID    ExecutionRequestID
	Status       ExecutionStatus
	Ordinal      int
	ColumnName   string
	LogicalType  DataType
	Chunks       int
	Cells        int
	MissingCells int
	NullCells    int
	ValueKinds   []ValueKind
}

// ClientBatchResultPayloadSummaryExchange is adapter-facing batch payload shape metadata.
type ClientBatchResultPayloadSummaryExchange struct {
	Connection   ConnectionContext
	Batch        BatchExecutionResult
	Rows         []ClientBatchResultPayloadSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientBatchResultPayloadSummary returns payload shape rows for every batch item.
func (s PlanningService) ListClientBatchResultPayloadSummary(connection ConnectionContext, batch BatchExecutionResult) ClientBatchResultPayloadSummaryExchange {
	_ = s
	exchange := ClientBatchResultPayloadSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Batch:       cloneBatchExecutionResult(batch),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, batch.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = batchResultPayloadSummaryRows(batch)
	}
	exchange.Result = exchange.batchResultPayloadSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch result payload summary metadata can be returned.
func (e ClientBatchResultPayloadSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch result payload summary diagnostics into protocol-facing errors.
func (e ClientBatchResultPayloadSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch result payload summary error, if any.
func (e ClientBatchResultPayloadSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientBatchResultPayloadSummaryExchange) batchResultPayloadSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchResultPayloadSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.batchResultPayloadSummaryResultRows(),
		Final: true,
	})
}

func batchResultPayloadSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Item", Type: DataTypeInt},
		{Name: "Batch_id", Type: DataTypeString, Nullable: true},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "Column_name", Type: DataTypeString, Nullable: true},
		{Name: "Logical_type", Type: DataTypeString, Nullable: true},
		{Name: "Chunks", Type: DataTypeInt},
		{Name: "Cells", Type: DataTypeInt},
		{Name: "Missing_cells", Type: DataTypeInt},
		{Name: "Null_cells", Type: DataTypeInt},
		{Name: "Value_kinds", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientBatchResultPayloadSummaryExchange) batchResultPayloadSummaryResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Item),
			metadataStringCell(string(row.BatchID)),
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(string(row.Status)),
			metadataIntCell(row.Ordinal),
			metadataStringCell(row.ColumnName),
			metadataStringCell(string(row.LogicalType)),
			metadataIntCell(row.Chunks),
			metadataIntCell(row.Cells),
			metadataIntCell(row.MissingCells),
			metadataIntCell(row.NullCells),
			metadataStringCell(joinValueKinds(row.ValueKinds)),
		})
	}
	return rows
}

func batchResultPayloadSummaryRows(batch BatchExecutionResult) []ClientBatchResultPayloadSummaryRow {
	if len(batch.Items) == 0 {
		return nil
	}
	rows := make([]ClientBatchResultPayloadSummaryRow, 0, len(batch.Items))
	for itemIndex, item := range batch.Items {
		for _, payload := range resultPayloadSummaryRows(item) {
			rows = append(rows, ClientBatchResultPayloadSummaryRow{
				Item:         itemIndex,
				BatchID:      batch.RequestID,
				RequestID:    payload.RequestID,
				Status:       item.Status,
				Ordinal:      payload.Ordinal,
				ColumnName:   payload.ColumnName,
				LogicalType:  payload.LogicalType,
				Chunks:       payload.Chunks,
				Cells:        payload.Cells,
				MissingCells: payload.MissingCells,
				NullCells:    payload.NullCells,
				ValueKinds:   append([]ValueKind(nil), payload.ValueKinds...),
			})
		}
	}
	return rows
}
