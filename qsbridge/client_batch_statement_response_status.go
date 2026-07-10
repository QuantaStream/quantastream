package qsbridge

// ClientBatchStatementResponseStatusRow describes one batch item OK/status response.
type ClientBatchStatementResponseStatusRow struct {
	Item           int
	RequestID      ExecutionRequestID
	AffectedRows   uint64
	LastInsertID   uint64
	Warnings       uint16
	Status         string
	SessionActions int
	Transaction    bool
	Flags          []ProtocolStatusFlag
	Diagnostics    []DiagnosticCode
}

// ClientBatchStatementResponseStatusSummaryRow describes aggregate batch OK/status metadata.
type ClientBatchStatementResponseStatusSummaryRow struct {
	ItemCount                int
	TotalAffectedRows        uint64
	ItemsWithAffectedRows    int
	ItemsWithLastInsertID    int
	TotalWarnings            int
	ItemsWithWarnings        int
	TotalSessionActions      int
	ItemsWithSessionActions  int
	TransactionItemCount     int
	ItemsWithDiagnostics     int
	RowsAffectedFlagCount    int
	LastInsertIDFlagCount    int
	WarningsFlagCount        int
	SessionStateChangedCount int
	TransactionFlagCount     int
}

// ClientBatchStatementResponseStatusExchange is adapter-facing batch OK/status metadata.
type ClientBatchStatementResponseStatusExchange struct {
	Connection   ConnectionContext
	Batch        BatchExecutionResult
	Rows         []ClientBatchStatementResponseStatusRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientBatchStatementResponseStatus returns OK/status metadata for batch item results.
func (s PlanningService) ListClientBatchStatementResponseStatus(connection ConnectionContext, batch BatchExecutionResult) ClientBatchStatementResponseStatusExchange {
	_ = s
	exchange := ClientBatchStatementResponseStatusExchange{
		Connection:  cloneConnectionContext(connection),
		Batch:       cloneBatchExecutionResult(batch),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, batch.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows, exchange.Diagnostics = batchStatementResponseStatusRows(connection.Protocol, batch, exchange.Diagnostics)
	}
	exchange.Result = exchange.batchStatementResponseStatusResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch statement response status metadata can be returned.
func (e ClientBatchStatementResponseStatusExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch statement response status diagnostics into protocol-facing errors.
func (e ClientBatchStatementResponseStatusExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch statement response status error, if any.
func (e ClientBatchStatementResponseStatusExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientBatchStatementResponseStatusExchange) batchStatementResponseStatusResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchStatementResponseStatusResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.batchStatementResponseStatusRows(),
		Final: true,
	})
}

func batchStatementResponseStatusResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Item", Type: DataTypeInt},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Affected_rows", Type: DataTypeInt},
		{Name: "Last_insert_id", Type: DataTypeInt},
		{Name: "Warnings", Type: DataTypeInt},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Session_actions", Type: DataTypeInt},
		{Name: "Transaction", Type: DataTypeBool},
		{Name: "Flags", Type: DataTypeString, Nullable: true},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientBatchStatementResponseStatusExchange) batchStatementResponseStatusRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Item),
			metadataStringCell(string(row.RequestID)),
			metadataIntCell(int(row.AffectedRows)),
			metadataIntCell(int(row.LastInsertID)),
			metadataIntCell(int(row.Warnings)),
			metadataStringCell(row.Status),
			metadataIntCell(row.SessionActions),
			metadataBoolCell(row.Transaction),
			metadataStringCell(joinProtocolStatusFlags(row.Flags)),
			metadataStringCell(joinDiagnosticCodes(row.Diagnostics)),
		})
	}
	return rows
}

func batchStatementResponseStatusRows(profile ProtocolProfile, batch BatchExecutionResult, diagnostics DiagnosticSet) ([]ClientBatchStatementResponseStatusRow, DiagnosticSet) {
	if len(batch.Items) == 0 {
		return nil, diagnostics
	}
	rows := make([]ClientBatchStatementResponseStatusRow, 0, len(batch.Items))
	for itemIndex, item := range batch.Items {
		response := item.ProtocolStatementResponse(profile)
		diagnostics = mergeDiagnosticSets(diagnostics, response.Diagnostics)
		status := statementResponseStatus(response)
		rows = append(rows, ClientBatchStatementResponseStatusRow{
			Item:           itemIndex,
			RequestID:      item.RequestID,
			AffectedRows:   status.AffectedRows,
			LastInsertID:   status.LastInsertID,
			Warnings:       status.Warnings,
			Status:         status.Status,
			SessionActions: status.SessionActions,
			Transaction:    status.Transaction,
			Flags:          append([]ProtocolStatusFlag(nil), status.Flags...),
			Diagnostics:    append([]DiagnosticCode(nil), status.Diagnostics...),
		})
	}
	return rows, diagnostics
}

func summarizeBatchStatementResponseStatusRows(rows []ClientBatchStatementResponseStatusRow) ClientBatchStatementResponseStatusSummaryRow {
	summary := ClientBatchStatementResponseStatusSummaryRow{ItemCount: len(rows)}
	for _, row := range rows {
		summary.TotalAffectedRows += row.AffectedRows
		if row.AffectedRows > 0 {
			summary.ItemsWithAffectedRows++
		}
		if row.LastInsertID > 0 {
			summary.ItemsWithLastInsertID++
		}
		summary.TotalWarnings += int(row.Warnings)
		if row.Warnings > 0 {
			summary.ItemsWithWarnings++
		}
		summary.TotalSessionActions += row.SessionActions
		if row.SessionActions > 0 {
			summary.ItemsWithSessionActions++
		}
		if row.Transaction {
			summary.TransactionItemCount++
		}
		if len(row.Diagnostics) > 0 {
			summary.ItemsWithDiagnostics++
		}
		if batchProtocolStatusFlagsContain(row.Flags, ProtocolStatusRowsAffected) {
			summary.RowsAffectedFlagCount++
		}
		if batchProtocolStatusFlagsContain(row.Flags, ProtocolStatusLastInsertID) {
			summary.LastInsertIDFlagCount++
		}
		if batchProtocolStatusFlagsContain(row.Flags, ProtocolStatusWarnings) {
			summary.WarningsFlagCount++
		}
		if batchProtocolStatusFlagsContain(row.Flags, ProtocolStatusSessionStateChanged) {
			summary.SessionStateChangedCount++
		}
		if batchProtocolStatusFlagsContain(row.Flags, ProtocolStatusTransactionAction) {
			summary.TransactionFlagCount++
		}
	}
	return summary
}

func batchProtocolStatusFlagsContain(flags []ProtocolStatusFlag, want ProtocolStatusFlag) bool {
	for _, flag := range flags {
		if flag == want {
			return true
		}
	}
	return false
}
