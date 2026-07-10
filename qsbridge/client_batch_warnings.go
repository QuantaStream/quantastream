package qsbridge

// ClientBatchWarningRow describes one batch item warning or note.
type ClientBatchWarningRow struct {
	Item      int
	RequestID ExecutionRequestID
	Level     StatementNoticeLevel
	Code      string
	SQLState  string
	Message   string
}

// ClientBatchWarningsSummaryRow describes aggregate batch warning metadata.
type ClientBatchWarningsSummaryRow struct {
	WarningCount       int
	DetailRowCount     int
	WarningRows        int
	NoteRows           int
	ErrorRows          int
	CodedRows          int
	SQLStateRows       int
	ItemsWithDetails   int
	DistinctRequestIDs int
}

// ClientBatchWarningsExchange is adapter-facing metadata for batch warning detail.
type ClientBatchWarningsExchange struct {
	Connection   ConnectionContext
	Batch        BatchExecutionResult
	Rows         []ClientBatchWarningRow
	WarningCount uint16
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientBatchWarnings returns SHOW WARNINGS-style metadata for batch item results.
func (s PlanningService) ListClientBatchWarnings(connection ConnectionContext, batch BatchExecutionResult) ClientBatchWarningsExchange {
	_ = s
	exchange := ClientBatchWarningsExchange{
		Connection:   cloneConnectionContext(connection),
		Batch:        cloneBatchExecutionResult(batch),
		WarningCount: batchWarningCount(batch),
		Diagnostics:  mergeDiagnosticSets(connection.Diagnostics, batch.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = batchWarningRows(batch)
	}
	exchange.Result = exchange.batchWarningListResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch warning-list metadata can be returned.
func (e ClientBatchWarningsExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch warning-list diagnostics into protocol-facing errors.
func (e ClientBatchWarningsExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch warning-list error, if any.
func (e ClientBatchWarningsExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientBatchWarningsExchange) batchWarningListResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchWarningListResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.batchWarningListRows(),
		Final: true,
	})
}

func batchWarningListResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Item", Type: DataTypeInt},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Level", Type: DataTypeString},
		{Name: "Code", Type: DataTypeString, Nullable: true},
		{Name: "SQLState", Type: DataTypeString, Nullable: true},
		{Name: "Message", Type: DataTypeString},
	}
}

func (e ClientBatchWarningsExchange) batchWarningListRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Item),
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(statementNoticeLevelLabel(row.Level)),
			metadataStringCell(row.Code),
			metadataStringCell(row.SQLState),
			metadataStringCell(row.Message),
		})
	}
	return rows
}

func batchWarningRows(batch BatchExecutionResult) []ClientBatchWarningRow {
	if len(batch.Items) == 0 {
		return nil
	}
	rows := make([]ClientBatchWarningRow, 0)
	for itemIndex, item := range batch.Items {
		for _, notice := range item.Statement.Notices {
			rows = append(rows, ClientBatchWarningRow{
				Item:      itemIndex,
				RequestID: item.RequestID,
				Level:     notice.Level,
				Code:      notice.Code,
				SQLState:  notice.SQLState,
				Message:   notice.Message,
			})
		}
	}
	return rows
}

func batchWarningCount(batch BatchExecutionResult) uint16 {
	count := 0
	for _, item := range batch.Items {
		count += int(statementWarningCount(item.Statement))
		if count > int(^uint16(0)) {
			return ^uint16(0)
		}
	}
	return uint16(count)
}

func summarizeBatchWarningRows(rows []ClientBatchWarningRow, warningCount uint16) ClientBatchWarningsSummaryRow {
	summary := ClientBatchWarningsSummaryRow{
		WarningCount:   int(warningCount),
		DetailRowCount: len(rows),
	}
	items := make(map[int]struct{})
	requests := make(map[ExecutionRequestID]struct{})
	for _, row := range rows {
		items[row.Item] = struct{}{}
		if row.RequestID != "" {
			requests[row.RequestID] = struct{}{}
		}
		switch row.Level {
		case StatementNoticeNote:
			summary.NoteRows++
		case StatementNoticeError:
			summary.ErrorRows++
		default:
			summary.WarningRows++
		}
		if row.Code != "" {
			summary.CodedRows++
		}
		if row.SQLState != "" {
			summary.SQLStateRows++
		}
	}
	summary.ItemsWithDetails = len(items)
	summary.DistinctRequestIDs = len(requests)
	return summary
}
