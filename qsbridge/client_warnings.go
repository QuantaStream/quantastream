package qsbridge

// ClientWarningsExchange is adapter-facing metadata for statement warning detail.
type ClientWarningsExchange struct {
	Connection   ConnectionContext
	Response     ProtocolStatementResponse
	Notices      []StatementNotice
	WarningCount uint16
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ClientWarningsSummaryRow describes aggregate statement warning metadata.
type ClientWarningsSummaryRow struct {
	WarningCount int
	NoticeCount  int
	WarningRows  int
	NoteRows     int
	ErrorRows    int
	CodedRows    int
	SQLStateRows int
}

// ListClientStatementWarnings returns SHOW WARNINGS-style metadata for a statement response.
func (s PlanningService) ListClientStatementWarnings(connection ConnectionContext, response ProtocolStatementResponse) ClientWarningsExchange {
	_ = s
	exchange := ClientWarningsExchange{
		Connection:   cloneConnectionContext(connection),
		Response:     cloneProtocolStatementResponse(response),
		Notices:      cloneStatementNotices(response.Notices),
		WarningCount: response.Warnings,
		Diagnostics:  cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.warningListResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Result = exchange.warningListResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether warning-list metadata can be returned.
func (e ClientWarningsExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts warning-list diagnostics into protocol-facing errors.
func (e ClientWarningsExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking warning-list error, if any.
func (e ClientWarningsExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientWarningsExchange) warningListResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     warningListResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.warningListRows(),
		Final: true,
	})
}

func warningListResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Level", Type: DataTypeString},
		{Name: "Code", Type: DataTypeString, Nullable: true},
		{Name: "SQLState", Type: DataTypeString, Nullable: true},
		{Name: "Message", Type: DataTypeString},
	}
}

func (e ClientWarningsExchange) warningListRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Notices))
	for _, notice := range e.Notices {
		rows = append(rows, ResultRow{
			metadataStringCell(statementNoticeLevelLabel(notice.Level)),
			metadataStringCell(notice.Code),
			metadataStringCell(notice.SQLState),
			metadataStringCell(notice.Message),
		})
	}
	return rows
}

func summarizeStatementNotices(notices []StatementNotice, warningCount uint16) ClientWarningsSummaryRow {
	summary := ClientWarningsSummaryRow{
		WarningCount: int(warningCount),
		NoticeCount:  len(notices),
	}
	for _, notice := range notices {
		switch notice.Level {
		case StatementNoticeNote:
			summary.NoteRows++
		case StatementNoticeError:
			summary.ErrorRows++
		default:
			summary.WarningRows++
		}
		if notice.Code != "" {
			summary.CodedRows++
		}
		if notice.SQLState != "" {
			summary.SQLStateRows++
		}
	}
	return summary
}

func statementNoticeLevelLabel(level StatementNoticeLevel) string {
	switch level {
	case StatementNoticeNote, StatementNoticeError, StatementNoticeWarning:
		return string(level)
	default:
		return string(StatementNoticeWarning)
	}
}
