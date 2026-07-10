package qsbridge

// ClientParsePreviewSummaryExchange is adapter-facing metadata for aggregate parser preview shape.
type ClientParsePreviewSummaryExchange struct {
	Connection   ConnectionContext
	Statements   []ClientParseStatement
	Row          ClientParsePreviewSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientParsePreview parses client statements and returns aggregate parser-preview shape metadata.
func (s PlanningService) SummarizeClientParsePreview(bundle ClientStatementBundle) ClientParsePreviewSummaryExchange {
	preview := s.PreviewClientParse(bundle)
	exchange := ClientParsePreviewSummaryExchange{
		Connection:  cloneConnectionContext(preview.Connection),
		Statements:  cloneClientParseStatements(preview.Statements),
		Diagnostics: cloneDiagnosticSet(preview.Diagnostics),
	}
	exchange.Row = summarizeClientParsePreview(exchange.Statements)
	exchange.Result = exchange.parsePreviewSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(bundle.Connection.Protocol)
	return exchange
}

// Supported reports whether every statement parsed without blocking diagnostics.
func (e ClientParsePreviewSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts parse preview summary diagnostics into protocol-facing errors.
func (e ClientParsePreviewSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking parse preview summary error, if any.
func (e ClientParsePreviewSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientParsePreviewSummaryExchange) parsePreviewSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     parsePreviewSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{parsePreviewSummaryResultRow(e.Row)},
		Final: true,
	})
}

func parsePreviewSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Statement_count", Type: DataTypeInt},
		{Name: "Select_count", Type: DataTypeInt},
		{Name: "Insert_count", Type: DataTypeInt},
		{Name: "Update_count", Type: DataTypeInt},
		{Name: "Delete_count", Type: DataTypeInt},
		{Name: "Session_count", Type: DataTypeInt},
		{Name: "Table_count", Type: DataTypeInt},
		{Name: "Projection_count", Type: DataTypeInt},
		{Name: "Join_count", Type: DataTypeInt},
		{Name: "Membership_count", Type: DataTypeInt},
		{Name: "Predicate_count", Type: DataTypeInt},
		{Name: "Group_by_count", Type: DataTypeInt},
		{Name: "Aggregate_count", Type: DataTypeInt},
		{Name: "Having_count", Type: DataTypeInt},
		{Name: "Order_by_count", Type: DataTypeInt},
		{Name: "Blocker_count", Type: DataTypeInt},
		{Name: "Diagnostic_count", Type: DataTypeInt},
	}
}

func parsePreviewSummaryResultRow(row ClientParsePreviewSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.StatementCount),
		metadataIntCell(row.SelectCount),
		metadataIntCell(row.InsertCount),
		metadataIntCell(row.UpdateCount),
		metadataIntCell(row.DeleteCount),
		metadataIntCell(row.SessionCount),
		metadataIntCell(row.TableCount),
		metadataIntCell(row.ProjectionCount),
		metadataIntCell(row.JoinCount),
		metadataIntCell(row.MembershipCount),
		metadataIntCell(row.PredicateCount),
		metadataIntCell(row.GroupByCount),
		metadataIntCell(row.AggregateCount),
		metadataIntCell(row.HavingCount),
		metadataIntCell(row.OrderByCount),
		metadataIntCell(row.BlockerCount),
		metadataIntCell(row.DiagnosticCount),
	}
}
