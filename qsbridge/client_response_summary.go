package qsbridge

// ClientResponseSummaryRow describes one response item as adapter-visible metadata.
type ClientResponseSummaryRow struct {
	Ordinal         int
	Kind            ClientResponseKind
	Outcome         ExecutionHandoffKind
	AccessIntent    PhysicalAccessIntent
	Lifecycle       ClientPlanLifecycleKind
	LifecycleSteps  int
	Status          ExecutionStatus
	Complete        bool
	MoreResults     bool
	Final           bool
	SchemaColumns   int
	RowsReturned    uint64
	AffectedRows    uint64
	Warnings        uint16
	ErrorCount      int
	Flags           []ClientResponseFlag
	StatementStatus string
	SQL             string
}

// ClientResponseSummaryExchange is adapter-facing response sequence metadata.
type ClientResponseSummaryExchange struct {
	Connection   ConnectionContext
	Rows         []ClientResponseSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientResponseSummary returns compact metadata for an ordered response sequence.
func (s PlanningService) ListClientResponseSummary(sequence ClientResponseSequence) ClientResponseSummaryExchange {
	_ = s
	exchange := ClientResponseSummaryExchange{
		Connection:  cloneConnectionContext(sequence.Connection),
		Diagnostics: cloneDiagnosticSet(sequence.Diagnostics),
	}
	if sequence.Connection.Supported() {
		exchange.Rows = responseSummaryRows(sequence.Items)
	}
	exchange.Result = exchange.responseSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(sequence.Connection.Protocol)
	return exchange
}

// Supported reports whether response summary metadata can be returned.
func (e ClientResponseSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts response summary diagnostics into protocol-facing errors.
func (e ClientResponseSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking response summary error, if any.
func (e ClientResponseSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientResponseSummaryExchange) responseSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     responseSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.responseSummaryResultRows(),
		Final: true,
	})
}

func responseSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "Kind", Type: DataTypeString},
		{Name: "Outcome", Type: DataTypeString, Nullable: true},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Complete", Type: DataTypeBool},
		{Name: "More_results", Type: DataTypeBool},
		{Name: "Final", Type: DataTypeBool},
		{Name: "Schema_columns", Type: DataTypeInt},
		{Name: "Rows_returned", Type: DataTypeInt},
		{Name: "Affected_rows", Type: DataTypeInt},
		{Name: "Warnings", Type: DataTypeInt},
		{Name: "Errors", Type: DataTypeInt},
		{Name: "Flags", Type: DataTypeString, Nullable: true},
		{Name: "Statement_status", Type: DataTypeString, Nullable: true},
		{Name: "SQL", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientResponseSummaryExchange) responseSummaryResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Ordinal),
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.Outcome)),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataStringCell(string(row.Status)),
			metadataBoolCell(row.Complete),
			metadataBoolCell(row.MoreResults),
			metadataBoolCell(row.Final),
			metadataIntCell(row.SchemaColumns),
			metadataIntCell(int(row.RowsReturned)),
			metadataIntCell(int(row.AffectedRows)),
			metadataIntCell(int(row.Warnings)),
			metadataIntCell(row.ErrorCount),
			metadataStringCell(joinClientResponseFlags(row.Flags)),
			metadataStringCell(row.StatementStatus),
			metadataStringCell(row.SQL),
		})
	}
	return rows
}

func responseSummaryRows(items []ClientResponseItem) []ClientResponseSummaryRow {
	if len(items) == 0 {
		return nil
	}
	rows := make([]ClientResponseSummaryRow, 0, len(items))
	for _, item := range items {
		rows = append(rows, ClientResponseSummaryRow{
			Ordinal:         item.Ordinal,
			Kind:            item.Kind,
			Outcome:         item.Outcome.Kind,
			AccessIntent:    item.Outcome.AccessIntent,
			Lifecycle:       item.Outcome.Lifecycle,
			LifecycleSteps:  item.Outcome.LifecycleSteps,
			Status:          item.Result.Status,
			Complete:        item.Result.Complete,
			MoreResults:     item.MoreResults,
			Final:           item.Final,
			SchemaColumns:   len(item.Schema.Columns),
			RowsReturned:    item.Result.RowsReturned,
			AffectedRows:    item.StatementResponse.AffectedRows,
			Warnings:        item.StatementResponse.Warnings,
			ErrorCount:      len(item.Errors),
			Flags:           append([]ClientResponseFlag(nil), item.Flags...),
			StatementStatus: item.StatementResponse.Status,
			SQL:             item.Statement.SQL,
		})
	}
	return rows
}

func joinClientResponseFlags(flags []ClientResponseFlag) string {
	values := make([]string, 0, len(flags))
	for _, flag := range flags {
		values = append(values, string(flag))
	}
	return joinStringValues(values)
}
