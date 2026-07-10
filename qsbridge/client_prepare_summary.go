package qsbridge

// ClientPrepareSummaryRow describes one prepared-statement prepare exchange.
type ClientPrepareSummaryRow struct {
	StatementID     PreparedStatementID
	StatementName   string
	Kind            QueryKind
	AccessIntent    PhysicalAccessIntent
	Lifecycle       ClientPlanLifecycleKind
	LifecycleSteps  int
	Registered      bool
	Supported       bool
	Parameters      int
	ResultColumns   int
	SQLLength       int
	DiagnosticCodes []DiagnosticCode
}

// ClientPrepareSummaryExchange is adapter-facing prepare outcome metadata.
type ClientPrepareSummaryExchange struct {
	Connection          ConnectionContext
	Prepare             ClientPrepareExchange
	Rows                []ClientPrepareSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientPrepare returns row metadata for one prepared-statement prepare exchange.
func (s PlanningService) SummarizeClientPrepare(connection ConnectionContext, prepare ClientPrepareExchange) ClientPrepareSummaryExchange {
	_ = s
	exchange := ClientPrepareSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Prepare:             cloneClientPrepareExchange(prepare),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientPrepareSummaryRow{prepareSummaryRow(prepare)}
	}
	exchange.Result = exchange.prepareSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepare summary metadata can be returned.
func (e ClientPrepareSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientPrepareSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientPrepareSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientPrepareSummaryExchange) prepareSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     prepareSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.prepareSummaryRows(),
		Final: true,
	})
}

func prepareSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Statement_id", Type: DataTypeInt},
		{Name: "Statement_name", Type: DataTypeString, Nullable: true},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Registered", Type: DataTypeBool},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Parameters", Type: DataTypeInt},
		{Name: "Result_columns", Type: DataTypeInt},
		{Name: "SQL_length", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPrepareSummaryExchange) prepareSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(int(row.StatementID)),
			metadataStringCell(row.StatementName),
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataBoolCell(row.Registered),
			metadataBoolCell(row.Supported),
			metadataIntCell(row.Parameters),
			metadataIntCell(row.ResultColumns),
			metadataIntCell(row.SQLLength),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func prepareSummaryRow(prepare ClientPrepareExchange) ClientPrepareSummaryRow {
	return ClientPrepareSummaryRow{
		StatementID:     prepare.Description.Handle.ID,
		StatementName:   prepare.Description.Handle.Name,
		Kind:            prepare.Description.Kind,
		AccessIntent:    prepare.Description.AccessIntent,
		Lifecycle:       clientPlanLifecycleKind(prepare.Description.Kind),
		LifecycleSteps:  clientPlanLifecycleStepCount(prepare.Description.Kind),
		Registered:      prepare.Registered,
		Supported:       prepare.Supported(),
		Parameters:      len(prepare.Description.Parameters),
		ResultColumns:   len(prepare.Description.ResultColumns),
		SQLLength:       len(prepare.Request.SQL),
		DiagnosticCodes: prepare.Diagnostics.Codes(),
	}
}

func cloneClientPrepareExchange(exchange ClientPrepareExchange) ClientPrepareExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Request = cloneClientPrepareRequest(exchange.Request)
	exchange.Prepared = clonePreparedPlan(exchange.Prepared)
	exchange.Description = clonePreparedPlanDescription(exchange.Description)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}
