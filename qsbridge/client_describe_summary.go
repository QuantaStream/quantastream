package qsbridge

// ClientDescribeSummaryRow describes one SQL or prepared-handle describe exchange.
type ClientDescribeSummaryRow struct {
	Source               string
	StatementID          PreparedStatementID
	StatementName        string
	Kind                 QueryKind
	AccessIntent         PhysicalAccessIntent
	Lifecycle            ClientPlanLifecycleKind
	LifecycleSteps       int
	Supported            bool
	Parameters           int
	ResultColumns        int
	HasResultSchema      bool
	HasStatementResponse bool
	StatementStatus      string
	SQLLength            int
	DiagnosticCodes      []DiagnosticCode
}

// ClientDescribeSummaryExchange is adapter-facing describe outcome metadata.
type ClientDescribeSummaryExchange struct {
	Connection          ConnectionContext
	Describe            ClientDescribeExchange
	Rows                []ClientDescribeSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientDescribe returns row metadata for one describe exchange.
func (s PlanningService) SummarizeClientDescribe(describe ClientDescribeExchange) ClientDescribeSummaryExchange {
	_ = s
	exchange := ClientDescribeSummaryExchange{
		Connection:          cloneConnectionContext(describe.Connection),
		Describe:            cloneClientDescribeExchange(describe),
		ExchangeDiagnostics: cloneDiagnosticSet(describe.Connection.Diagnostics),
	}
	if describe.Connection.Supported() {
		exchange.Rows = []ClientDescribeSummaryRow{clientDescribeSummaryRow(describe)}
	}
	exchange.Result = exchange.clientDescribeSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(describe.Connection.Protocol)
	return exchange
}

// Supported reports whether describe summary metadata can be returned.
func (e ClientDescribeSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientDescribeSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientDescribeSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientDescribeSummaryExchange) clientDescribeSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     clientDescribeSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.clientDescribeSummaryRows(),
		Final: true,
	})
}

func clientDescribeSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Source", Type: DataTypeString},
		{Name: "Statement_id", Type: DataTypeInt},
		{Name: "Statement_name", Type: DataTypeString, Nullable: true},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Parameters", Type: DataTypeInt},
		{Name: "Result_columns", Type: DataTypeInt},
		{Name: "Has_result_schema", Type: DataTypeBool},
		{Name: "Has_statement_response", Type: DataTypeBool},
		{Name: "Statement_status", Type: DataTypeString, Nullable: true},
		{Name: "SQL_length", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientDescribeSummaryExchange) clientDescribeSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(row.Source),
			metadataIntCell(int(row.StatementID)),
			metadataStringCell(row.StatementName),
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataBoolCell(row.Supported),
			metadataIntCell(row.Parameters),
			metadataIntCell(row.ResultColumns),
			metadataBoolCell(row.HasResultSchema),
			metadataBoolCell(row.HasStatementResponse),
			metadataStringCell(row.StatementStatus),
			metadataIntCell(row.SQLLength),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func clientDescribeSummaryRow(describe ClientDescribeExchange) ClientDescribeSummaryRow {
	source := "sql"
	if describe.Handle.ID != 0 || describe.Handle.Name != "" {
		source = "prepared"
	}
	return ClientDescribeSummaryRow{
		Source:               source,
		StatementID:          describe.Description.Handle.ID,
		StatementName:        describe.Description.Handle.Name,
		Kind:                 describe.Description.Kind,
		AccessIntent:         describe.Description.AccessIntent,
		Lifecycle:            clientPlanLifecycleKind(describe.Description.Kind),
		LifecycleSteps:       clientPlanLifecycleStepCount(describe.Description.Kind),
		Supported:            describe.Supported(),
		Parameters:           len(describe.Description.Parameters),
		ResultColumns:        len(describe.Description.ResultColumns),
		HasResultSchema:      describe.HasResultSchema,
		HasStatementResponse: describe.HasStatementResponse,
		StatementStatus:      describe.StatementResponse.Status,
		SQLLength:            len(describe.SQL),
		DiagnosticCodes:      describe.Diagnostics.Codes(),
	}
}

func cloneClientDescribeExchange(exchange ClientDescribeExchange) ClientDescribeExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Description = clonePreparedPlanDescription(exchange.Description)
	exchange.ResultSchema = cloneProtocolResultSchema(exchange.ResultSchema)
	exchange.StatementResponse = cloneProtocolStatementResponse(exchange.StatementResponse)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}
