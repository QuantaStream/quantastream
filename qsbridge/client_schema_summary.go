package qsbridge

// ClientSchemaSelectionSummaryRow describes one current-schema selection exchange.
type ClientSchemaSelectionSummaryRow struct {
	RequestedSchema string
	PreviousSchema  string
	NextSchema      string
	Applied         bool
	SessionActions  int
	Status          string
	Supported       bool
	DiagnosticCodes []DiagnosticCode
}

// ClientSchemaSelectionSummaryExchange is adapter-facing schema selection metadata.
type ClientSchemaSelectionSummaryExchange struct {
	Connection          ConnectionContext
	Selection           ClientSchemaSelectionExchange
	Rows                []ClientSchemaSelectionSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientUseSchema returns row metadata for one current-schema selection exchange.
func (s PlanningService) SummarizeClientUseSchema(connection ConnectionContext, selection ClientSchemaSelectionExchange) ClientSchemaSelectionSummaryExchange {
	_ = s
	exchange := ClientSchemaSelectionSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Selection:           cloneClientSchemaSelectionExchange(selection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientSchemaSelectionSummaryRow{schemaSelectionSummaryRow(selection)}
	}
	exchange.Result = exchange.schemaSelectionSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether schema selection summary metadata can be returned.
func (e ClientSchemaSelectionSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientSchemaSelectionSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientSchemaSelectionSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientSchemaSelectionSummaryExchange) schemaSelectionSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     schemaSelectionSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.schemaSelectionSummaryRows(),
		Final: true,
	})
}

func schemaSelectionSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Requested_schema", Type: DataTypeString, Nullable: true},
		{Name: "Previous_schema", Type: DataTypeString, Nullable: true},
		{Name: "Next_schema", Type: DataTypeString, Nullable: true},
		{Name: "Applied", Type: DataTypeBool},
		{Name: "Session_actions", Type: DataTypeInt},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientSchemaSelectionSummaryExchange) schemaSelectionSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(row.RequestedSchema),
			metadataStringCell(row.PreviousSchema),
			metadataStringCell(row.NextSchema),
			metadataBoolCell(row.Applied),
			metadataIntCell(row.SessionActions),
			metadataStringCell(row.Status),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func schemaSelectionSummaryRow(selection ClientSchemaSelectionExchange) ClientSchemaSelectionSummaryRow {
	return ClientSchemaSelectionSummaryRow{
		RequestedSchema: selection.Schema,
		PreviousSchema:  selection.Session.Transition.Before.CurrentSchema,
		NextSchema:      selection.Session.Transition.After.CurrentSchema,
		Applied:         selection.Session.Applied,
		SessionActions:  len(selection.Response.SessionActions),
		Status:          selection.Response.Status,
		Supported:       selection.Supported(),
		DiagnosticCodes: selection.Diagnostics.Codes(),
	}
}

func cloneClientSchemaSelectionExchange(exchange ClientSchemaSelectionExchange) ClientSchemaSelectionExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Session = cloneClientSessionActionExchange(exchange.Session)
	exchange.Response = cloneProtocolStatementResponse(exchange.Response)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}
