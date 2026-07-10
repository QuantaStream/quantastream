package qsbridge

// ClientPlanInspectionSummaryRow describes one explain/profile planning exchange.
type ClientPlanInspectionSummaryRow struct {
	Ordinal         int
	RequestID       ExecutionRequestID
	Kind            QueryKind
	Supported       bool
	Explain         bool
	Profile         bool
	LogicalNodes    int
	PhysicalNodes   int
	Capabilities    int
	Parameters      int
	ResultColumns   int
	Timings         int
	Counters        int
	SQLLength       int
	DiagnosticCodes []DiagnosticCode
}

// ClientPlanInspectionSummaryExchange is adapter-facing inspection summary metadata.
type ClientPlanInspectionSummaryExchange struct {
	Connection          ConnectionContext
	Inspection          ClientPlanInspectionExchange
	Rows                []ClientPlanInspectionSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientPlanInspection returns compact row metadata for an inspection exchange.
func (s PlanningService) SummarizeClientPlanInspection(inspection ClientPlanInspectionExchange) ClientPlanInspectionSummaryExchange {
	_ = s
	exchange := ClientPlanInspectionSummaryExchange{
		Connection:          cloneConnectionContext(inspection.Connection),
		Inspection:          cloneClientPlanInspectionExchange(inspection),
		ExchangeDiagnostics: cloneDiagnosticSet(inspection.Connection.Diagnostics),
	}
	if inspection.Connection.Supported() {
		exchange.Rows = []ClientPlanInspectionSummaryRow{clientPlanInspectionSummaryRow(inspection)}
	}
	exchange.Result = exchange.clientPlanInspectionSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(inspection.Connection.Protocol)
	return exchange
}

// Supported reports whether inspection summary metadata can be returned.
func (e ClientPlanInspectionSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientPlanInspectionSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientPlanInspectionSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientPlanInspectionSummaryExchange) clientPlanInspectionSummaryResult() ExecutionResult {
	result := ExecutionResult{
		RequestID:   e.inspectionRequestID(),
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     clientPlanInspectionSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.clientPlanInspectionSummaryRows(),
		Final: true,
	})
}

func (e ClientPlanInspectionSummaryExchange) inspectionRequestID() ExecutionRequestID {
	if len(e.Rows) == 0 {
		return ""
	}
	return e.Rows[0].RequestID
}

func clientPlanInspectionSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Explain", Type: DataTypeBool},
		{Name: "Profile", Type: DataTypeBool},
		{Name: "Logical_nodes", Type: DataTypeInt},
		{Name: "Physical_nodes", Type: DataTypeInt},
		{Name: "Capabilities", Type: DataTypeInt},
		{Name: "Parameters", Type: DataTypeInt},
		{Name: "Result_columns", Type: DataTypeInt},
		{Name: "Timings", Type: DataTypeInt},
		{Name: "Counters", Type: DataTypeInt},
		{Name: "SQL_length", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPlanInspectionSummaryExchange) clientPlanInspectionSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Ordinal),
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(string(row.Kind)),
			metadataBoolCell(row.Supported),
			metadataBoolCell(row.Explain),
			metadataBoolCell(row.Profile),
			metadataIntCell(row.LogicalNodes),
			metadataIntCell(row.PhysicalNodes),
			metadataIntCell(row.Capabilities),
			metadataIntCell(row.Parameters),
			metadataIntCell(row.ResultColumns),
			metadataIntCell(row.Timings),
			metadataIntCell(row.Counters),
			metadataIntCell(row.SQLLength),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func clientPlanInspectionSummaryRow(inspection ClientPlanInspectionExchange) ClientPlanInspectionSummaryRow {
	return ClientPlanInspectionSummaryRow{
		Ordinal:         inspection.Statement.Ordinal,
		RequestID:       inspection.Profile.RequestID,
		Kind:            inspection.Prepared.Kind,
		Supported:       inspection.Supported(),
		Explain:         inspection.Profile.TraceExplain,
		Profile:         inspection.Profile.IncludeProfile,
		LogicalNodes:    len(inspection.Inspection.Logical.Nodes),
		PhysicalNodes:   len(inspection.Inspection.Physical.Nodes),
		Capabilities:    len(inspection.Inspection.Capabilities),
		Parameters:      len(inspection.Prepared.Parameters),
		ResultColumns:   len(inspection.Prepared.ResultColumns),
		Timings:         len(inspection.Profile.Timings),
		Counters:        len(inspection.Profile.Counters),
		SQLLength:       len(inspection.Request.SQL),
		DiagnosticCodes: inspection.Diagnostics.Codes(),
	}
}

func cloneClientPlanInspectionExchange(exchange ClientPlanInspectionExchange) ClientPlanInspectionExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Prepared = clonePreparedPlan(exchange.Prepared)
	exchange.Inspection = cloneInspectionReport(exchange.Inspection)
	exchange.Profile = cloneExecutionProfile(exchange.Profile)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}
