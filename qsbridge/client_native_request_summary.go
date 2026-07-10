package qsbridge

// ClientNativeRequestKind identifies single-statement or batch native metadata.
type ClientNativeRequestKind string

const (
	// ClientNativeRequestSingle identifies a single native execution descriptor.
	ClientNativeRequestSingle ClientNativeRequestKind = "single"
	// ClientNativeRequestBatch identifies a batch native execution descriptor.
	ClientNativeRequestBatch ClientNativeRequestKind = "batch"
)

// ClientNativeRequestSummaryRow describes one native executor handoff descriptor.
type ClientNativeRequestSummaryRow struct {
	Kind               ClientNativeRequestKind
	RequestID          ExecutionRequestID
	SQL                string
	Schema             string
	User               UserName
	Supported          bool
	AccessIntent       PhysicalAccessIntent
	Lifecycle          ClientPlanLifecycleKind
	LifecycleSteps     int
	ResultKind         ResultKind
	ResultColumns      int
	StatementActions   int
	AccessRequirements int
	MaxRows            int
	BatchSize          int
	Streaming          bool
	Cursor             CursorMode
	Cancelable         bool
	ParameterCount     int
	ParameterSetCount  int
	DiagnosticCodes    []DiagnosticCode
}

// ClientNativeRequestSummaryExchange is adapter-facing native request metadata.
type ClientNativeRequestSummaryExchange struct {
	Connection          ConnectionContext
	Rows                []ClientNativeRequestSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientNativeRequest returns row metadata for one single native execution request.
func (s PlanningService) SummarizeClientNativeRequest(connection ConnectionContext, request ExecutionRequest) ClientNativeRequestSummaryExchange {
	_ = s
	exchange := ClientNativeRequestSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientNativeRequestSummaryRow{nativeRequestSummaryRow(request)}
	}
	exchange.Result = exchange.nativeRequestSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// SummarizeClientNativeBatchRequest returns row metadata for one batch native execution request.
func (s PlanningService) SummarizeClientNativeBatchRequest(connection ConnectionContext, request BatchExecutionRequest) ClientNativeRequestSummaryExchange {
	_ = s
	exchange := ClientNativeRequestSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientNativeRequestSummaryRow{nativeBatchRequestSummaryRow(request)}
	}
	exchange.Result = exchange.nativeRequestSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether native request summary metadata can be returned.
func (e ClientNativeRequestSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientNativeRequestSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientNativeRequestSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientNativeRequestSummaryExchange) nativeRequestSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     nativeRequestSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.nativeRequestSummaryRows(),
		Final: true,
	})
}

func nativeRequestSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Kind", Type: DataTypeString},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "User", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Result_kind", Type: DataTypeString, Nullable: true},
		{Name: "Result_columns", Type: DataTypeInt},
		{Name: "Session_actions", Type: DataTypeInt},
		{Name: "Access_requirements", Type: DataTypeInt},
		{Name: "Max_rows", Type: DataTypeInt},
		{Name: "Batch_size", Type: DataTypeInt},
		{Name: "Streaming", Type: DataTypeBool},
		{Name: "Cursor", Type: DataTypeString, Nullable: true},
		{Name: "Cancelable", Type: DataTypeBool},
		{Name: "Parameters", Type: DataTypeInt},
		{Name: "Parameter_sets", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "SQL", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientNativeRequestSummaryExchange) nativeRequestSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(row.Schema),
			metadataStringCell(string(row.User)),
			metadataBoolCell(row.Supported),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataStringCell(string(row.ResultKind)),
			metadataIntCell(row.ResultColumns),
			metadataIntCell(row.StatementActions),
			metadataIntCell(row.AccessRequirements),
			metadataIntCell(row.MaxRows),
			metadataIntCell(row.BatchSize),
			metadataBoolCell(row.Streaming),
			metadataStringCell(string(row.Cursor)),
			metadataBoolCell(row.Cancelable),
			metadataIntCell(row.ParameterCount),
			metadataIntCell(row.ParameterSetCount),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
			metadataStringCell(row.SQL),
		})
	}
	return rows
}

func nativeRequestSummaryRow(request ExecutionRequest) ClientNativeRequestSummaryRow {
	prepared := request.Bound.Prepared
	return ClientNativeRequestSummaryRow{
		Kind:               ClientNativeRequestSingle,
		RequestID:          request.Options.RequestID,
		SQL:                prepared.SQL,
		Schema:             prepared.DefaultSchema,
		User:               prepared.Session.User,
		Supported:          request.SupportedForExecution(),
		AccessIntent:       prepared.AccessIntent(),
		Lifecycle:          clientPlanLifecycleKind(prepared.Kind),
		LifecycleSteps:     clientPlanLifecycleStepCount(prepared.Kind),
		ResultKind:         request.Result.Kind,
		ResultColumns:      len(request.ResultColumns),
		StatementActions:   len(request.SessionActions),
		AccessRequirements: len(request.Access),
		MaxRows:            request.Options.MaxRows,
		BatchSize:          request.Options.BatchSize,
		Streaming:          request.Options.Streaming,
		Cursor:             request.Options.Cursor,
		Cancelable:         request.Options.Cancelable,
		ParameterCount:     len(request.Bound.Parameters.Bindings),
		DiagnosticCodes:    request.Diagnostics.Codes(),
	}
}

func nativeBatchRequestSummaryRow(request BatchExecutionRequest) ClientNativeRequestSummaryRow {
	prepared := request.Prepared
	return ClientNativeRequestSummaryRow{
		Kind:               ClientNativeRequestBatch,
		RequestID:          request.Options.RequestID,
		SQL:                prepared.SQL,
		Schema:             prepared.DefaultSchema,
		User:               prepared.Session.User,
		Supported:          request.SupportedForExecution(),
		AccessIntent:       prepared.AccessIntent(),
		Lifecycle:          clientPlanLifecycleKind(prepared.Kind),
		LifecycleSteps:     clientPlanLifecycleStepCount(prepared.Kind),
		ResultKind:         request.Result.Kind,
		ResultColumns:      len(request.ResultColumns),
		StatementActions:   len(request.SessionActions),
		AccessRequirements: len(request.Access),
		MaxRows:            request.Options.MaxRows,
		BatchSize:          request.Options.BatchSize,
		Streaming:          request.Options.Streaming,
		Cursor:             request.Options.Cursor,
		Cancelable:         request.Options.Cancelable,
		ParameterSetCount:  len(request.ParameterSets),
		DiagnosticCodes:    request.Diagnostics.Codes(),
	}
}
