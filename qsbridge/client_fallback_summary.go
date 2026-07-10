package qsbridge

// ClientFallbackKind identifies single-statement or batch fallback metadata.
type ClientFallbackKind string

const (
	// ClientFallbackSingle identifies a single execution fallback descriptor.
	ClientFallbackSingle ClientFallbackKind = "single"
	// ClientFallbackBatch identifies a batch execution fallback descriptor.
	ClientFallbackBatch ClientFallbackKind = "batch"
)

// ClientFallbackSummaryRow describes one legacy fallback handoff descriptor.
type ClientFallbackSummaryRow struct {
	Kind              ClientFallbackKind
	RequestID         ExecutionRequestID
	SQL               string
	Schema            string
	User              UserName
	Roles             []RoleName
	Route             RouteKind
	RouteReason       RouteReason
	NativeEligible    bool
	Supported         bool
	MaxRows           int
	BatchSize         int
	Streaming         bool
	Cursor            CursorMode
	ParameterCount    int
	ParameterSetCount int
	DiagnosticCodes   []DiagnosticCode
}

// ClientFallbackSummaryExchange is adapter-facing legacy fallback metadata.
type ClientFallbackSummaryExchange struct {
	Connection          ConnectionContext
	Rows                []ClientFallbackSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientFallbackRequest returns row metadata for one single fallback request.
func (s PlanningService) SummarizeClientFallbackRequest(connection ConnectionContext, fallback FallbackRequest) ClientFallbackSummaryExchange {
	_ = s
	exchange := ClientFallbackSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientFallbackSummaryRow{fallbackSummaryRow(fallback)}
	}
	exchange.Result = exchange.fallbackSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// SummarizeClientBatchFallbackRequest returns row metadata for one batch fallback request.
func (s PlanningService) SummarizeClientBatchFallbackRequest(connection ConnectionContext, fallback BatchFallbackRequest) ClientFallbackSummaryExchange {
	_ = s
	exchange := ClientFallbackSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientFallbackSummaryRow{batchFallbackSummaryRow(fallback)}
	}
	exchange.Result = exchange.fallbackSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether fallback summary metadata can be returned.
func (e ClientFallbackSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientFallbackSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientFallbackSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientFallbackSummaryExchange) fallbackSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     fallbackSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.fallbackSummaryRows(),
		Final: true,
	})
}

func fallbackSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Kind", Type: DataTypeString},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "User", Type: DataTypeString, Nullable: true},
		{Name: "Roles", Type: DataTypeString, Nullable: true},
		{Name: "Route", Type: DataTypeString},
		{Name: "Route_reason", Type: DataTypeString, Nullable: true},
		{Name: "Native_eligible", Type: DataTypeBool},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Max_rows", Type: DataTypeInt},
		{Name: "Batch_size", Type: DataTypeInt},
		{Name: "Streaming", Type: DataTypeBool},
		{Name: "Cursor", Type: DataTypeString, Nullable: true},
		{Name: "Parameters", Type: DataTypeInt},
		{Name: "Parameter_sets", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "SQL", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientFallbackSummaryExchange) fallbackSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(row.Schema),
			metadataStringCell(string(row.User)),
			metadataStringCell(joinRoleNames(row.Roles)),
			metadataStringCell(string(row.Route)),
			metadataStringCell(string(row.RouteReason)),
			metadataBoolCell(row.NativeEligible),
			metadataBoolCell(row.Supported),
			metadataIntCell(row.MaxRows),
			metadataIntCell(row.BatchSize),
			metadataBoolCell(row.Streaming),
			metadataStringCell(string(row.Cursor)),
			metadataIntCell(row.ParameterCount),
			metadataIntCell(row.ParameterSetCount),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
			metadataStringCell(row.SQL),
		})
	}
	return rows
}

func fallbackSummaryRow(fallback FallbackRequest) ClientFallbackSummaryRow {
	return ClientFallbackSummaryRow{
		Kind:            ClientFallbackSingle,
		RequestID:       fallback.Options.RequestID,
		SQL:             fallback.SQL,
		Schema:          fallback.DefaultSchema,
		User:            fallback.Session.User,
		Roles:           append([]RoleName(nil), fallback.Session.Roles...),
		Route:           fallback.Route.Kind,
		RouteReason:     fallback.Route.Reason,
		NativeEligible:  fallback.Route.NativeEligible,
		Supported:       fallback.Supported(),
		MaxRows:         fallback.Options.MaxRows,
		BatchSize:       fallback.Options.BatchSize,
		Streaming:       fallback.Options.Streaming,
		Cursor:          fallback.Options.Cursor,
		ParameterCount:  len(fallback.Parameters),
		DiagnosticCodes: fallback.Diagnostics.Codes(),
	}
}

func batchFallbackSummaryRow(fallback BatchFallbackRequest) ClientFallbackSummaryRow {
	return ClientFallbackSummaryRow{
		Kind:              ClientFallbackBatch,
		RequestID:         fallback.Options.RequestID,
		SQL:               fallback.SQL,
		Schema:            fallback.DefaultSchema,
		User:              fallback.Session.User,
		Roles:             append([]RoleName(nil), fallback.Session.Roles...),
		Route:             fallback.Route.Kind,
		RouteReason:       fallback.Route.Reason,
		NativeEligible:    fallback.Route.NativeEligible,
		Supported:         fallback.Supported(),
		MaxRows:           fallback.Options.MaxRows,
		BatchSize:         fallback.Options.BatchSize,
		Streaming:         fallback.Options.Streaming,
		Cursor:            fallback.Options.Cursor,
		ParameterSetCount: len(fallback.ParameterSets),
		DiagnosticCodes:   fallback.Diagnostics.Codes(),
	}
}
