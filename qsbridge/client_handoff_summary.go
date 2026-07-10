package qsbridge

// ClientHandoffSummaryRow describes aggregate metadata for one handoff bundle.
type ClientHandoffSummaryRow struct {
	User                   UserName
	Schema                 string
	Protocol               ProtocolKind
	Supported              bool
	StatementCount         int
	ReadCount              int
	WriteCount             int
	SelectLifecycleCount   int
	MutationLifecycleCount int
	NativeCount            int
	LegacyFallbackCount    int
	RejectedCount          int
	DeniedCount            int
	ProtocolRejectedCount  int
	DiagnosticCodes        []DiagnosticCode
}

// ClientHandoffSummaryExchange is adapter-facing aggregate handoff metadata.
type ClientHandoffSummaryExchange struct {
	Connection          ConnectionContext
	Bundle              ClientHandoffBundle
	Rows                []ClientHandoffSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientHandoffBundle returns aggregate row metadata for final handoff decisions.
func (s PlanningService) SummarizeClientHandoffBundle(bundle ClientHandoffBundle) ClientHandoffSummaryExchange {
	_ = s
	exchange := ClientHandoffSummaryExchange{
		Connection:          cloneConnectionContext(bundle.Connection),
		Bundle:              cloneClientHandoffBundle(bundle),
		ExchangeDiagnostics: cloneDiagnosticSet(bundle.Connection.Diagnostics),
	}
	if bundle.Connection.Supported() {
		exchange.Rows = []ClientHandoffSummaryRow{clientHandoffSummaryRow(bundle)}
	}
	exchange.Result = exchange.clientHandoffSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(bundle.Connection.Protocol)
	return exchange
}

// Supported reports whether handoff summary metadata can be returned.
func (e ClientHandoffSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientHandoffSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking handoff summary error, if any.
func (e ClientHandoffSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientHandoffSummaryExchange) clientHandoffSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     clientHandoffSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.clientHandoffSummaryRows(),
		Final: true,
	})
}

func clientHandoffSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "User", Type: DataTypeString, Nullable: true},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "Protocol", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Statements", Type: DataTypeInt},
		{Name: "Reads", Type: DataTypeInt},
		{Name: "Writes", Type: DataTypeInt},
		{Name: "Select_lifecycle", Type: DataTypeInt},
		{Name: "Mutation_lifecycle", Type: DataTypeInt},
		{Name: "Native", Type: DataTypeInt},
		{Name: "Legacy_fallback", Type: DataTypeInt},
		{Name: "Rejected", Type: DataTypeInt},
		{Name: "Denied", Type: DataTypeInt},
		{Name: "Protocol_rejected", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientHandoffSummaryExchange) clientHandoffSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.User)),
			metadataStringCell(row.Schema),
			metadataStringCell(string(row.Protocol)),
			metadataBoolCell(row.Supported),
			metadataIntCell(row.StatementCount),
			metadataIntCell(row.ReadCount),
			metadataIntCell(row.WriteCount),
			metadataIntCell(row.SelectLifecycleCount),
			metadataIntCell(row.MutationLifecycleCount),
			metadataIntCell(row.NativeCount),
			metadataIntCell(row.LegacyFallbackCount),
			metadataIntCell(row.RejectedCount),
			metadataIntCell(row.DeniedCount),
			metadataIntCell(row.ProtocolRejectedCount),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func clientHandoffSummaryRow(bundle ClientHandoffBundle) ClientHandoffSummaryRow {
	row := ClientHandoffSummaryRow{
		User:            bundle.Connection.Session.User,
		Schema:          bundle.Connection.Session.CurrentSchema,
		Protocol:        bundle.Connection.Protocol.Kind,
		Supported:       bundle.Supported(),
		StatementCount:  len(bundle.Statements),
		DiagnosticCodes: bundle.Diagnostics.Codes(),
	}
	for _, statement := range bundle.Statements {
		switch statement.Plan.Prepared.AccessIntent() {
		case PhysicalAccessRead:
			row.ReadCount++
		case PhysicalAccessWrite:
			row.WriteCount++
		}
		switch clientPlanLifecycleKind(statement.Plan.Prepared.Kind) {
		case ClientPlanLifecycleSelect:
			row.SelectLifecycleCount++
		case ClientPlanLifecycleMutation:
			row.MutationLifecycleCount++
		}
		switch statement.Handoff.HandoffKind() {
		case ExecutionHandoffNative:
			row.NativeCount++
		case ExecutionHandoffLegacyFallback:
			row.LegacyFallbackCount++
		case ExecutionHandoffRejected:
			row.RejectedCount++
		case ExecutionHandoffDenied:
			row.DeniedCount++
		case ExecutionHandoffProtocolRejected:
			row.ProtocolRejectedCount++
		}
	}
	return row
}
