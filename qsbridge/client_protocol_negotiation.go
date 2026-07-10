package qsbridge

// ClientProtocolNegotiationRow describes one protocol execution negotiation.
type ClientProtocolNegotiationRow struct {
	Protocol        ProtocolKind
	Driver          string
	Mode            ProtocolExecutionMode
	Supported       bool
	RequestID       ExecutionRequestID
	MaxRows         int
	BatchSize       int
	Streaming       bool
	Cursor          CursorMode
	Cancelable      bool
	Explain         bool
	Profile         bool
	Capabilities    []ProtocolCapability
	DiagnosticCodes []DiagnosticCode
}

// ClientProtocolNegotiationSummaryRow describes aggregate protocol negotiation metadata.
type ClientProtocolNegotiationSummaryRow struct {
	NegotiationCount        int
	SupportedCount          int
	UnsupportedCount        int
	CapabilityCount         int
	DiagnosticCount         int
	StreamingRequestedCount int
	CursorRequestedCount    int
	CancelableCount         int
	ExplainCount            int
	ProfileCount            int
}

// ClientProtocolNegotiationExchange is adapter-facing protocol negotiation metadata.
type ClientProtocolNegotiationExchange struct {
	Connection          ConnectionContext
	Negotiation         ProtocolNegotiation
	Rows                []ClientProtocolNegotiationRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	Diagnostics         DiagnosticSet
	ExchangeDiagnostics DiagnosticSet
}

// NegotiateClientProtocolExecution validates one requested protocol execution shape.
func (s PlanningService) NegotiateClientProtocolExecution(connection ConnectionContext, mode ProtocolExecutionMode, options ExecutionOptions) ClientProtocolNegotiationExchange {
	_ = s
	negotiation := connection.Protocol.NegotiateExecution(mode, options)
	exchange := ClientProtocolNegotiationExchange{
		Connection:          cloneConnectionContext(connection),
		Negotiation:         cloneProtocolNegotiation(negotiation),
		Diagnostics:         cloneDiagnosticSet(negotiation.Diagnostics),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientProtocolNegotiationRow{protocolNegotiationRow(negotiation)}
	}
	exchange.Result = exchange.protocolNegotiationResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether protocol negotiation metadata can be returned.
func (e ClientProtocolNegotiationExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientProtocolNegotiationExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientProtocolNegotiationExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientProtocolNegotiationExchange) protocolNegotiationResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     protocolNegotiationResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.protocolNegotiationRows(),
		Final: true,
	})
}

func protocolNegotiationResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Protocol", Type: DataTypeString, Nullable: true},
		{Name: "Driver", Type: DataTypeString, Nullable: true},
		{Name: "Mode", Type: DataTypeString},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Max_rows", Type: DataTypeInt},
		{Name: "Batch_size", Type: DataTypeInt},
		{Name: "Streaming", Type: DataTypeBool},
		{Name: "Cursor", Type: DataTypeString, Nullable: true},
		{Name: "Cancelable", Type: DataTypeBool},
		{Name: "Explain", Type: DataTypeBool},
		{Name: "Profile", Type: DataTypeBool},
		{Name: "Capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientProtocolNegotiationExchange) protocolNegotiationRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Protocol)),
			metadataStringCell(row.Driver),
			metadataStringCell(string(row.Mode)),
			metadataBoolCell(row.Supported),
			metadataStringCell(string(row.RequestID)),
			metadataIntCell(row.MaxRows),
			metadataIntCell(row.BatchSize),
			metadataBoolCell(row.Streaming),
			metadataStringCell(string(row.Cursor)),
			metadataBoolCell(row.Cancelable),
			metadataBoolCell(row.Explain),
			metadataBoolCell(row.Profile),
			metadataStringCell(joinProtocolCapabilities(row.Capabilities)),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func protocolNegotiationRow(negotiation ProtocolNegotiation) ClientProtocolNegotiationRow {
	return ClientProtocolNegotiationRow{
		Protocol:        negotiation.Profile.Kind,
		Driver:          negotiation.Profile.Driver,
		Mode:            negotiation.Mode,
		Supported:       negotiation.Supported(),
		RequestID:       negotiation.Options.RequestID,
		MaxRows:         negotiation.Options.MaxRows,
		BatchSize:       negotiation.Options.BatchSize,
		Streaming:       negotiation.Options.Streaming,
		Cursor:          negotiation.Options.Cursor,
		Cancelable:      negotiation.Options.Cancelable,
		Explain:         negotiation.Options.TraceExplain,
		Profile:         negotiation.Options.IncludeProfile,
		Capabilities:    append([]ProtocolCapability(nil), negotiation.Profile.Capabilities...),
		DiagnosticCodes: negotiation.Diagnostics.Codes(),
	}
}

func cloneProtocolNegotiation(negotiation ProtocolNegotiation) ProtocolNegotiation {
	negotiation.Profile = negotiation.Profile.Clone()
	negotiation.Diagnostics = cloneDiagnosticSet(negotiation.Diagnostics)
	return negotiation
}

func summarizeClientProtocolNegotiations(rows []ClientProtocolNegotiationRow) ClientProtocolNegotiationSummaryRow {
	summary := ClientProtocolNegotiationSummaryRow{NegotiationCount: len(rows)}
	for _, row := range rows {
		if row.Supported {
			summary.SupportedCount++
		} else {
			summary.UnsupportedCount++
		}
		summary.CapabilityCount += len(row.Capabilities)
		summary.DiagnosticCount += len(row.DiagnosticCodes)
		if row.Streaming {
			summary.StreamingRequestedCount++
		}
		if row.Cursor != "" {
			summary.CursorRequestedCount++
		}
		if row.Cancelable {
			summary.CancelableCount++
		}
		if row.Explain {
			summary.ExplainCount++
		}
		if row.Profile {
			summary.ProfileCount++
		}
	}
	return summary
}
