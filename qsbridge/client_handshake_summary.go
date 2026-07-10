package qsbridge

// ClientHandshakeSummaryRow describes one protocol handshake greeting exchange.
type ClientHandshakeSummaryRow struct {
	SessionID            SessionID
	Protocol             ProtocolKind
	Driver               string
	ServerVersion        string
	AuthPlugin           string
	CharacterSet         string
	Collation            string
	StatusFlags          []ClientHandshakeStatusFlag
	AcceptedCapabilities string
	Supported            bool
	DiagnosticCodes      []DiagnosticCode
}

// ClientHandshakeSummaryExchange is adapter-facing handshake metadata.
type ClientHandshakeSummaryExchange struct {
	Handshake           ClientHandshakeExchange
	Rows                []ClientHandshakeSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientHandshake returns row metadata for one protocol handshake exchange.
func (s PlanningService) SummarizeClientHandshake(handshake ClientHandshakeExchange) ClientHandshakeSummaryExchange {
	_ = s
	exchange := ClientHandshakeSummaryExchange{
		Handshake: cloneClientHandshakeExchange(handshake),
		Rows: []ClientHandshakeSummaryRow{
			handshakeSummaryRow(handshake),
		},
	}
	exchange.Result = exchange.handshakeSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(handshake.Greeting.Protocol)
	return exchange
}

// Supported reports whether handshake summary metadata can be returned.
func (e ClientHandshakeSummaryExchange) Supported() bool {
	return !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientHandshakeSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientHandshakeSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientHandshakeSummaryExchange) handshakeSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     handshakeSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.handshakeSummaryRows(),
		Final: true,
	})
}

func handshakeSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Session_id", Type: DataTypeString, Nullable: true},
		{Name: "Protocol", Type: DataTypeString, Nullable: true},
		{Name: "Driver", Type: DataTypeString, Nullable: true},
		{Name: "Server_version", Type: DataTypeString, Nullable: true},
		{Name: "Auth_plugin", Type: DataTypeString, Nullable: true},
		{Name: "Character_set", Type: DataTypeString, Nullable: true},
		{Name: "Collation", Type: DataTypeString, Nullable: true},
		{Name: "Status_flags", Type: DataTypeString, Nullable: true},
		{Name: "Accepted_capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientHandshakeSummaryExchange) handshakeSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.SessionID)),
			metadataStringCell(string(row.Protocol)),
			metadataStringCell(row.Driver),
			metadataStringCell(row.ServerVersion),
			metadataStringCell(row.AuthPlugin),
			metadataStringCell(row.CharacterSet),
			metadataStringCell(row.Collation),
			metadataStringCell(joinHandshakeStatusFlags(row.StatusFlags)),
			metadataStringCell(row.AcceptedCapabilities),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func handshakeSummaryRow(handshake ClientHandshakeExchange) ClientHandshakeSummaryRow {
	return ClientHandshakeSummaryRow{
		SessionID:            handshake.Greeting.SessionID,
		Protocol:             handshake.Greeting.Protocol.Kind,
		Driver:               handshake.Greeting.Protocol.Driver,
		ServerVersion:        handshake.Greeting.ServerVersion,
		AuthPlugin:           handshake.Greeting.AuthPlugin,
		CharacterSet:         handshake.Greeting.CharacterSet,
		Collation:            handshake.Greeting.Collation,
		StatusFlags:          append([]ClientHandshakeStatusFlag(nil), handshake.Greeting.StatusFlags...),
		AcceptedCapabilities: joinClientCapabilities(handshake.Negotiation.Accepted),
		Supported:            handshake.Supported(),
		DiagnosticCodes:      handshake.Diagnostics.Codes(),
	}
}

func cloneClientHandshakeExchange(exchange ClientHandshakeExchange) ClientHandshakeExchange {
	exchange.Request = exchange.Request.Clone()
	exchange.Greeting = cloneClientHandshakeGreeting(exchange.Greeting)
	exchange.Negotiation = cloneConnectionCapabilityNegotiation(exchange.Negotiation)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}

func cloneClientHandshakeGreeting(greeting ClientHandshakeGreeting) ClientHandshakeGreeting {
	greeting.Protocol = greeting.Protocol.Clone()
	greeting.StatusFlags = append([]ClientHandshakeStatusFlag(nil), greeting.StatusFlags...)
	return greeting
}

func joinHandshakeStatusFlags(flags []ClientHandshakeStatusFlag) string {
	if len(flags) == 0 {
		return ""
	}
	values := make([]string, 0, len(flags))
	for _, flag := range flags {
		values = append(values, string(flag))
	}
	return joinStringValues(values)
}
