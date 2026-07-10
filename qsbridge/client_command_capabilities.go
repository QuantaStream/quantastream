package qsbridge

// ClientCommandCapabilityRow describes one non-SQL protocol command supported by qsbridge metadata.
type ClientCommandCapabilityRow struct {
	Command              ClientCommandKind
	Supported            bool
	RequiresPayload      bool
	SessionAction        bool
	ClosesConnection     bool
	RequiredCapabilities []ProtocolCapability
	Detail               string
}

// ClientCommandCapabilitySummaryRow describes aggregate non-SQL command capability metadata.
type ClientCommandCapabilitySummaryRow struct {
	CommandCount                   int
	SupportedCount                 int
	UnsupportedCount               int
	RequiresPayloadCount           int
	SessionActionCount             int
	ClosesConnectionCount          int
	StatementResultCapabilityCount int
	SessionActionCapabilityCount   int
	AllSupported                   bool
}

// ClientCommandCapabilityExchange is adapter-facing command capability metadata.
type ClientCommandCapabilityExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Rows                []ClientCommandCapabilityRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientCommandCapabilities returns qsbridge-supported non-SQL command metadata.
func (s PlanningService) ListClientCommandCapabilities(connection ConnectionContext) ClientCommandCapabilityExchange {
	_ = s
	exchange := ClientCommandCapabilityExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = commandCapabilityRows(connection.Protocol)
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.commandCapabilityResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether command capability metadata can be returned.
func (e ClientCommandCapabilityExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientCommandCapabilityExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientCommandCapabilityExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientCommandCapabilityExchange) commandCapabilityResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     commandCapabilityResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.commandCapabilityResultRows(),
		Final: true,
	})
}

func commandCapabilityResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Command", Type: DataTypeString},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Requires_payload", Type: DataTypeBool},
		{Name: "Session_action", Type: DataTypeBool},
		{Name: "Closes_connection", Type: DataTypeBool},
		{Name: "Required_capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientCommandCapabilityExchange) commandCapabilityResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Command)),
			metadataBoolCell(row.Supported),
			metadataBoolCell(row.RequiresPayload),
			metadataBoolCell(row.SessionAction),
			metadataBoolCell(row.ClosesConnection),
			metadataStringCell(joinProtocolCapabilities(row.RequiredCapabilities)),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}

func commandCapabilityRows(profile ProtocolProfile) []ClientCommandCapabilityRow {
	rows := []ClientCommandCapabilityRow{
		commandCapabilityRow(profile, ClientCommandPing, false, false, false, []ProtocolCapability{ProtocolCapabilityStatementResults}, "protocol ping"),
		commandCapabilityRow(profile, ClientCommandQuit, false, false, true, []ProtocolCapability{ProtocolCapabilityStatementResults}, "connection close"),
		commandCapabilityRow(profile, ClientCommandResetConnection, false, true, false, []ProtocolCapability{ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions}, "connection session reset"),
		commandCapabilityRow(profile, ClientCommandInitSchema, true, true, false, []ProtocolCapability{ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions}, "current schema selection"),
	}
	return rows
}

func summarizeCommandCapabilityRows(rows []ClientCommandCapabilityRow) ClientCommandCapabilitySummaryRow {
	summary := ClientCommandCapabilitySummaryRow{
		CommandCount: len(rows),
		AllSupported: len(rows) > 0,
	}
	for _, row := range rows {
		if row.Supported {
			summary.SupportedCount++
		} else {
			summary.UnsupportedCount++
			summary.AllSupported = false
		}
		if row.RequiresPayload {
			summary.RequiresPayloadCount++
		}
		if row.SessionAction {
			summary.SessionActionCount++
		}
		if row.ClosesConnection {
			summary.ClosesConnectionCount++
		}
		if protocolCapabilitiesContain(row.RequiredCapabilities, ProtocolCapabilityStatementResults) {
			summary.StatementResultCapabilityCount++
		}
		if protocolCapabilitiesContain(row.RequiredCapabilities, ProtocolCapabilitySessionActions) {
			summary.SessionActionCapabilityCount++
		}
	}
	return summary
}

func commandCapabilityRow(profile ProtocolProfile, command ClientCommandKind, payload bool, sessionAction bool, closes bool, capabilities []ProtocolCapability, detail string) ClientCommandCapabilityRow {
	return ClientCommandCapabilityRow{
		Command:              command,
		Supported:            profileSupportsAll(profile, capabilities),
		RequiresPayload:      payload,
		SessionAction:        sessionAction,
		ClosesConnection:     closes,
		RequiredCapabilities: append([]ProtocolCapability(nil), capabilities...),
		Detail:               detail,
	}
}

func profileSupportsAll(profile ProtocolProfile, capabilities []ProtocolCapability) bool {
	for _, capability := range capabilities {
		if !profile.Supports(capability) {
			return false
		}
	}
	return true
}

func protocolCapabilitiesContain(capabilities []ProtocolCapability, target ProtocolCapability) bool {
	for _, capability := range capabilities {
		if capability == target {
			return true
		}
	}
	return false
}

func joinProtocolCapabilities(capabilities []ProtocolCapability) string {
	if len(capabilities) == 0 {
		return ""
	}
	values := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		values = append(values, string(capability))
	}
	return joinStringValues(values)
}
