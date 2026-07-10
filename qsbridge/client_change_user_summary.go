package qsbridge

// ClientChangeUserSummaryRow describes one change-user exchange.
type ClientChangeUserSummaryRow struct {
	PreviousUser         UserName
	NextUser             UserName
	PreviousSchema       string
	NextSchema           string
	AcceptedCapabilities string
	Applied              bool
	SessionActions       int
	Status               string
	Supported            bool
	DiagnosticCodes      []DiagnosticCode
}

// ClientChangeUserSummaryExchange is adapter-facing change-user metadata.
type ClientChangeUserSummaryExchange struct {
	Connection          ConnectionContext
	ChangeUser          ClientChangeUserExchange
	Rows                []ClientChangeUserSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientChangeUser returns row metadata for one change-user exchange.
func (s PlanningService) SummarizeClientChangeUser(connection ConnectionContext, changeUser ClientChangeUserExchange) ClientChangeUserSummaryExchange {
	_ = s
	exchange := ClientChangeUserSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ChangeUser:          cloneClientChangeUserExchange(changeUser),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientChangeUserSummaryRow{changeUserSummaryRow(changeUser)}
	}
	exchange.Result = exchange.changeUserSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether change-user summary metadata can be returned.
func (e ClientChangeUserSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientChangeUserSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientChangeUserSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientChangeUserSummaryExchange) changeUserSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     changeUserSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.changeUserSummaryRows(),
		Final: true,
	})
}

func changeUserSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Previous_user", Type: DataTypeString, Nullable: true},
		{Name: "Next_user", Type: DataTypeString, Nullable: true},
		{Name: "Previous_schema", Type: DataTypeString, Nullable: true},
		{Name: "Next_schema", Type: DataTypeString, Nullable: true},
		{Name: "Accepted_capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Applied", Type: DataTypeBool},
		{Name: "Session_actions", Type: DataTypeInt},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientChangeUserSummaryExchange) changeUserSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.PreviousUser)),
			metadataStringCell(string(row.NextUser)),
			metadataStringCell(row.PreviousSchema),
			metadataStringCell(row.NextSchema),
			metadataStringCell(row.AcceptedCapabilities),
			metadataBoolCell(row.Applied),
			metadataIntCell(row.SessionActions),
			metadataStringCell(row.Status),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func changeUserSummaryRow(changeUser ClientChangeUserExchange) ClientChangeUserSummaryRow {
	return ClientChangeUserSummaryRow{
		PreviousUser:         changeUser.Transition.Before.User,
		NextUser:             changeUser.Transition.After.User,
		PreviousSchema:       changeUser.Transition.Before.CurrentSchema,
		NextSchema:           changeUser.Transition.After.CurrentSchema,
		AcceptedCapabilities: joinClientCapabilities(changeUser.Negotiation.Accepted),
		Applied:              changeUser.Applied,
		SessionActions:       len(changeUser.Response.SessionActions),
		Status:               changeUser.Response.Status,
		Supported:            changeUser.Supported(),
		DiagnosticCodes:      changeUser.Diagnostics.Codes(),
	}
}

func cloneClientChangeUserExchange(exchange ClientChangeUserExchange) ClientChangeUserExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Request = exchange.Request.Clone()
	exchange.Negotiation = cloneConnectionCapabilityNegotiation(exchange.Negotiation)
	exchange.Transition = cloneSessionTransition(exchange.Transition)
	exchange.Response = cloneProtocolStatementResponse(exchange.Response)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}

func joinClientCapabilities(capabilities ClientCapabilities) string {
	if len(capabilities) == 0 {
		return ""
	}
	values := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		values = append(values, string(capability))
	}
	return joinStringValues(values)
}
