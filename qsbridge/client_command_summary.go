package qsbridge

// ClientCommandSummaryRow describes one non-SQL protocol command exchange.
type ClientCommandSummaryRow struct {
	Command         ClientCommandKind
	PayloadPresent  bool
	PayloadLength   int
	CloseConnection bool
	SessionApplied  bool
	SessionActions  int
	Status          string
	Warnings        int
	StatusFlags     []ProtocolStatusFlag
	Supported       bool
	DiagnosticCodes []DiagnosticCode
}

// ClientCommandSummaryExchange is adapter-facing command exchange metadata.
type ClientCommandSummaryExchange struct {
	Connection          ConnectionContext
	Command             ClientCommandExchange
	Rows                []ClientCommandSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientCommand returns row metadata for one non-SQL protocol command.
func (s PlanningService) SummarizeClientCommand(connection ConnectionContext, command ClientCommandExchange) ClientCommandSummaryExchange {
	_ = s
	exchange := ClientCommandSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Command:             cloneClientCommandExchange(command),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientCommandSummaryRow{commandSummaryRow(command)}
	}
	exchange.Result = exchange.commandSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether command summary metadata can be returned.
func (e ClientCommandSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientCommandSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientCommandSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientCommandSummaryExchange) commandSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     commandSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.commandSummaryRows(),
		Final: true,
	})
}

func commandSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Command", Type: DataTypeString},
		{Name: "Payload_present", Type: DataTypeBool},
		{Name: "Payload_length", Type: DataTypeInt},
		{Name: "Close_connection", Type: DataTypeBool},
		{Name: "Session_applied", Type: DataTypeBool},
		{Name: "Session_actions", Type: DataTypeInt},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Warnings", Type: DataTypeInt},
		{Name: "Status_flags", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientCommandSummaryExchange) commandSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Command)),
			metadataBoolCell(row.PayloadPresent),
			metadataIntCell(row.PayloadLength),
			metadataBoolCell(row.CloseConnection),
			metadataBoolCell(row.SessionApplied),
			metadataIntCell(row.SessionActions),
			metadataStringCell(row.Status),
			metadataIntCell(row.Warnings),
			metadataStringCell(joinProtocolStatusFlags(row.StatusFlags)),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func commandSummaryRow(command ClientCommandExchange) ClientCommandSummaryRow {
	return ClientCommandSummaryRow{
		Command:         command.Kind,
		PayloadPresent:  command.Payload != "",
		PayloadLength:   len(command.Payload),
		CloseConnection: command.CloseConnection,
		SessionApplied:  command.Session.Applied,
		SessionActions:  len(command.Response.SessionActions),
		Status:          command.Response.Status,
		Warnings:        int(command.Response.Warnings),
		StatusFlags:     append([]ProtocolStatusFlag(nil), command.Response.Flags...),
		Supported:       command.Supported(),
		DiagnosticCodes: command.Diagnostics.Codes(),
	}
}

func cloneClientCommandExchange(exchange ClientCommandExchange) ClientCommandExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Session = cloneClientSessionActionExchange(exchange.Session)
	exchange.Response = cloneProtocolStatementResponse(exchange.Response)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}
