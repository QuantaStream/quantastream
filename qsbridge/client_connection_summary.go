package qsbridge

// ClientConnectionSummaryOperation identifies a connection lifecycle summary row.
type ClientConnectionSummaryOperation string

const (
	// ClientConnectionSummaryAccept identifies connection acceptance metadata.
	ClientConnectionSummaryAccept ClientConnectionSummaryOperation = "accept"
	// ClientConnectionSummaryClose identifies connection close metadata.
	ClientConnectionSummaryClose ClientConnectionSummaryOperation = "close"
)

// ClientConnectionSummaryRow describes one connection lifecycle exchange.
type ClientConnectionSummaryRow struct {
	Operation            ClientConnectionSummaryOperation
	SessionID            SessionID
	User                 UserName
	Schema               string
	AcceptedCapabilities string
	Registered           bool
	CloseConnection      bool
	RemovedSession       bool
	Status               string
	Supported            bool
	DiagnosticCodes      []DiagnosticCode
}

// ClientConnectionSummaryExchange is adapter-facing connection lifecycle metadata.
type ClientConnectionSummaryExchange struct {
	Connection          ConnectionContext
	Rows                []ClientConnectionSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientConnection returns row metadata for one accepted connection exchange.
func (s PlanningService) SummarizeClientConnection(connection ClientConnectionExchange) ClientConnectionSummaryExchange {
	_ = s
	exchange := ClientConnectionSummaryExchange{
		Connection: cloneConnectionContext(connection.Connection),
		Rows: []ClientConnectionSummaryRow{
			connectionSummaryAcceptRow(connection),
		},
	}
	exchange.Result = exchange.connectionSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Connection.Protocol)
	return exchange
}

// SummarizeClientConnectionClose returns row metadata for one connection close exchange.
func (s PlanningService) SummarizeClientConnectionClose(connection ConnectionContext, closed ClientConnectionCloseExchange) ClientConnectionSummaryExchange {
	_ = s
	exchange := ClientConnectionSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientConnectionSummaryRow{connectionSummaryCloseRow(closed)}
	}
	exchange.Result = exchange.connectionSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether connection summary metadata can be returned.
func (e ClientConnectionSummaryExchange) Supported() bool {
	return !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientConnectionSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientConnectionSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientConnectionSummaryExchange) connectionSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     connectionSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.connectionSummaryRows(),
		Final: true,
	})
}

func connectionSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Operation", Type: DataTypeString},
		{Name: "Session_id", Type: DataTypeString, Nullable: true},
		{Name: "User", Type: DataTypeString, Nullable: true},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "Accepted_capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Registered", Type: DataTypeBool},
		{Name: "Close_connection", Type: DataTypeBool},
		{Name: "Removed_session", Type: DataTypeBool},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientConnectionSummaryExchange) connectionSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Operation)),
			metadataStringCell(string(row.SessionID)),
			metadataStringCell(string(row.User)),
			metadataStringCell(row.Schema),
			metadataStringCell(row.AcceptedCapabilities),
			metadataBoolCell(row.Registered),
			metadataBoolCell(row.CloseConnection),
			metadataBoolCell(row.RemovedSession),
			metadataStringCell(row.Status),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func connectionSummaryAcceptRow(connection ClientConnectionExchange) ClientConnectionSummaryRow {
	return ClientConnectionSummaryRow{
		Operation:            ClientConnectionSummaryAccept,
		SessionID:            connection.Connection.Session.ID,
		User:                 connection.Connection.Session.User,
		Schema:               connection.Connection.Session.CurrentSchema,
		AcceptedCapabilities: joinClientCapabilities(connection.Negotiation.Accepted),
		Registered:           connection.Registered,
		Supported:            connection.Supported(),
		DiagnosticCodes:      connection.Diagnostics.Codes(),
	}
}

func connectionSummaryCloseRow(closed ClientConnectionCloseExchange) ClientConnectionSummaryRow {
	return ClientConnectionSummaryRow{
		Operation:       ClientConnectionSummaryClose,
		SessionID:       closed.Connection.Session.ID,
		User:            closed.Connection.Session.User,
		Schema:          closed.Connection.Session.CurrentSchema,
		CloseConnection: closed.CloseConnection,
		RemovedSession:  closed.RemovedSession,
		Status:          closed.Response.Status,
		Supported:       closed.Supported(),
		DiagnosticCodes: closed.Diagnostics.Codes(),
	}
}

func cloneClientConnectionExchange(exchange ClientConnectionExchange) ClientConnectionExchange {
	exchange.Request = exchange.Request.Clone()
	exchange.Negotiation = cloneConnectionCapabilityNegotiation(exchange.Negotiation)
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}

func cloneClientConnectionCloseExchange(exchange ClientConnectionCloseExchange) ClientConnectionCloseExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Response = cloneProtocolStatementResponse(exchange.Response)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}
