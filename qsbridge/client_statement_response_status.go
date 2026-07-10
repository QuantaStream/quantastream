package qsbridge

// ClientStatementResponseStatus describes adapter-visible OK/status metadata.
type ClientStatementResponseStatus struct {
	AffectedRows   uint64
	LastInsertID   uint64
	Warnings       uint16
	Status         string
	SessionActions int
	Transaction    bool
	Flags          []ProtocolStatusFlag
	Diagnostics    []DiagnosticCode
}

// ClientStatementResponseStatusExchange is adapter-facing statement response metadata.
type ClientStatementResponseStatusExchange struct {
	Connection   ConnectionContext
	Response     ProtocolStatementResponse
	Status       ClientStatementResponseStatus
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientStatementResponseStatus returns compact metadata for one statement response.
func (s PlanningService) ListClientStatementResponseStatus(connection ConnectionContext, response ProtocolStatementResponse) ClientStatementResponseStatusExchange {
	_ = s
	diagnostics := mergeDiagnosticSets(connection.Diagnostics, response.Diagnostics)
	exchange := ClientStatementResponseStatusExchange{
		Connection:  cloneConnectionContext(connection),
		Response:    cloneProtocolStatementResponse(response),
		Status:      statementResponseStatus(response),
		Diagnostics: diagnostics,
	}
	exchange.Result = exchange.statementResponseStatusResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether statement response status metadata can be returned.
func (e ClientStatementResponseStatusExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts statement response status diagnostics into protocol-facing errors.
func (e ClientStatementResponseStatusExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking statement response status error, if any.
func (e ClientStatementResponseStatusExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientStatementResponseStatusExchange) statementResponseStatusResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     statementResponseStatusResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{e.statementResponseStatusRow()},
		Final: true,
	})
}

func statementResponseStatusResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Affected_rows", Type: DataTypeInt},
		{Name: "Last_insert_id", Type: DataTypeInt},
		{Name: "Warnings", Type: DataTypeInt},
		{Name: "Status", Type: DataTypeString, Nullable: true},
		{Name: "Session_actions", Type: DataTypeInt},
		{Name: "Transaction", Type: DataTypeBool},
		{Name: "Flags", Type: DataTypeString, Nullable: true},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientStatementResponseStatusExchange) statementResponseStatusRow() ResultRow {
	status := e.Status
	return ResultRow{
		metadataIntCell(int(status.AffectedRows)),
		metadataIntCell(int(status.LastInsertID)),
		metadataIntCell(int(status.Warnings)),
		metadataStringCell(status.Status),
		metadataIntCell(status.SessionActions),
		metadataBoolCell(status.Transaction),
		metadataStringCell(joinProtocolStatusFlags(status.Flags)),
		metadataStringCell(joinDiagnosticCodes(status.Diagnostics)),
	}
}

func statementResponseStatus(response ProtocolStatementResponse) ClientStatementResponseStatus {
	return ClientStatementResponseStatus{
		AffectedRows:   response.AffectedRows,
		LastInsertID:   response.LastInsertID,
		Warnings:       response.Warnings,
		Status:         response.Status,
		SessionActions: len(response.SessionActions),
		Transaction:    hasTransactionAction(response.SessionActions),
		Flags:          append([]ProtocolStatusFlag(nil), response.Flags...),
		Diagnostics:    response.Diagnostics.Codes(),
	}
}

func joinProtocolStatusFlags(flags []ProtocolStatusFlag) string {
	values := make([]string, 0, len(flags))
	for _, flag := range flags {
		values = append(values, string(flag))
	}
	return joinStringValues(values)
}
