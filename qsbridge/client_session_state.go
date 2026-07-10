package qsbridge

// ClientSessionStateKind identifies a protocol-visible session state category.
type ClientSessionStateKind string

const (
	// ClientSessionStateSchema records a current-schema/database change.
	ClientSessionStateSchema ClientSessionStateKind = "schema"
	// ClientSessionStateSystemVariable records a system/session variable change.
	ClientSessionStateSystemVariable ClientSessionStateKind = "system_variable"
	// ClientSessionStateTransaction records transaction state metadata.
	ClientSessionStateTransaction ClientSessionStateKind = "transaction_state"
	// ClientSessionStateGeneral records a general session state marker.
	ClientSessionStateGeneral ClientSessionStateKind = "state_change"
)

// ClientSessionStateChange describes one adapter-visible session state change.
type ClientSessionStateChange struct {
	Kind  ClientSessionStateKind
	Name  string
	Value string
}

// ClientSessionStateSummaryRow describes aggregate session-state metadata.
type ClientSessionStateSummaryRow struct {
	ChangeCount          int
	SchemaChangeCount    int
	VariableChangeCount  int
	TransactionCount     int
	ResetConnectionCount int
	ChangeUserCount      int
}

// ClientSessionStateExchange is adapter-facing metadata for session-state tracking.
type ClientSessionStateExchange struct {
	Connection   ConnectionContext
	Response     ProtocolStatementResponse
	Changes      []ClientSessionStateChange
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientSessionStateChanges returns protocol-neutral rows for response session actions.
func (s PlanningService) ListClientSessionStateChanges(connection ConnectionContext, response ProtocolStatementResponse) ClientSessionStateExchange {
	_ = s
	exchange := ClientSessionStateExchange{
		Connection:  cloneConnectionContext(connection),
		Response:    cloneProtocolStatementResponse(response),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, response.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.sessionStateResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, validateClientSessionActions(connection.Protocol, response.SessionActions))
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Changes = sessionStateChanges(response.SessionActions)
	}
	exchange.Result = exchange.sessionStateResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether session-state metadata can be returned.
func (e ClientSessionStateExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts session-state diagnostics into protocol-facing errors.
func (e ClientSessionStateExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking session-state error, if any.
func (e ClientSessionStateExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientSessionStateExchange) sessionStateResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     sessionStateResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.sessionStateRows(),
		Final: true,
	})
}

func sessionStateResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Type", Type: DataTypeString},
		{Name: "Name", Type: DataTypeString, Nullable: true},
		{Name: "Value", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientSessionStateExchange) sessionStateRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Changes))
	for _, change := range e.Changes {
		rows = append(rows, ResultRow{
			metadataStringCell(string(change.Kind)),
			metadataStringCell(change.Name),
			metadataStringCell(change.Value),
		})
	}
	return rows
}

func sessionStateChanges(actions []SessionAction) []ClientSessionStateChange {
	if len(actions) == 0 {
		return nil
	}
	changes := make([]ClientSessionStateChange, 0, len(actions))
	for _, action := range actions {
		switch action.Kind {
		case SessionActionUseSchema:
			changes = append(changes, ClientSessionStateChange{Kind: ClientSessionStateSchema, Name: "database", Value: action.Value})
		case SessionActionSetVariable:
			changes = append(changes, ClientSessionStateChange{Kind: ClientSessionStateSystemVariable, Name: action.Name, Value: action.Value})
		case SessionActionSetSQLMode:
			changes = append(changes, ClientSessionStateChange{Kind: ClientSessionStateSystemVariable, Name: "sql_mode", Value: action.Value})
		case SessionActionSetTimeZone:
			changes = append(changes, ClientSessionStateChange{Kind: ClientSessionStateSystemVariable, Name: "time_zone", Value: action.Value})
		case SessionActionBeginTransaction:
			changes = append(changes, ClientSessionStateChange{Kind: ClientSessionStateTransaction, Name: "transaction", Value: "BEGIN"})
		case SessionActionCommitTransaction:
			changes = append(changes, ClientSessionStateChange{Kind: ClientSessionStateTransaction, Name: "transaction", Value: "COMMIT"})
		case SessionActionRollbackTransaction:
			changes = append(changes, ClientSessionStateChange{Kind: ClientSessionStateTransaction, Name: "transaction", Value: "ROLLBACK"})
		case SessionActionResetConnection:
			changes = append(changes, ClientSessionStateChange{Kind: ClientSessionStateGeneral, Name: "reset_connection", Value: "1"})
		case SessionActionChangeUser:
			changes = append(changes, ClientSessionStateChange{Kind: ClientSessionStateGeneral, Name: "change_user", Value: action.Value})
		}
	}
	return changes
}

func summarizeSessionStateChanges(changes []ClientSessionStateChange) ClientSessionStateSummaryRow {
	summary := ClientSessionStateSummaryRow{ChangeCount: len(changes)}
	for _, change := range changes {
		switch change.Kind {
		case ClientSessionStateSchema:
			summary.SchemaChangeCount++
		case ClientSessionStateSystemVariable:
			summary.VariableChangeCount++
		case ClientSessionStateTransaction:
			summary.TransactionCount++
		case ClientSessionStateGeneral:
			switch change.Name {
			case "reset_connection":
				summary.ResetConnectionCount++
			case "change_user":
				summary.ChangeUserCount++
			}
		}
	}
	return summary
}
