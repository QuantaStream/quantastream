package qsbridge

// ClientSessionActionSummaryRow describes one session action exchange.
type ClientSessionActionSummaryRow struct {
	SessionID       SessionID
	User            UserName
	BeforeSchema    string
	AfterSchema     string
	BeforeSQLModes  int
	AfterSQLModes   int
	BeforeVariables int
	AfterVariables  int
	Actions         int
	SchemaActions   int
	VariableActions int
	SQLModeActions  int
	TimeZoneActions int
	TransactionActs int
	ResetActions    int
	ChangeUserActs  int
	Applied         bool
	Supported       bool
	DiagnosticCodes []DiagnosticCode
}

// ClientSessionActionSummaryExchange is adapter-facing session action summary metadata.
type ClientSessionActionSummaryExchange struct {
	Connection          ConnectionContext
	Session             ClientSessionActionExchange
	Rows                []ClientSessionActionSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientSessionActions returns compact row metadata for a session action exchange.
func (s PlanningService) SummarizeClientSessionActions(session ClientSessionActionExchange) ClientSessionActionSummaryExchange {
	_ = s
	exchange := ClientSessionActionSummaryExchange{
		Connection:          cloneConnectionContext(session.Connection),
		Session:             cloneClientSessionActionExchange(session),
		ExchangeDiagnostics: cloneDiagnosticSet(session.Connection.Diagnostics),
	}
	if session.Connection.Supported() {
		exchange.Rows = []ClientSessionActionSummaryRow{clientSessionActionSummaryRow(session)}
	}
	exchange.Result = exchange.clientSessionActionSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(session.Connection.Protocol)
	return exchange
}

// Supported reports whether session action summary metadata can be returned.
func (e ClientSessionActionSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientSessionActionSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientSessionActionSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientSessionActionSummaryExchange) clientSessionActionSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     clientSessionActionSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.clientSessionActionSummaryRows(),
		Final: true,
	})
}

func clientSessionActionSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Session_id", Type: DataTypeString, Nullable: true},
		{Name: "User", Type: DataTypeString, Nullable: true},
		{Name: "Before_schema", Type: DataTypeString, Nullable: true},
		{Name: "After_schema", Type: DataTypeString, Nullable: true},
		{Name: "Before_sql_modes", Type: DataTypeInt},
		{Name: "After_sql_modes", Type: DataTypeInt},
		{Name: "Before_variables", Type: DataTypeInt},
		{Name: "After_variables", Type: DataTypeInt},
		{Name: "Actions", Type: DataTypeInt},
		{Name: "Schema_actions", Type: DataTypeInt},
		{Name: "Variable_actions", Type: DataTypeInt},
		{Name: "Sql_mode_actions", Type: DataTypeInt},
		{Name: "Time_zone_actions", Type: DataTypeInt},
		{Name: "Transaction_actions", Type: DataTypeInt},
		{Name: "Reset_actions", Type: DataTypeInt},
		{Name: "Change_user_actions", Type: DataTypeInt},
		{Name: "Applied", Type: DataTypeBool},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientSessionActionSummaryExchange) clientSessionActionSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.SessionID)),
			metadataStringCell(string(row.User)),
			metadataStringCell(row.BeforeSchema),
			metadataStringCell(row.AfterSchema),
			metadataIntCell(row.BeforeSQLModes),
			metadataIntCell(row.AfterSQLModes),
			metadataIntCell(row.BeforeVariables),
			metadataIntCell(row.AfterVariables),
			metadataIntCell(row.Actions),
			metadataIntCell(row.SchemaActions),
			metadataIntCell(row.VariableActions),
			metadataIntCell(row.SQLModeActions),
			metadataIntCell(row.TimeZoneActions),
			metadataIntCell(row.TransactionActs),
			metadataIntCell(row.ResetActions),
			metadataIntCell(row.ChangeUserActs),
			metadataBoolCell(row.Applied),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func clientSessionActionSummaryRow(session ClientSessionActionExchange) ClientSessionActionSummaryRow {
	row := ClientSessionActionSummaryRow{
		SessionID:       session.Transition.Before.ID,
		User:            session.Transition.Before.User,
		BeforeSchema:    session.Transition.Before.CurrentSchema,
		AfterSchema:     session.Transition.After.CurrentSchema,
		BeforeSQLModes:  len(session.Transition.Before.SQLModes),
		AfterSQLModes:   len(session.Transition.After.SQLModes),
		BeforeVariables: len(session.Transition.Before.Variables),
		AfterVariables:  len(session.Transition.After.Variables),
		Actions:         len(session.Transition.Actions),
		Applied:         session.Applied,
		Supported:       session.Supported(),
		DiagnosticCodes: session.Diagnostics.Codes(),
	}
	for _, action := range session.Transition.Actions {
		switch action.Kind {
		case SessionActionUseSchema:
			row.SchemaActions++
		case SessionActionSetVariable:
			row.VariableActions++
		case SessionActionSetSQLMode:
			row.SQLModeActions++
		case SessionActionSetTimeZone:
			row.TimeZoneActions++
		case SessionActionBeginTransaction, SessionActionCommitTransaction, SessionActionRollbackTransaction:
			row.TransactionActs++
		case SessionActionResetConnection:
			row.ResetActions++
		case SessionActionChangeUser:
			row.ChangeUserActs++
		}
	}
	return row
}
