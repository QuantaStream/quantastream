package qsbridge

// ClientSessionStatusRow describes one adapter-visible session metadata row.
type ClientSessionStatusRow struct {
	SessionID SessionID
	User      UserName
	Schema    string
	TimeZone  string
	Roles     []RoleName
	SQLModes  []SQLMode
	Variables int
}

// ClientSessionStatusSummaryRow describes aggregate session inventory metadata.
type ClientSessionStatusSummaryRow struct {
	SessionCount          int
	SchemaSelectedCount   int
	TimeZoneCount         int
	RoleCount             int
	SQLModeCount          int
	VariableCount         int
	SessionsWithRoles     int
	SessionsWithSQLModes  int
	SessionsWithVariables int
	DistinctUserCount     int
	DistinctSchemaCount   int
}

// ClientSessionStatusExchange is adapter-facing session inventory metadata.
type ClientSessionStatusExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Rows                []ClientSessionStatusRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientSessions returns adapter-owned session metadata as rows.
func (s PlanningService) ListClientSessions(connection ConnectionContext, registry SessionRegistry) ClientSessionStatusExchange {
	_ = s
	exchange := ClientSessionStatusExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
		exchange.Result = exchange.sessionStatusResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if registry == nil {
		exchange.ExchangeDiagnostics = mergeDiagnosticSets(exchange.ExchangeDiagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "session registry is not configured"),
		})
	} else {
		exchange.Rows = sessionStatusRows(registry.List())
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.sessionStatusResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether session inventory metadata can be returned.
func (e ClientSessionStatusExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientSessionStatusExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientSessionStatusExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientSessionStatusExchange) sessionStatusResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     sessionStatusResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.sessionStatusResultRows(),
		Final: true,
	})
}

func sessionStatusResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Session_id", Type: DataTypeString},
		{Name: "User", Type: DataTypeString},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "Time_zone", Type: DataTypeString, Nullable: true},
		{Name: "Roles", Type: DataTypeString, Nullable: true},
		{Name: "SQL_modes", Type: DataTypeString, Nullable: true},
		{Name: "Variables", Type: DataTypeInt},
	}
}

func (e ClientSessionStatusExchange) sessionStatusResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.SessionID)),
			metadataStringCell(string(row.User)),
			metadataStringCell(row.Schema),
			metadataStringCell(row.TimeZone),
			metadataStringCell(joinRoleNames(row.Roles)),
			metadataStringCell(joinSessionStatusSQLModes(row.SQLModes)),
			metadataIntCell(row.Variables),
		})
	}
	return rows
}

func sessionStatusRows(sessions []SessionContext) []ClientSessionStatusRow {
	if len(sessions) == 0 {
		return nil
	}
	rows := make([]ClientSessionStatusRow, 0, len(sessions))
	for _, session := range sessions {
		rows = append(rows, ClientSessionStatusRow{
			SessionID: session.ID,
			User:      session.User,
			Schema:    session.CurrentSchema,
			TimeZone:  session.TimeZone,
			Roles:     append([]RoleName(nil), session.Roles...),
			SQLModes:  append([]SQLMode(nil), session.SQLModes...),
			Variables: len(session.Variables),
		})
	}
	return rows
}

func summarizeSessionStatusRows(rows []ClientSessionStatusRow) ClientSessionStatusSummaryRow {
	summary := ClientSessionStatusSummaryRow{SessionCount: len(rows)}
	users := make(map[UserName]struct{})
	schemas := make(map[string]struct{})
	for _, row := range rows {
		if row.User != "" {
			users[row.User] = struct{}{}
		}
		if row.Schema != "" {
			summary.SchemaSelectedCount++
			schemas[row.Schema] = struct{}{}
		}
		if row.TimeZone != "" {
			summary.TimeZoneCount++
		}
		summary.RoleCount += len(row.Roles)
		summary.SQLModeCount += len(row.SQLModes)
		summary.VariableCount += row.Variables
		if len(row.Roles) > 0 {
			summary.SessionsWithRoles++
		}
		if len(row.SQLModes) > 0 {
			summary.SessionsWithSQLModes++
		}
		if row.Variables > 0 {
			summary.SessionsWithVariables++
		}
	}
	summary.DistinctUserCount = len(users)
	summary.DistinctSchemaCount = len(schemas)
	return summary
}

func joinRoleNames(roles []RoleName) string {
	if len(roles) == 0 {
		return ""
	}
	values := make([]string, 0, len(roles))
	for _, role := range roles {
		values = append(values, string(role))
	}
	return joinStringValues(values)
}

func joinSessionStatusSQLModes(modes []SQLMode) string {
	if len(modes) == 0 {
		return ""
	}
	values := make([]string, 0, len(modes))
	for _, mode := range modes {
		values = append(values, string(mode))
	}
	return joinStringValues(values)
}
