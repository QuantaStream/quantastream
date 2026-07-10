package qsbridge

// ClientSessionStatusSummaryExchange is adapter-facing session inventory summary metadata.
type ClientSessionStatusSummaryExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Row                 ClientSessionStatusSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientSessions returns aggregate adapter-owned session metadata.
func (s PlanningService) SummarizeClientSessions(connection ConnectionContext, registry SessionRegistry) ClientSessionStatusSummaryExchange {
	_ = s
	exchange := ClientSessionStatusSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
		exchange.Result = exchange.sessionStatusSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if registry == nil {
		exchange.ExchangeDiagnostics = mergeDiagnosticSets(exchange.ExchangeDiagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "session registry is not configured"),
		})
	} else {
		exchange.Row = summarizeSessionStatusRows(sessionStatusRows(registry.List()))
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.sessionStatusSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether session inventory summary metadata can be returned.
func (e ClientSessionStatusSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientSessionStatusSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientSessionStatusSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientSessionStatusSummaryExchange) sessionStatusSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     sessionStatusSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{sessionStatusSummaryResultRow(e.Row)},
		Final: true,
	})
}

func sessionStatusSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Session_count", Type: DataTypeInt},
		{Name: "Schema_selected_count", Type: DataTypeInt},
		{Name: "Time_zone_count", Type: DataTypeInt},
		{Name: "Role_count", Type: DataTypeInt},
		{Name: "SQL_mode_count", Type: DataTypeInt},
		{Name: "Variable_count", Type: DataTypeInt},
		{Name: "Sessions_with_roles", Type: DataTypeInt},
		{Name: "Sessions_with_sql_modes", Type: DataTypeInt},
		{Name: "Sessions_with_variables", Type: DataTypeInt},
		{Name: "Distinct_user_count", Type: DataTypeInt},
		{Name: "Distinct_schema_count", Type: DataTypeInt},
	}
}

func sessionStatusSummaryResultRow(row ClientSessionStatusSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.SessionCount),
		metadataIntCell(row.SchemaSelectedCount),
		metadataIntCell(row.TimeZoneCount),
		metadataIntCell(row.RoleCount),
		metadataIntCell(row.SQLModeCount),
		metadataIntCell(row.VariableCount),
		metadataIntCell(row.SessionsWithRoles),
		metadataIntCell(row.SessionsWithSQLModes),
		metadataIntCell(row.SessionsWithVariables),
		metadataIntCell(row.DistinctUserCount),
		metadataIntCell(row.DistinctSchemaCount),
	}
}
