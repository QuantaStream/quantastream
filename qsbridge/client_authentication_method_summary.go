package qsbridge

// ClientAuthenticationMethodSummaryExchange is adapter-facing authentication method summary metadata.
type ClientAuthenticationMethodSummaryExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Row          ClientAuthenticationMethodSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientAuthenticationMethods returns aggregate authentication method metadata.
func (s PlanningService) SummarizeClientAuthenticationMethods(connection ConnectionContext, methods []ClientAuthenticationMethod, pattern string) ClientAuthenticationMethodSummaryExchange {
	_ = s
	exchange := ClientAuthenticationMethodSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeClientAuthenticationMethods(filterClientAuthenticationMethods(methods, pattern))
	}
	exchange.Result = exchange.authenticationMethodSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether authentication method summary metadata can be returned.
func (e ClientAuthenticationMethodSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts authentication method summary diagnostics into protocol-facing errors.
func (e ClientAuthenticationMethodSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking authentication method summary error, if any.
func (e ClientAuthenticationMethodSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientAuthenticationMethodSummaryExchange) authenticationMethodSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     authenticationMethodSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{authenticationMethodSummaryResultRow(e.Row)},
		Final: true,
	})
}

func authenticationMethodSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Method_count", Type: DataTypeInt},
		{Name: "Default_count", Type: DataTypeInt},
		{Name: "Enabled_count", Type: DataTypeInt},
		{Name: "Disabled_count", Type: DataTypeInt},
		{Name: "Password_exchange_count", Type: DataTypeInt},
		{Name: "Token_exchange_count", Type: DataTypeInt},
		{Name: "External_identity_count", Type: DataTypeInt},
		{Name: "Mysql_password_count", Type: DataTypeInt},
		{Name: "Jwt_count", Type: DataTypeInt},
		{Name: "Oauth_count", Type: DataTypeInt},
	}
}

func authenticationMethodSummaryResultRow(row ClientAuthenticationMethodSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.MethodCount),
		metadataIntCell(row.DefaultCount),
		metadataIntCell(row.EnabledCount),
		metadataIntCell(row.DisabledCount),
		metadataIntCell(row.PasswordExchangeCount),
		metadataIntCell(row.TokenExchangeCount),
		metadataIntCell(row.ExternalIdentityCount),
		metadataIntCell(row.MySQLPasswordCount),
		metadataIntCell(row.JWTCount),
		metadataIntCell(row.OAuthCount),
	}
}
