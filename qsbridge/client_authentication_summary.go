package qsbridge

// ClientAuthenticationSummaryRow describes one authentication request and decision.
type ClientAuthenticationSummaryRow struct {
	SessionID       SessionID
	Method          AuthenticationMethod
	RequestedUser   UserName
	DefaultSchema   string
	ClientAddress   string
	Authenticated   bool
	PrincipalUser   UserName
	PrincipalRoles  []RoleName
	AttributeCount  int
	DiagnosticCodes []DiagnosticCode
}

// ClientAuthenticationSummaryExchange is adapter-facing authentication decision metadata.
type ClientAuthenticationSummaryExchange struct {
	ConnectionDiagnostics DiagnosticSet
	Request               AuthenticationRequest
	Decision              AuthenticationDecision
	Rows                  []ClientAuthenticationSummaryRow
	Result                ExecutionResult
	ResultSchema          ProtocolResultSchema
}

// SummarizeClientAuthentication returns protocol-neutral rows for one authentication decision.
func (s PlanningService) SummarizeClientAuthentication(protocol ProtocolProfile, request AuthenticationRequest, decision AuthenticationDecision) ClientAuthenticationSummaryExchange {
	_ = s
	exchange := ClientAuthenticationSummaryExchange{
		Request:  request.Clone(),
		Decision: cloneAuthenticationDecision(decision),
		Rows:     []ClientAuthenticationSummaryRow{authenticationSummaryRow(request, decision)},
	}
	exchange.Result = exchange.authenticationSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(protocol)
	return exchange
}

// Supported reports whether authentication succeeded.
func (e ClientAuthenticationSummaryExchange) Supported() bool {
	return e.Decision.Supported() && !e.ConnectionDiagnostics.BlocksNative()
}

// ProtocolErrors converts authentication diagnostics into protocol-facing errors.
func (e ClientAuthenticationSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Decision.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking authentication error, if any.
func (e ClientAuthenticationSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Decision.Diagnostics.FirstProtocolError()
}

func (e ClientAuthenticationSummaryExchange) authenticationSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     authenticationSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Decision.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.authenticationSummaryRows(),
		Final: true,
	})
}

func authenticationSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Session_id", Type: DataTypeString, Nullable: true},
		{Name: "Method", Type: DataTypeString, Nullable: true},
		{Name: "Requested_user", Type: DataTypeString, Nullable: true},
		{Name: "Default_schema", Type: DataTypeString, Nullable: true},
		{Name: "Client_address", Type: DataTypeString, Nullable: true},
		{Name: "Authenticated", Type: DataTypeBool},
		{Name: "Principal_user", Type: DataTypeString, Nullable: true},
		{Name: "Principal_roles", Type: DataTypeString, Nullable: true},
		{Name: "Attribute_count", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientAuthenticationSummaryExchange) authenticationSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.SessionID)),
			metadataStringCell(string(row.Method)),
			metadataStringCell(string(row.RequestedUser)),
			metadataStringCell(row.DefaultSchema),
			metadataStringCell(row.ClientAddress),
			metadataBoolCell(row.Authenticated),
			metadataStringCell(string(row.PrincipalUser)),
			metadataStringCell(joinRoleNames(row.PrincipalRoles)),
			metadataIntCell(row.AttributeCount),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func authenticationSummaryRow(request AuthenticationRequest, decision AuthenticationDecision) ClientAuthenticationSummaryRow {
	return ClientAuthenticationSummaryRow{
		SessionID:       request.SessionID,
		Method:          request.Method,
		RequestedUser:   request.User,
		DefaultSchema:   request.DefaultSchema,
		ClientAddress:   request.ClientAddress,
		Authenticated:   decision.Authenticated,
		PrincipalUser:   decision.Principal.User,
		PrincipalRoles:  append([]RoleName(nil), decision.Principal.Roles...),
		AttributeCount:  len(decision.Principal.Attributes),
		DiagnosticCodes: decision.Diagnostics.Codes(),
	}
}

func cloneAuthenticationDecision(decision AuthenticationDecision) AuthenticationDecision {
	decision.Principal = cloneAuthenticationPrincipal(decision.Principal)
	decision.Diagnostics = cloneDiagnosticSet(decision.Diagnostics)
	return decision
}
