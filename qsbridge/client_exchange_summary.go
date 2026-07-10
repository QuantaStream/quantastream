package qsbridge

// ClientExchangeSummaryRow describes aggregate metadata for one client exchange.
type ClientExchangeSummaryRow struct {
	User                   UserName
	Schema                 string
	Protocol               ProtocolKind
	Supported              bool
	StatementCount         int
	ReadCount              int
	WriteCount             int
	SelectLifecycleCount   int
	MutationLifecycleCount int
	HandoffCount           int
	PreviewCount           int
	ResponseCount          int
	NativeCount            int
	LegacyFallbackCount    int
	RejectedCount          int
	DeniedCount            int
	ProtocolRejectedCount  int
	QueryResponses         int
	StatementResponses     int
	ErrorResponses         int
	DiagnosticCodes        []DiagnosticCode
}

// ClientExchangeSummaryExchange is adapter-facing aggregate exchange metadata.
type ClientExchangeSummaryExchange struct {
	Connection          ConnectionContext
	Exchange            ClientExchange
	Rows                []ClientExchangeSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientExchange returns aggregate row metadata for a client exchange.
func (s PlanningService) SummarizeClientExchange(exchange ClientExchange) ClientExchangeSummaryExchange {
	_ = s
	result := ClientExchangeSummaryExchange{
		Connection:          cloneConnectionContext(exchange.Connection),
		Exchange:            cloneClientExchange(exchange),
		ExchangeDiagnostics: cloneDiagnosticSet(exchange.Connection.Diagnostics),
	}
	if exchange.Connection.Supported() {
		result.Rows = []ClientExchangeSummaryRow{clientExchangeSummaryRow(exchange)}
	}
	result.Result = result.clientExchangeSummaryResult()
	result.ResultSchema = result.Result.ProtocolResultSchema(exchange.Connection.Protocol)
	return result
}

// Supported reports whether exchange summary metadata can be returned.
func (e ClientExchangeSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientExchangeSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange summary error, if any.
func (e ClientExchangeSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientExchangeSummaryExchange) clientExchangeSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     clientExchangeSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.clientExchangeSummaryRows(),
		Final: true,
	})
}

func clientExchangeSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "User", Type: DataTypeString, Nullable: true},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "Protocol", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Statements", Type: DataTypeInt},
		{Name: "Reads", Type: DataTypeInt},
		{Name: "Writes", Type: DataTypeInt},
		{Name: "Select_lifecycle", Type: DataTypeInt},
		{Name: "Mutation_lifecycle", Type: DataTypeInt},
		{Name: "Handoffs", Type: DataTypeInt},
		{Name: "Previews", Type: DataTypeInt},
		{Name: "Responses", Type: DataTypeInt},
		{Name: "Native", Type: DataTypeInt},
		{Name: "Legacy_fallback", Type: DataTypeInt},
		{Name: "Rejected", Type: DataTypeInt},
		{Name: "Denied", Type: DataTypeInt},
		{Name: "Protocol_rejected", Type: DataTypeInt},
		{Name: "Query_responses", Type: DataTypeInt},
		{Name: "Statement_responses", Type: DataTypeInt},
		{Name: "Error_responses", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientExchangeSummaryExchange) clientExchangeSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.User)),
			metadataStringCell(row.Schema),
			metadataStringCell(string(row.Protocol)),
			metadataBoolCell(row.Supported),
			metadataIntCell(row.StatementCount),
			metadataIntCell(row.ReadCount),
			metadataIntCell(row.WriteCount),
			metadataIntCell(row.SelectLifecycleCount),
			metadataIntCell(row.MutationLifecycleCount),
			metadataIntCell(row.HandoffCount),
			metadataIntCell(row.PreviewCount),
			metadataIntCell(row.ResponseCount),
			metadataIntCell(row.NativeCount),
			metadataIntCell(row.LegacyFallbackCount),
			metadataIntCell(row.RejectedCount),
			metadataIntCell(row.DeniedCount),
			metadataIntCell(row.ProtocolRejectedCount),
			metadataIntCell(row.QueryResponses),
			metadataIntCell(row.StatementResponses),
			metadataIntCell(row.ErrorResponses),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func clientExchangeSummaryRow(exchange ClientExchange) ClientExchangeSummaryRow {
	sequence := exchange.ResponseSequence()
	row := ClientExchangeSummaryRow{
		User:            exchange.Connection.Session.User,
		Schema:          exchange.Connection.Session.CurrentSchema,
		Protocol:        exchange.Connection.Protocol.Kind,
		Supported:       exchange.Supported(),
		StatementCount:  len(exchange.Request.Statements),
		HandoffCount:    len(exchange.Handoff.Statements),
		PreviewCount:    len(exchange.Preview.Statements),
		ResponseCount:   len(sequence.Items),
		DiagnosticCodes: exchange.Diagnostics.Codes(),
	}
	for _, statement := range exchange.Handoff.Statements {
		switch statement.Plan.Prepared.AccessIntent() {
		case PhysicalAccessRead:
			row.ReadCount++
		case PhysicalAccessWrite:
			row.WriteCount++
		}
		switch clientPlanLifecycleKind(statement.Plan.Prepared.Kind) {
		case ClientPlanLifecycleSelect:
			row.SelectLifecycleCount++
		case ClientPlanLifecycleMutation:
			row.MutationLifecycleCount++
		}
		switch statement.Handoff.Outcome().Kind {
		case ExecutionHandoffNative:
			row.NativeCount++
		case ExecutionHandoffLegacyFallback:
			row.LegacyFallbackCount++
		case ExecutionHandoffRejected:
			row.RejectedCount++
		case ExecutionHandoffDenied:
			row.DeniedCount++
		case ExecutionHandoffProtocolRejected:
			row.ProtocolRejectedCount++
		}
	}
	for _, item := range sequence.Items {
		switch item.Kind {
		case ClientResponseQuery:
			row.QueryResponses++
		case ClientResponseStatement:
			row.StatementResponses++
		case ClientResponseError:
			row.ErrorResponses++
		}
	}
	return row
}

func cloneClientExchange(exchange ClientExchange) ClientExchange {
	return ClientExchange{
		Request:     cloneClientStatementBundle(exchange.Request),
		Connection:  cloneConnectionContext(exchange.Connection),
		Handoff:     cloneClientHandoffBundle(exchange.Handoff),
		Preview:     cloneClientResultPreviewBundle(exchange.Preview),
		Diagnostics: cloneDiagnosticSet(exchange.Diagnostics),
	}
}

func cloneClientHandoffBundle(bundle ClientHandoffBundle) ClientHandoffBundle {
	bundle.Connection = cloneConnectionContext(bundle.Connection)
	bundle.Diagnostics = cloneDiagnosticSet(bundle.Diagnostics)
	if len(bundle.Statements) > 0 {
		statements := make([]ClientStatementHandoff, 0, len(bundle.Statements))
		for _, statement := range bundle.Statements {
			statements = append(statements, cloneClientStatementHandoff(statement))
		}
		bundle.Statements = statements
	}
	return bundle
}

func cloneClientStatementHandoff(statement ClientStatementHandoff) ClientStatementHandoff {
	return ClientStatementHandoff{
		Statement: statement.Statement,
		Plan:      cloneClientStatementPlan(statement.Plan),
		Options:   statement.Options.clone(),
		Handoff:   cloneProtocolRoutedAuthorizedExecutionRequest(statement.Handoff),
	}
}

func cloneProtocolRoutedAuthorizedExecutionRequest(request ProtocolRoutedAuthorizedExecutionRequest) ProtocolRoutedAuthorizedExecutionRequest {
	return ProtocolRoutedAuthorizedExecutionRequest{
		Prepared:      clonePreparedPlan(request.Prepared),
		Request:       cloneExecutionRequest(request.Request),
		Protocol:      cloneProtocolNegotiation(request.Protocol),
		Route:         cloneRouteDecision(request.Route),
		Authorization: cloneAuthorizationDecision(request.Authorization),
	}
}

func cloneAuthorizationDecision(decision AuthorizationDecision) AuthorizationDecision {
	decision.Requirements = cloneAccessRequirements(decision.Requirements)
	decision.Diagnostics = cloneDiagnosticSet(decision.Diagnostics)
	return decision
}
