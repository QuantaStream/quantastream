package qsbridge

// ClientRouteDecisionRow describes one adapter-visible final routing decision.
type ClientRouteDecisionRow struct {
	Ordinal           int
	SQL               string
	Handoff           ExecutionHandoffKind
	Supported         bool
	AccessIntent      PhysicalAccessIntent
	Lifecycle         ClientPlanLifecycleKind
	LifecycleSteps    int
	Route             RouteKind
	RouteReason       RouteReason
	NativeEligible    bool
	ProtocolMode      ProtocolExecutionMode
	ProtocolSupported bool
	Authorized        bool
	Diagnostics       []DiagnosticCode
}

// ClientRouteDecisionExchange is adapter-facing route decision metadata.
type ClientRouteDecisionExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Rows                []ClientRouteDecisionRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientRouteDecisions returns protocol-neutral rows for a client handoff bundle.
func (s PlanningService) ListClientRouteDecisions(bundle ClientHandoffBundle) ClientRouteDecisionExchange {
	_ = s
	exchange := ClientRouteDecisionExchange{
		Connection:          cloneConnectionContext(bundle.Connection),
		Diagnostics:         cloneDiagnosticSet(bundle.Diagnostics),
		ExchangeDiagnostics: cloneDiagnosticSet(bundle.Connection.Diagnostics),
	}
	if bundle.Connection.Supported() {
		exchange.Rows = clientRouteDecisionRows(bundle)
	}
	exchange.Result = exchange.routeDecisionResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(bundle.Connection.Protocol)
	return exchange
}

// Supported reports whether route-decision metadata can be returned.
func (e ClientRouteDecisionExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientRouteDecisionExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientRouteDecisionExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientRouteDecisionExchange) routeDecisionResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     routeDecisionResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.routeDecisionResultRows(),
		Final: true,
	})
}

func routeDecisionResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "SQL", Type: DataTypeString},
		{Name: "Handoff", Type: DataTypeString},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Route", Type: DataTypeString},
		{Name: "Route_reason", Type: DataTypeString},
		{Name: "Native_eligible", Type: DataTypeBool},
		{Name: "Protocol_mode", Type: DataTypeString},
		{Name: "Protocol_supported", Type: DataTypeBool},
		{Name: "Authorized", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientRouteDecisionExchange) routeDecisionResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Ordinal),
			metadataStringCell(row.SQL),
			metadataStringCell(string(row.Handoff)),
			metadataBoolCell(row.Supported),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataStringCell(string(row.Route)),
			metadataStringCell(string(row.RouteReason)),
			metadataBoolCell(row.NativeEligible),
			metadataStringCell(string(row.ProtocolMode)),
			metadataBoolCell(row.ProtocolSupported),
			metadataBoolCell(row.Authorized),
			metadataStringCell(joinDiagnosticCodes(row.Diagnostics)),
		})
	}
	return rows
}

func clientRouteDecisionRows(bundle ClientHandoffBundle) []ClientRouteDecisionRow {
	if len(bundle.Statements) == 0 {
		return nil
	}
	rows := make([]ClientRouteDecisionRow, 0, len(bundle.Statements))
	for _, statement := range bundle.Statements {
		diagnostics := statement.Handoff.Diagnostics()
		rows = append(rows, ClientRouteDecisionRow{
			Ordinal:           statement.Statement.Ordinal,
			SQL:               statement.Statement.SQL,
			Handoff:           statement.Handoff.HandoffKind(),
			Supported:         statement.Handoff.Supported(),
			AccessIntent:      statement.Plan.Prepared.AccessIntent(),
			Lifecycle:         clientPlanLifecycleKind(statement.Plan.Prepared.Kind),
			LifecycleSteps:    clientPlanLifecycleStepCount(statement.Plan.Prepared.Kind),
			Route:             statement.Handoff.Route.Kind,
			RouteReason:       statement.Handoff.Route.Reason,
			NativeEligible:    statement.Handoff.Route.NativeEligible,
			ProtocolMode:      statement.Handoff.Protocol.Mode,
			ProtocolSupported: statement.Handoff.Protocol.Supported(),
			Authorized:        statement.Handoff.Authorization.Supported(),
			Diagnostics:       append([]DiagnosticCode(nil), diagnosticCodes(diagnostics)...),
		})
	}
	return rows
}
