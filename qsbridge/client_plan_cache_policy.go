package qsbridge

// ClientPlanCachePolicyRow describes one prepared-plan cache identity policy factor.
type ClientPlanCachePolicyRow struct {
	Factor        PlanCacheFactor
	Participation PlanCacheParticipation
	Included      bool
	Reason        string
}

// ClientPlanCachePolicyExchange is adapter-facing prepared-plan cache policy metadata.
type ClientPlanCachePolicyExchange struct {
	Connection   ConnectionContext
	Policies     []PlanCacheKeyPolicy
	Rows         []ClientPlanCachePolicyRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientPlanCachePolicy returns the prepared-plan cache identity policy.
func (s PlanningService) ListClientPlanCachePolicy(connection ConnectionContext) ClientPlanCachePolicyExchange {
	_ = s
	policies := DefaultPlanCacheKeyPolicy()
	exchange := ClientPlanCachePolicyExchange{
		Connection:  cloneConnectionContext(connection),
		Policies:    clonePlanCacheKeyPolicies(policies),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = planCachePolicyRows(policies)
	}
	exchange.Result = exchange.planCachePolicyResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether plan-cache policy metadata can be returned.
func (e ClientPlanCachePolicyExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts plan-cache policy diagnostics into protocol-facing errors.
func (e ClientPlanCachePolicyExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking plan-cache policy error, if any.
func (e ClientPlanCachePolicyExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientPlanCachePolicyExchange) planCachePolicyResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     planCachePolicyResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.planCachePolicyResultRows(),
		Final: true,
	})
}

func planCachePolicyResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Factor", Type: DataTypeString},
		{Name: "Participation", Type: DataTypeString},
		{Name: "Included", Type: DataTypeBool},
		{Name: "Reason", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPlanCachePolicyExchange) planCachePolicyResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Factor)),
			metadataStringCell(string(row.Participation)),
			metadataBoolCell(row.Included),
			metadataStringCell(row.Reason),
		})
	}
	return rows
}

func planCachePolicyRows(policies []PlanCacheKeyPolicy) []ClientPlanCachePolicyRow {
	rows := make([]ClientPlanCachePolicyRow, 0, len(policies))
	for _, policy := range policies {
		rows = append(rows, ClientPlanCachePolicyRow{
			Factor:        policy.Factor,
			Participation: policy.Participation,
			Included:      policy.Participation == PlanCacheParticipationIncluded,
			Reason:        policy.Reason,
		})
	}
	return rows
}
