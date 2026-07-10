package qsbridge

// ClientMutationLifecycleSummaryRow describes one adapter-visible mutation planning stage.
type ClientMutationLifecycleSummaryRow struct {
	Stage           MutationLifecycleStage
	Kind            QueryKind
	Mutation        MutationKind
	AccessIntent    PhysicalAccessIntent
	Target          string
	Complete        bool
	Supported       bool
	LogicalRoot     PlanNodeKind
	PhysicalRoot    PhysicalNodeKind
	Columns         int
	Rows            int
	Assignments     int
	Predicates      int
	ParameterCount  int
	DiagnosticCodes []DiagnosticCode
	Detail          string
}

// ClientMutationLifecycleSummaryExchange is adapter-facing mutation lifecycle metadata.
type ClientMutationLifecycleSummaryExchange struct {
	Connection          ConnectionContext
	Lifecycle           MutationLifecycle
	Rows                []ClientMutationLifecycleSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// PrepareClientMutationLifecycleSummary returns protocol-neutral mutation lifecycle rows.
func (s PlanningService) PrepareClientMutationLifecycleSummary(connection ConnectionContext, lifecycle MutationLifecycle) ClientMutationLifecycleSummaryExchange {
	_ = s
	exchange := ClientMutationLifecycleSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Lifecycle:           cloneMutationLifecycle(lifecycle),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = mutationLifecycleSummaryRows(lifecycle)
	}
	exchange.Result = exchange.mutationLifecycleSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether mutation lifecycle metadata describes a supported mutation.
func (e ClientMutationLifecycleSummaryExchange) Supported() bool {
	return e.Connection.Supported() && e.Lifecycle.Supported && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientMutationLifecycleSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientMutationLifecycleSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientMutationLifecycleSummaryExchange) mutationLifecycleSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     mutationLifecycleSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.mutationLifecycleSummaryResultRows(),
		Final: true,
	})
}

func mutationLifecycleSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Stage", Type: DataTypeString},
		{Name: "Kind", Type: DataTypeString},
		{Name: "Mutation", Type: DataTypeString, Nullable: true},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Target", Type: DataTypeString, Nullable: true},
		{Name: "Complete", Type: DataTypeBool},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Logical_root", Type: DataTypeString, Nullable: true},
		{Name: "Physical_root", Type: DataTypeString, Nullable: true},
		{Name: "Columns", Type: DataTypeInt},
		{Name: "Rows", Type: DataTypeInt},
		{Name: "Assignments", Type: DataTypeInt},
		{Name: "Predicates", Type: DataTypeInt},
		{Name: "Parameters", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientMutationLifecycleSummaryExchange) mutationLifecycleSummaryResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Stage)),
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.Mutation)),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(row.Target),
			metadataBoolCell(row.Complete),
			metadataBoolCell(row.Supported),
			metadataStringCell(string(row.LogicalRoot)),
			metadataStringCell(string(row.PhysicalRoot)),
			metadataIntCell(row.Columns),
			metadataIntCell(row.Rows),
			metadataIntCell(row.Assignments),
			metadataIntCell(row.Predicates),
			metadataIntCell(row.ParameterCount),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}

func mutationLifecycleSummaryRows(lifecycle MutationLifecycle) []ClientMutationLifecycleSummaryRow {
	rows := make([]ClientMutationLifecycleSummaryRow, 0, len(lifecycle.Steps))
	for _, step := range lifecycle.Steps {
		target := step.Target
		if target == "" {
			target = lifecycle.Target
		}
		rows = append(rows, ClientMutationLifecycleSummaryRow{
			Stage:           step.Stage,
			Kind:            lifecycle.Kind,
			Mutation:        lifecycle.Mutation,
			AccessIntent:    lifecycle.AccessIntent,
			Target:          target,
			Complete:        step.Complete,
			Supported:       step.Supported,
			LogicalRoot:     step.LogicalRoot,
			PhysicalRoot:    step.PhysicalRoot,
			Columns:         step.Columns,
			Rows:            step.Rows,
			Assignments:     step.Assignments,
			Predicates:      step.Predicates,
			ParameterCount:  step.ParameterCount,
			DiagnosticCodes: append([]DiagnosticCode(nil), step.Diagnostics...),
			Detail:          step.Detail,
		})
	}
	return rows
}

func cloneMutationLifecycle(lifecycle MutationLifecycle) MutationLifecycle {
	lifecycle.Diagnostics = cloneDiagnosticSet(lifecycle.Diagnostics)
	lifecycle.Columns = append([]string(nil), lifecycle.Columns...)
	lifecycle.Steps = append([]MutationLifecycleStep(nil), lifecycle.Steps...)
	for i := range lifecycle.Steps {
		lifecycle.Steps[i].Diagnostics = append([]DiagnosticCode(nil), lifecycle.Steps[i].Diagnostics...)
	}
	return lifecycle
}
