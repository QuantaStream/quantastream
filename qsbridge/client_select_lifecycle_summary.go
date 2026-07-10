package qsbridge

// ClientSelectLifecycleSummaryRow describes one adapter-visible SELECT planning stage.
type ClientSelectLifecycleSummaryRow struct {
	Stage           SelectLifecycleStage
	Kind            QueryKind
	Complete        bool
	Supported       bool
	LogicalRoot     PlanNodeKind
	PhysicalRoot    PhysicalNodeKind
	RequiredFields  int
	ResultColumns   int
	NativeBlockers  int
	CapabilityCount int
	DiagnosticCodes []DiagnosticCode
	Detail          string
}

// ClientSelectLifecycleSummaryExchange is adapter-facing SELECT lifecycle metadata.
type ClientSelectLifecycleSummaryExchange struct {
	Connection          ConnectionContext
	Lifecycle           SimpleSelectLifecycle
	Rows                []ClientSelectLifecycleSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// PrepareClientSelectLifecycleSummary returns protocol-neutral SELECT lifecycle rows.
func (s PlanningService) PrepareClientSelectLifecycleSummary(connection ConnectionContext, lifecycle SimpleSelectLifecycle) ClientSelectLifecycleSummaryExchange {
	_ = s
	exchange := ClientSelectLifecycleSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Lifecycle:           cloneSimpleSelectLifecycle(lifecycle),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = selectLifecycleSummaryRows(lifecycle)
	}
	exchange.Result = exchange.selectLifecycleSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether SELECT lifecycle metadata describes a supported SELECT.
func (e ClientSelectLifecycleSummaryExchange) Supported() bool {
	return e.Connection.Supported() && e.Lifecycle.Supported && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientSelectLifecycleSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientSelectLifecycleSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientSelectLifecycleSummaryExchange) selectLifecycleSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     selectLifecycleSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.selectLifecycleSummaryResultRows(),
		Final: true,
	})
}

func selectLifecycleSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Stage", Type: DataTypeString},
		{Name: "Kind", Type: DataTypeString},
		{Name: "Complete", Type: DataTypeBool},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Logical_root", Type: DataTypeString, Nullable: true},
		{Name: "Physical_root", Type: DataTypeString, Nullable: true},
		{Name: "Required_fields", Type: DataTypeInt},
		{Name: "Result_columns", Type: DataTypeInt},
		{Name: "Native_blockers", Type: DataTypeInt},
		{Name: "Capabilities", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientSelectLifecycleSummaryExchange) selectLifecycleSummaryResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Stage)),
			metadataStringCell(string(row.Kind)),
			metadataBoolCell(row.Complete),
			metadataBoolCell(row.Supported),
			metadataStringCell(string(row.LogicalRoot)),
			metadataStringCell(string(row.PhysicalRoot)),
			metadataIntCell(row.RequiredFields),
			metadataIntCell(row.ResultColumns),
			metadataIntCell(row.NativeBlockers),
			metadataIntCell(row.CapabilityCount),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}

func selectLifecycleSummaryRows(lifecycle SimpleSelectLifecycle) []ClientSelectLifecycleSummaryRow {
	rows := make([]ClientSelectLifecycleSummaryRow, 0, len(lifecycle.Steps))
	for _, step := range lifecycle.Steps {
		rows = append(rows, ClientSelectLifecycleSummaryRow{
			Stage:           step.Stage,
			Kind:            lifecycle.Kind,
			Complete:        step.Complete,
			Supported:       step.Supported,
			LogicalRoot:     step.LogicalRoot,
			PhysicalRoot:    step.PhysicalRoot,
			RequiredFields:  step.RequiredFields,
			ResultColumns:   step.ResultColumns,
			NativeBlockers:  step.NativeBlockers,
			CapabilityCount: step.CapabilityCount,
			DiagnosticCodes: append([]DiagnosticCode(nil), step.Diagnostics...),
			Detail:          step.Detail,
		})
	}
	return rows
}

func cloneSimpleSelectLifecycle(lifecycle SimpleSelectLifecycle) SimpleSelectLifecycle {
	lifecycle.Diagnostics = cloneDiagnosticSet(lifecycle.Diagnostics)
	lifecycle.Sources = append([]string(nil), lifecycle.Sources...)
	lifecycle.RequiredFields = append([]string(nil), lifecycle.RequiredFields...)
	lifecycle.ResultColumns = append([]ResultColumn(nil), lifecycle.ResultColumns...)
	lifecycle.Steps = append([]SelectLifecycleStep(nil), lifecycle.Steps...)
	for i := range lifecycle.Steps {
		lifecycle.Steps[i].Diagnostics = append([]DiagnosticCode(nil), lifecycle.Steps[i].Diagnostics...)
	}
	return lifecycle
}
