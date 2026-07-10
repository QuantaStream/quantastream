package qsbridge

// ClientProfileRow describes one adapter-visible explain/profile row.
type ClientProfileRow struct {
	Section string
	Name    string
	Value   ResultCell
	Unit    string
	Detail  string
}

// ClientExecutionProfileSummaryRow describes aggregate execution profile metadata.
type ClientExecutionProfileSummaryRow struct {
	RowCount        int
	AccessIntent    PhysicalAccessIntent
	Lifecycle       ClientPlanLifecycleKind
	LifecycleSteps  int
	ExplainCount    int
	TimingCount     int
	CounterCount    int
	DiagnosticCount int
	TraceExplain    bool
	IncludeProfile  bool
}

// ClientExecutionProfileExchange is adapter-facing explain/profile metadata.
type ClientExecutionProfileExchange struct {
	Connection   ConnectionContext
	Profile      ExecutionProfile
	Rows         []ClientProfileRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// PrepareClientExecutionProfile returns protocol-neutral explain/profile rows.
func (s PlanningService) PrepareClientExecutionProfile(connection ConnectionContext, profile ExecutionProfile) ClientExecutionProfileExchange {
	_ = s
	exchange := ClientExecutionProfileExchange{
		Connection:  cloneConnectionContext(connection),
		Profile:     cloneExecutionProfile(profile),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), profile.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = executionProfileRows(profile)
	}
	exchange.Result = exchange.executionProfileResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether profile metadata can be returned.
func (e ClientExecutionProfileExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts profile diagnostics into protocol-facing errors.
func (e ClientExecutionProfileExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking profile error, if any.
func (e ClientExecutionProfileExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientExecutionProfileExchange) executionProfileResult() ExecutionResult {
	result := ExecutionResult{
		RequestID:   e.Profile.RequestID,
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     executionProfileResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
		Profile:     cloneExecutionProfile(e.Profile),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.executionProfileResultRows(),
		Final: true,
	})
}

func executionProfileResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Section", Type: DataTypeString},
		{Name: "Name", Type: DataTypeString},
		{Name: "Value", Type: DataTypeString, Nullable: true},
		{Name: "Unit", Type: DataTypeString, Nullable: true},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientExecutionProfileExchange) executionProfileResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(row.Section),
			metadataStringCell(row.Name),
			row.Value,
			metadataStringCell(row.Unit),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}

func executionProfileRows(profile ExecutionProfile) []ClientProfileRow {
	rows := make([]ClientProfileRow, 0, 2+len(profile.Timings)+len(profile.Counters)+len(profile.Diagnostics))
	if profile.LogicalPlan != "" {
		rows = append(rows, ClientProfileRow{
			Section: "explain",
			Name:    "logical",
			Value:   metadataStringCell(profile.LogicalPlan),
		})
	}
	if profile.PhysicalPlan != "" {
		rows = append(rows, ClientProfileRow{
			Section: "explain",
			Name:    "physical",
			Value:   metadataStringCell(profile.PhysicalPlan),
		})
	}
	for _, timing := range profile.Timings {
		rows = append(rows, ClientProfileRow{
			Section: "timing",
			Name:    timing.Name,
			Value:   profileInt64Cell(timing.Elapsed),
			Unit:    timing.Unit,
		})
	}
	for _, counter := range profile.Counters {
		rows = append(rows, ClientProfileRow{
			Section: "counter",
			Name:    counter.Name,
			Value:   profileUint64Cell(counter.Value),
			Unit:    counter.Unit,
		})
	}
	for _, diagnostic := range profile.Diagnostics {
		rows = append(rows, ClientProfileRow{
			Section: "diagnostic",
			Name:    string(diagnostic.Code),
			Value:   metadataStringCell(string(diagnostic.Severity)),
			Unit:    string(diagnostic.Phase),
			Detail:  diagnostic.Message,
		})
	}
	return rows
}

func profileInt64Cell(value int64) ResultCell {
	return ResultCell{Kind: ValueInt, Value: value}
}

func profileUint64Cell(value uint64) ResultCell {
	return ResultCell{Kind: ValueInt, Value: value}
}

func summarizeExecutionProfileRows(profile ExecutionProfile, rows []ClientProfileRow) ClientExecutionProfileSummaryRow {
	summary := ClientExecutionProfileSummaryRow{
		RowCount:       len(rows),
		AccessIntent:   profile.AccessIntent,
		Lifecycle:      profile.Lifecycle,
		LifecycleSteps: profile.LifecycleSteps,
		TraceExplain:   profile.TraceExplain,
		IncludeProfile: profile.IncludeProfile,
	}
	for _, row := range rows {
		switch row.Section {
		case "explain":
			summary.ExplainCount++
		case "timing":
			summary.TimingCount++
		case "counter":
			summary.CounterCount++
		case "diagnostic":
			summary.DiagnosticCount++
		}
	}
	return summary
}
