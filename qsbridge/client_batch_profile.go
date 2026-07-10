package qsbridge

// ClientBatchProfileRow describes one adapter-visible batch item explain/profile row.
type ClientBatchProfileRow struct {
	Item           int
	RequestID      ExecutionRequestID
	AccessIntent   PhysicalAccessIntent
	Lifecycle      ClientPlanLifecycleKind
	LifecycleSteps int
	Section        string
	Name           string
	Value          ResultCell
	Unit           string
	Detail         string
}

// ClientBatchProfileSummaryRow describes aggregate batch explain/profile metadata.
type ClientBatchProfileSummaryRow struct {
	ItemCount              int
	ProfiledItems          int
	RowCount               int
	ReadIntentItems        int
	WriteIntentItems       int
	SelectLifecycleItems   int
	MutationLifecycleItems int
	ExplainCount           int
	TimingCount            int
	CounterCount           int
	DiagnosticCount        int
}

// ClientBatchExecutionProfileExchange is adapter-facing batch explain/profile metadata.
type ClientBatchExecutionProfileExchange struct {
	Connection   ConnectionContext
	Batch        BatchExecutionResult
	Rows         []ClientBatchProfileRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// PrepareClientBatchExecutionProfile returns profile rows for each batch item result.
func (s PlanningService) PrepareClientBatchExecutionProfile(connection ConnectionContext, batch BatchExecutionResult) ClientBatchExecutionProfileExchange {
	_ = s
	exchange := ClientBatchExecutionProfileExchange{
		Connection:  cloneConnectionContext(connection),
		Batch:       cloneBatchExecutionResult(batch),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), batchProfileDiagnostics(batch)),
	}
	if connection.Supported() {
		exchange.Rows = batchExecutionProfileRows(batch)
	}
	exchange.Result = exchange.batchExecutionProfileResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch profile metadata can be returned.
func (e ClientBatchExecutionProfileExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch profile diagnostics into protocol-facing errors.
func (e ClientBatchExecutionProfileExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch profile error, if any.
func (e ClientBatchExecutionProfileExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientBatchExecutionProfileExchange) batchExecutionProfileResult() ExecutionResult {
	result := ExecutionResult{
		RequestID:   e.Batch.RequestID,
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchExecutionProfileResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.batchExecutionProfileResultRows(),
		Final: true,
	})
}

func batchExecutionProfileResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Item", Type: DataTypeInt},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Section", Type: DataTypeString},
		{Name: "Name", Type: DataTypeString},
		{Name: "Value", Type: DataTypeString, Nullable: true},
		{Name: "Unit", Type: DataTypeString, Nullable: true},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientBatchExecutionProfileExchange) batchExecutionProfileResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Item),
			metadataStringCell(string(row.RequestID)),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataStringCell(row.Section),
			metadataStringCell(row.Name),
			row.Value,
			metadataStringCell(row.Unit),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}

func batchExecutionProfileRows(batch BatchExecutionResult) []ClientBatchProfileRow {
	if len(batch.Items) == 0 {
		return nil
	}
	rows := make([]ClientBatchProfileRow, 0, len(batch.Items))
	for itemIndex, item := range batch.Items {
		for _, profileRow := range executionProfileRows(item.Profile) {
			rows = append(rows, ClientBatchProfileRow{
				Item:           itemIndex,
				RequestID:      item.RequestID,
				AccessIntent:   item.Profile.AccessIntent,
				Lifecycle:      item.Profile.Lifecycle,
				LifecycleSteps: item.Profile.LifecycleSteps,
				Section:        profileRow.Section,
				Name:           profileRow.Name,
				Value:          cloneResultCell(profileRow.Value),
				Unit:           profileRow.Unit,
				Detail:         profileRow.Detail,
			})
		}
	}
	return rows
}

func summarizeBatchExecutionProfileRows(batch BatchExecutionResult, rows []ClientBatchProfileRow) ClientBatchProfileSummaryRow {
	summary := ClientBatchProfileSummaryRow{
		ItemCount: len(batch.Items),
		RowCount:  len(rows),
	}
	profiledItems := make(map[int]struct{}, len(batch.Items))
	readItems := make(map[int]struct{}, len(batch.Items))
	writeItems := make(map[int]struct{}, len(batch.Items))
	selectItems := make(map[int]struct{}, len(batch.Items))
	mutationItems := make(map[int]struct{}, len(batch.Items))
	for _, row := range rows {
		profiledItems[row.Item] = struct{}{}
		switch row.AccessIntent {
		case PhysicalAccessRead:
			readItems[row.Item] = struct{}{}
		case PhysicalAccessWrite:
			writeItems[row.Item] = struct{}{}
		}
		switch row.Lifecycle {
		case ClientPlanLifecycleSelect:
			selectItems[row.Item] = struct{}{}
		case ClientPlanLifecycleMutation:
			mutationItems[row.Item] = struct{}{}
		}
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
	summary.ProfiledItems = len(profiledItems)
	summary.ReadIntentItems = len(readItems)
	summary.WriteIntentItems = len(writeItems)
	summary.SelectLifecycleItems = len(selectItems)
	summary.MutationLifecycleItems = len(mutationItems)
	return summary
}

func batchProfileDiagnostics(batch BatchExecutionResult) DiagnosticSet {
	diagnostics := cloneDiagnosticSet(batch.Diagnostics)
	for _, item := range batch.Items {
		diagnostics = mergeDiagnosticSets(diagnostics, item.Profile.Diagnostics)
	}
	return diagnostics
}

func cloneResultCell(cell ResultCell) ResultCell {
	return cell
}
