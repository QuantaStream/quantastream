package qsbridge

// ClientBatchSessionStateChange describes one batch item session-state change.
type ClientBatchSessionStateChange struct {
	Item      int
	RequestID ExecutionRequestID
	Kind      ClientSessionStateKind
	Name      string
	Value     string
}

// ClientBatchSessionStateSummaryRow describes aggregate batch session-state metadata.
type ClientBatchSessionStateSummaryRow struct {
	ItemCount            int
	ChangedItemCount     int
	ChangeCount          int
	SchemaChangeCount    int
	VariableChangeCount  int
	TransactionCount     int
	ResetConnectionCount int
	ChangeUserCount      int
}

// ClientBatchSessionStateExchange is adapter-facing metadata for batch session tracking.
type ClientBatchSessionStateExchange struct {
	Connection   ConnectionContext
	Batch        BatchExecutionResult
	Changes      []ClientBatchSessionStateChange
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientBatchSessionStateChanges returns protocol-neutral rows for batch item session actions.
func (s PlanningService) ListClientBatchSessionStateChanges(connection ConnectionContext, batch BatchExecutionResult) ClientBatchSessionStateExchange {
	_ = s
	exchange := ClientBatchSessionStateExchange{
		Connection:  cloneConnectionContext(connection),
		Batch:       cloneBatchExecutionResult(batch),
		Diagnostics: mergeDiagnosticSets(connection.Diagnostics, batch.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.batchSessionStateResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, validateBatchSessionActions(connection.Protocol, batch))
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Changes = batchSessionStateChanges(batch)
	}
	exchange.Result = exchange.batchSessionStateResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether batch session-state metadata can be returned.
func (e ClientBatchSessionStateExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts batch session-state diagnostics into protocol-facing errors.
func (e ClientBatchSessionStateExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking batch session-state error, if any.
func (e ClientBatchSessionStateExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientBatchSessionStateExchange) batchSessionStateResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     batchSessionStateResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.batchSessionStateRows(),
		Final: true,
	})
}

func batchSessionStateResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Item", Type: DataTypeInt},
		{Name: "Request_id", Type: DataTypeString, Nullable: true},
		{Name: "Type", Type: DataTypeString},
		{Name: "Name", Type: DataTypeString, Nullable: true},
		{Name: "Value", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientBatchSessionStateExchange) batchSessionStateRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Changes))
	for _, change := range e.Changes {
		rows = append(rows, ResultRow{
			metadataIntCell(change.Item),
			metadataStringCell(string(change.RequestID)),
			metadataStringCell(string(change.Kind)),
			metadataStringCell(change.Name),
			metadataStringCell(change.Value),
		})
	}
	return rows
}

func validateBatchSessionActions(profile ProtocolProfile, batch BatchExecutionResult) DiagnosticSet {
	var diagnostics DiagnosticSet
	for _, item := range batch.Items {
		diagnostics = mergeDiagnosticSets(diagnostics, validateClientSessionActions(profile, batchItemSessionActions(item)))
	}
	return diagnostics
}

func batchSessionStateChanges(batch BatchExecutionResult) []ClientBatchSessionStateChange {
	if len(batch.Items) == 0 {
		return nil
	}
	changes := make([]ClientBatchSessionStateChange, 0)
	for itemIndex, item := range batch.Items {
		for _, change := range sessionStateChanges(batchItemSessionActions(item)) {
			changes = append(changes, ClientBatchSessionStateChange{
				Item:      itemIndex,
				RequestID: item.RequestID,
				Kind:      change.Kind,
				Name:      change.Name,
				Value:     change.Value,
			})
		}
	}
	return changes
}

func summarizeBatchSessionStateChanges(batch BatchExecutionResult, changes []ClientBatchSessionStateChange) ClientBatchSessionStateSummaryRow {
	summary := ClientBatchSessionStateSummaryRow{
		ItemCount:   len(batch.Items),
		ChangeCount: len(changes),
	}
	changedItems := make(map[int]struct{}, len(batch.Items))
	for _, change := range changes {
		changedItems[change.Item] = struct{}{}
		switch change.Kind {
		case ClientSessionStateSchema:
			summary.SchemaChangeCount++
		case ClientSessionStateSystemVariable:
			summary.VariableChangeCount++
		case ClientSessionStateTransaction:
			summary.TransactionCount++
		case ClientSessionStateGeneral:
			switch change.Name {
			case "reset_connection":
				summary.ResetConnectionCount++
			case "change_user":
				summary.ChangeUserCount++
			}
		}
	}
	summary.ChangedItemCount = len(changedItems)
	return summary
}

func batchItemSessionActions(item ExecutionResult) []SessionAction {
	if len(item.SessionActions) > 0 {
		return item.SessionActions
	}
	return item.Statement.SessionActions
}
