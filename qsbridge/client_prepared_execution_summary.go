package qsbridge

// ClientPreparedExecutionSummaryOperation identifies a prepared execution summary row.
type ClientPreparedExecutionSummaryOperation string

const (
	// ClientPreparedExecutionSingle identifies single prepared execution metadata.
	ClientPreparedExecutionSingle ClientPreparedExecutionSummaryOperation = "execute"
	// ClientPreparedExecutionBatch identifies batch prepared execution metadata.
	ClientPreparedExecutionBatch ClientPreparedExecutionSummaryOperation = "batch_execute"
)

// ClientPreparedExecutionSummaryRow describes one prepared execution exchange.
type ClientPreparedExecutionSummaryRow struct {
	Operation       ClientPreparedExecutionSummaryOperation
	StatementID     PreparedStatementID
	StatementName   string
	Kind            QueryKind
	AccessIntent    PhysicalAccessIntent
	Lifecycle       ClientPlanLifecycleKind
	LifecycleSteps  int
	Handoff         ExecutionHandoffKind
	ResponseKind    ClientResponseKind
	ResultStatus    ExecutionStatus
	Bindings        int
	ParameterSets   int
	Supported       bool
	DiagnosticCodes []DiagnosticCode
}

// ClientPreparedExecutionSummaryExchange is adapter-facing prepared execution metadata.
type ClientPreparedExecutionSummaryExchange struct {
	Connection          ConnectionContext
	Rows                []ClientPreparedExecutionSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientPreparedExecution returns row metadata for one prepared execution exchange.
func (s PlanningService) SummarizeClientPreparedExecution(connection ConnectionContext, executed ClientPreparedExecutionExchange) ClientPreparedExecutionSummaryExchange {
	_ = s
	exchange := ClientPreparedExecutionSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientPreparedExecutionSummaryRow{preparedExecutionSummaryRow(executed)}
	}
	exchange.Result = exchange.preparedExecutionSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// SummarizeClientPreparedBatchExecution returns row metadata for one prepared batch execution exchange.
func (s PlanningService) SummarizeClientPreparedBatchExecution(connection ConnectionContext, executed ClientPreparedBatchExecutionExchange) ClientPreparedExecutionSummaryExchange {
	_ = s
	exchange := ClientPreparedExecutionSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientPreparedExecutionSummaryRow{preparedBatchExecutionSummaryRow(executed)}
	}
	exchange.Result = exchange.preparedExecutionSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepared execution summary metadata can be returned.
func (e ClientPreparedExecutionSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientPreparedExecutionSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientPreparedExecutionSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientPreparedExecutionSummaryExchange) preparedExecutionSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedExecutionSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.preparedExecutionSummaryRows(),
		Final: true,
	})
}

func preparedExecutionSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Operation", Type: DataTypeString},
		{Name: "Statement_id", Type: DataTypeInt},
		{Name: "Statement_name", Type: DataTypeString, Nullable: true},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Handoff", Type: DataTypeString, Nullable: true},
		{Name: "Response_kind", Type: DataTypeString, Nullable: true},
		{Name: "Result_status", Type: DataTypeString, Nullable: true},
		{Name: "Bindings", Type: DataTypeInt},
		{Name: "Parameter_sets", Type: DataTypeInt},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPreparedExecutionSummaryExchange) preparedExecutionSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Operation)),
			metadataIntCell(int(row.StatementID)),
			metadataStringCell(row.StatementName),
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataStringCell(string(row.Handoff)),
			metadataStringCell(string(row.ResponseKind)),
			metadataStringCell(string(row.ResultStatus)),
			metadataIntCell(row.Bindings),
			metadataIntCell(row.ParameterSets),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func preparedExecutionSummaryRow(executed ClientPreparedExecutionExchange) ClientPreparedExecutionSummaryRow {
	return ClientPreparedExecutionSummaryRow{
		Operation:       ClientPreparedExecutionSingle,
		StatementID:     executed.Handle.ID,
		StatementName:   executed.Handle.Name,
		Kind:            executed.Prepared.Kind,
		AccessIntent:    executed.Prepared.AccessIntent(),
		Lifecycle:       clientPlanLifecycleKind(executed.Prepared.Kind),
		LifecycleSteps:  clientPlanLifecycleStepCount(executed.Prepared.Kind),
		Handoff:         executed.Handoff.HandoffKind(),
		ResponseKind:    executed.Response.Kind,
		ResultStatus:    executed.Response.Result.Status,
		Bindings:        len(executed.Handoff.Request.Bound.Parameters.Bindings),
		ParameterSets:   1,
		Supported:       executed.Supported(),
		DiagnosticCodes: executed.Diagnostics.Codes(),
	}
}

func preparedBatchExecutionSummaryRow(executed ClientPreparedBatchExecutionExchange) ClientPreparedExecutionSummaryRow {
	return ClientPreparedExecutionSummaryRow{
		Operation:       ClientPreparedExecutionBatch,
		StatementID:     executed.Handle.ID,
		StatementName:   executed.Handle.Name,
		Kind:            executed.Prepared.Kind,
		AccessIntent:    executed.Prepared.AccessIntent(),
		Lifecycle:       clientPlanLifecycleKind(executed.Prepared.Kind),
		LifecycleSteps:  clientPlanLifecycleStepCount(executed.Prepared.Kind),
		Handoff:         executed.Handoff.HandoffKind(),
		ResultStatus:    executed.Result.Status,
		ParameterSets:   len(executed.Handoff.Request.ParameterSets),
		Supported:       executed.Supported(),
		DiagnosticCodes: executed.Diagnostics.Codes(),
	}
}
