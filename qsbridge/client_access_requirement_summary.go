package qsbridge

// ClientAccessRequirementSummaryExchange is adapter-facing access requirement summary metadata.
type ClientAccessRequirementSummaryExchange struct {
	Connection          ConnectionContext
	Prepared            PreparedPlan
	Diagnostics         DiagnosticSet
	Row                 ClientAccessRequirementSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientAccessRequirements returns aggregate authorization requirements for one prepared plan.
func (s PlanningService) SummarizeClientAccessRequirements(connection ConnectionContext, prepared PreparedPlan) ClientAccessRequirementSummaryExchange {
	_ = s
	exchange := ClientAccessRequirementSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Prepared:            clonePreparedPlan(prepared),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeAccessRequirementRows(accessRequirementRows(prepared.RequiredAccess()))
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.accessRequirementSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether access requirement summary metadata can be returned.
func (e ClientAccessRequirementSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientAccessRequirementSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientAccessRequirementSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientAccessRequirementSummaryExchange) accessRequirementSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     accessRequirementSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{accessRequirementSummaryResultRow(e.Row)},
		Final: true,
	})
}

func accessRequirementSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Requirement_count", Type: DataTypeInt},
		{Name: "Select_count", Type: DataTypeInt},
		{Name: "Insert_count", Type: DataTypeInt},
		{Name: "Update_count", Type: DataTypeInt},
		{Name: "Delete_count", Type: DataTypeInt},
		{Name: "Table_count", Type: DataTypeInt},
		{Name: "Field_count", Type: DataTypeInt},
		{Name: "Has_mutation", Type: DataTypeBool},
	}
}

func accessRequirementSummaryResultRow(row ClientAccessRequirementSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.RequirementCount),
		metadataIntCell(row.SelectCount),
		metadataIntCell(row.InsertCount),
		metadataIntCell(row.UpdateCount),
		metadataIntCell(row.DeleteCount),
		metadataIntCell(row.TableCount),
		metadataIntCell(row.FieldCount),
		metadataBoolCell(row.HasMutation),
	}
}
