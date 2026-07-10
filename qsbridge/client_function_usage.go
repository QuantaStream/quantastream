package qsbridge

// ClientFunctionUsageRow describes one bound function occurrence for explain tooling.
type ClientFunctionUsageRow struct {
	Ordinal       int
	Name          string
	Origin        FunctionOrigin
	Placement     FunctionPlacement
	Context       FunctionUsageContext
	ReturnType    DataType
	Deterministic bool
}

// ClientFunctionUsageExchange is adapter-facing function usage metadata.
type ClientFunctionUsageExchange struct {
	Connection          ConnectionContext
	Prepared            PreparedPlan
	Rows                []ClientFunctionUsageRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientFunctionUsages returns bound function occurrences for one prepared plan.
func (s PlanningService) ListClientFunctionUsages(connection ConnectionContext, prepared PreparedPlan) ClientFunctionUsageExchange {
	_ = s
	exchange := ClientFunctionUsageExchange{
		Connection:          cloneConnectionContext(connection),
		Prepared:            clonePreparedPlan(prepared),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = clientFunctionUsageRows(prepared.Query.FunctionUsages())
	}
	exchange.Result = exchange.functionUsageResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether function usage metadata can be returned.
func (e ClientFunctionUsageExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientFunctionUsageExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientFunctionUsageExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientFunctionUsageExchange) functionUsageResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     functionUsageResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.functionUsageResultRows(),
		Final: true,
	})
}

func functionUsageResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "Name", Type: DataTypeString},
		{Name: "Origin", Type: DataTypeString, Nullable: true},
		{Name: "Placement", Type: DataTypeString, Nullable: true},
		{Name: "Context", Type: DataTypeString},
		{Name: "Return_type", Type: DataTypeString, Nullable: true},
		{Name: "Deterministic", Type: DataTypeBool},
	}
}

func (e ClientFunctionUsageExchange) functionUsageResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Ordinal),
			metadataStringCell(row.Name),
			metadataStringCell(string(row.Origin)),
			metadataStringCell(string(row.Placement)),
			metadataStringCell(string(row.Context)),
			metadataStringCell(string(row.ReturnType)),
			metadataBoolCell(row.Deterministic),
		})
	}
	return rows
}

func clientFunctionUsageRows(usages []FunctionUsage) []ClientFunctionUsageRow {
	if len(usages) == 0 {
		return nil
	}
	rows := make([]ClientFunctionUsageRow, 0, len(usages))
	for index, usage := range usages {
		rows = append(rows, ClientFunctionUsageRow{
			Ordinal:       index + 1,
			Name:          usage.Name,
			Origin:        usage.Origin,
			Placement:     usage.Placement,
			Context:       usage.Context,
			ReturnType:    usage.ReturnType,
			Deterministic: usage.Deterministic,
		})
	}
	return rows
}
