package qsbridge

// ClientPreparedLongDataRow describes one accumulated prepared long-data parameter.
type ClientPreparedLongDataRow struct {
	StatementID   PreparedStatementID
	StatementName string
	Parameter     string
	Kind          ValueKind
	Chunks        uint64
	TotalBytes    uint64
	Final         bool
}

// ClientPreparedLongDataStatusSummaryRow describes aggregate prepared long-data inventory metadata.
type ClientPreparedLongDataStatusSummaryRow struct {
	StateCount             int
	NamedStatementCount    int
	FinalStateCount        int
	StringKindCount        int
	TotalChunks            uint64
	TotalBytes             uint64
	LargestStateBytes      uint64
	DistinctStatementCount int
}

// ClientPreparedLongDataStatusExchange is adapter-facing long-data inventory metadata.
type ClientPreparedLongDataStatusExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Rows                []ClientPreparedLongDataRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientPreparedLongData returns adapter-owned prepared long-data states as rows.
func (s PlanningService) ListClientPreparedLongData(connection ConnectionContext, registry PreparedLongDataRegistry) ClientPreparedLongDataStatusExchange {
	_ = s
	exchange := ClientPreparedLongDataStatusExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
		exchange.Result = exchange.preparedLongDataStatusResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if registry == nil {
		exchange.ExchangeDiagnostics = mergeDiagnosticSets(exchange.ExchangeDiagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared long-data registry is not configured"),
		})
	} else {
		exchange.Rows = preparedLongDataRows(registry.List())
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.preparedLongDataStatusResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepared long-data inventory metadata can be returned.
func (e ClientPreparedLongDataStatusExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientPreparedLongDataStatusExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientPreparedLongDataStatusExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientPreparedLongDataStatusExchange) preparedLongDataStatusResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedLongDataStatusResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.preparedLongDataStatusRows(),
		Final: true,
	})
}

func preparedLongDataStatusResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Statement_id", Type: DataTypeInt},
		{Name: "Statement_name", Type: DataTypeString, Nullable: true},
		{Name: "Parameter", Type: DataTypeString},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Chunks", Type: DataTypeInt},
		{Name: "Total_bytes", Type: DataTypeInt},
		{Name: "Final", Type: DataTypeBool},
	}
}

func (e ClientPreparedLongDataStatusExchange) preparedLongDataStatusRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(int(row.StatementID)),
			metadataStringCell(row.StatementName),
			metadataStringCell(row.Parameter),
			metadataStringCell(string(row.Kind)),
			metadataIntCell(int(row.Chunks)),
			metadataIntCell(int(row.TotalBytes)),
			metadataBoolCell(row.Final),
		})
	}
	return rows
}

func summarizePreparedLongDataRows(rows []ClientPreparedLongDataRow) ClientPreparedLongDataStatusSummaryRow {
	summary := ClientPreparedLongDataStatusSummaryRow{StateCount: len(rows)}
	statements := make(map[struct {
		id   PreparedStatementID
		name string
	}]struct{})
	for _, row := range rows {
		if row.StatementName != "" {
			summary.NamedStatementCount++
		}
		if row.StatementID != 0 || row.StatementName != "" {
			statements[struct {
				id   PreparedStatementID
				name string
			}{id: row.StatementID, name: row.StatementName}] = struct{}{}
		}
		if row.Final {
			summary.FinalStateCount++
		}
		if row.Kind == ValueString {
			summary.StringKindCount++
		}
		summary.TotalChunks += row.Chunks
		summary.TotalBytes += row.TotalBytes
		if row.TotalBytes > summary.LargestStateBytes {
			summary.LargestStateBytes = row.TotalBytes
		}
	}
	summary.DistinctStatementCount = len(statements)
	return summary
}

func preparedLongDataRows(states []PreparedLongDataState) []ClientPreparedLongDataRow {
	if len(states) == 0 {
		return nil
	}
	rows := make([]ClientPreparedLongDataRow, 0, len(states))
	for _, state := range states {
		rows = append(rows, ClientPreparedLongDataRow{
			StatementID:   state.Handle.ID,
			StatementName: state.Handle.Name,
			Parameter:     parameterValueLabel(state.Parameter),
			Kind:          state.Parameter.Kind,
			Chunks:        state.Chunks,
			TotalBytes:    state.TotalBytes,
			Final:         state.Final,
		})
	}
	return rows
}
