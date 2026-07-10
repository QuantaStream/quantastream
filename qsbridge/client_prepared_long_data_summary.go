package qsbridge

// ClientPreparedLongDataSummaryRow describes one prepared long-data store exchange.
type ClientPreparedLongDataSummaryRow struct {
	StatementID     PreparedStatementID
	StatementName   string
	Parameter       string
	Kind            ValueKind
	ChunkBytes      uint64
	FinalFragment   bool
	Stored          bool
	StateChunks     uint64
	StateTotalBytes uint64
	StateFinal      bool
	Supported       bool
	DiagnosticCodes []DiagnosticCode
}

// ClientPreparedLongDataSummaryExchange is adapter-facing long-data store metadata.
type ClientPreparedLongDataSummaryExchange struct {
	Connection          ConnectionContext
	LongData            ClientPreparedLongDataExchange
	Rows                []ClientPreparedLongDataSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientPreparedLongData returns row metadata for one long-data store exchange.
func (s PlanningService) SummarizeClientPreparedLongData(connection ConnectionContext, longData ClientPreparedLongDataExchange) ClientPreparedLongDataSummaryExchange {
	_ = s
	exchange := ClientPreparedLongDataSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		LongData:            cloneClientPreparedLongDataExchange(longData),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = []ClientPreparedLongDataSummaryRow{preparedLongDataSummaryRow(longData)}
	}
	exchange.Result = exchange.preparedLongDataSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepared long-data summary metadata can be returned.
func (e ClientPreparedLongDataSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientPreparedLongDataSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientPreparedLongDataSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientPreparedLongDataSummaryExchange) preparedLongDataSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedLongDataSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.preparedLongDataSummaryRows(),
		Final: true,
	})
}

func preparedLongDataSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Statement_id", Type: DataTypeInt},
		{Name: "Statement_name", Type: DataTypeString, Nullable: true},
		{Name: "Parameter", Type: DataTypeString},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Chunk_bytes", Type: DataTypeInt},
		{Name: "Final_fragment", Type: DataTypeBool},
		{Name: "Stored", Type: DataTypeBool},
		{Name: "State_chunks", Type: DataTypeInt},
		{Name: "State_total_bytes", Type: DataTypeInt},
		{Name: "State_final", Type: DataTypeBool},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPreparedLongDataSummaryExchange) preparedLongDataSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(int(row.StatementID)),
			metadataStringCell(row.StatementName),
			metadataStringCell(row.Parameter),
			metadataStringCell(string(row.Kind)),
			metadataIntCell(int(row.ChunkBytes)),
			metadataBoolCell(row.FinalFragment),
			metadataBoolCell(row.Stored),
			metadataIntCell(int(row.StateChunks)),
			metadataIntCell(int(row.StateTotalBytes)),
			metadataBoolCell(row.StateFinal),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func preparedLongDataSummaryRow(longData ClientPreparedLongDataExchange) ClientPreparedLongDataSummaryRow {
	return ClientPreparedLongDataSummaryRow{
		StatementID:     longData.Fragment.Handle.ID,
		StatementName:   longData.Fragment.Handle.Name,
		Parameter:       parameterValueLabel(longData.Fragment.Parameter),
		Kind:            longData.Fragment.Parameter.Kind,
		ChunkBytes:      longData.Fragment.ChunkBytes,
		FinalFragment:   longData.Fragment.Final,
		Stored:          longData.Stored,
		StateChunks:     longData.State.Chunks,
		StateTotalBytes: longData.State.TotalBytes,
		StateFinal:      longData.State.Final,
		Supported:       longData.Supported(),
		DiagnosticCodes: longData.Diagnostics.Codes(),
	}
}

func cloneClientPreparedLongDataExchange(exchange ClientPreparedLongDataExchange) ClientPreparedLongDataExchange {
	exchange.Connection = cloneConnectionContext(exchange.Connection)
	exchange.Fragment = clonePreparedLongDataFragment(exchange.Fragment)
	exchange.Prepared = clonePreparedPlan(exchange.Prepared)
	exchange.State = clonePreparedLongDataState(exchange.State)
	exchange.Diagnostics = cloneDiagnosticSet(exchange.Diagnostics)
	return exchange
}
