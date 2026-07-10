package qsbridge

// ClientCharsetSummaryExchange is adapter-facing metadata for aggregate character set and collation metadata.
type ClientCharsetSummaryExchange struct {
	Connection   ConnectionContext
	Pattern      string
	Row          ClientCharsetSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientCharsetMetadata returns aggregate adapter-supplied character set and collation metadata.
func (s PlanningService) SummarizeClientCharsetMetadata(connection ConnectionContext, characterSets []ClientCharacterSet, collations []ClientCollation, pattern string) ClientCharsetSummaryExchange {
	_ = s
	exchange := ClientCharsetSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Row = summarizeClientCharsetMetadata(
			filterClientCharacterSets(characterSets, pattern),
			filterClientCollations(collations, pattern),
		)
	}
	exchange.Result = exchange.charsetSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether character set summary metadata can be returned.
func (e ClientCharsetSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts character set summary diagnostics into protocol-facing errors.
func (e ClientCharsetSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking character set summary error, if any.
func (e ClientCharsetSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCharsetSummaryExchange) charsetSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     charsetSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{charsetSummaryResultRow(e.Row)},
		Final: true,
	})
}

func charsetSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Character_set_count", Type: DataTypeInt},
		{Name: "Collation_count", Type: DataTypeInt},
		{Name: "Default_collation_count", Type: DataTypeInt},
		{Name: "Compiled_collation_count", Type: DataTypeInt},
		{Name: "Multi_byte_charset_count", Type: DataTypeInt},
		{Name: "Zero_sortlen_count", Type: DataTypeInt},
	}
}

func charsetSummaryResultRow(row ClientCharsetSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.CharacterSetCount),
		metadataIntCell(row.CollationCount),
		metadataIntCell(row.DefaultCollationCount),
		metadataIntCell(row.CompiledCollationCount),
		metadataIntCell(row.MultiByteCharsetCount),
		metadataIntCell(row.ZeroSortLenCount),
	}
}
