package qsbridge

// ClientTableStatusSummaryExchange is client-facing table status summary metadata.
type ClientTableStatusSummaryExchange struct {
	Connection   ConnectionContext
	Schema       string
	Pattern      string
	Row          ClientTableStatusSummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientTableStatus returns SHOW TABLE STATUS-style aggregate catalog traits.
func (s PlanningService) SummarizeClientTableStatus(connection ConnectionContext, catalog CatalogMetadata, schema string, pattern string) ClientTableStatusSummaryExchange {
	_ = s
	schema = effectiveClientMetadataSchema(connection, schema)
	exchange := ClientTableStatusSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Schema:      schema,
		Pattern:     pattern,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if catalog == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, catalogMetadataUnsupportedDiagnostics())
		exchange.Result = exchange.tableStatusSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if schema == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "table status metadata requires a schema name or selected schema"),
		})
		exchange.Result = exchange.tableStatusSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	tables, diagnostics := catalog.ListTables(schema)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Row = summarizeTableStatusRows(tableStatusRows(tables, pattern))
	}
	exchange.Result = exchange.tableStatusSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether table status summary metadata can be returned.
func (e ClientTableStatusSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts table status summary diagnostics into protocol-facing errors.
func (e ClientTableStatusSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking table status summary error, if any.
func (e ClientTableStatusSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientTableStatusSummaryExchange) tableStatusSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     tableStatusSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{tableStatusSummaryResultRow(e.Row)},
		Final: true,
	})
}

func tableStatusSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Table_count", Type: DataTypeInt},
		{Name: "Partitioned_count", Type: DataTypeInt},
		{Name: "Searchable_count", Type: DataTypeInt},
		{Name: "Replicated_count", Type: DataTypeInt},
		{Name: "Field_count", Type: DataTypeInt},
		{Name: "Relationship_count", Type: DataTypeInt},
		{Name: "Distinct_engine_count", Type: DataTypeInt},
		{Name: "Distinct_index_count", Type: DataTypeInt},
	}
}

func tableStatusSummaryResultRow(row ClientTableStatusSummary) ResultRow {
	return ResultRow{
		metadataIntCell(row.TableCount),
		metadataIntCell(row.PartitionedCount),
		metadataIntCell(row.SearchableCount),
		metadataIntCell(row.ReplicatedCount),
		metadataIntCell(row.FieldCount),
		metadataIntCell(row.RelationshipCount),
		metadataIntCell(row.DistinctEngineCount),
		metadataIntCell(row.DistinctIndexCount),
	}
}
