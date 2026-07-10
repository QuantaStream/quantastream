package qsbridge

// ClientTableStatus describes one adapter-visible table status row.
type ClientTableStatus struct {
	Schema        string
	Name          string
	Engine        string
	Index         string
	Partitioned   bool
	Searchable    bool
	Replicated    bool
	Fields        int
	Relationships int
}

// ClientTableStatusSummary describes aggregate adapter-visible table status metadata.
type ClientTableStatusSummary struct {
	TableCount          int
	PartitionedCount    int
	SearchableCount     int
	ReplicatedCount     int
	FieldCount          int
	RelationshipCount   int
	DistinctEngineCount int
	DistinctIndexCount  int
}

// ClientTableStatusExchange is client-facing table status metadata.
type ClientTableStatusExchange struct {
	Connection   ConnectionContext
	Schema       string
	Pattern      string
	Tables       []ClientTableStatus
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientTableStatus returns SHOW TABLE STATUS-style catalog traits for adapters.
func (s PlanningService) ListClientTableStatus(connection ConnectionContext, catalog CatalogMetadata, schema string, pattern string) ClientTableStatusExchange {
	_ = s
	schema = effectiveClientMetadataSchema(connection, schema)
	exchange := ClientTableStatusExchange{
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
		exchange.Result = exchange.tableStatusResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if schema == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "table status metadata requires a schema name or selected schema"),
		})
		exchange.Result = exchange.tableStatusResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	tables, diagnostics := catalog.ListTables(schema)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Tables = tableStatusRows(tables, pattern)
	}
	exchange.Result = exchange.tableStatusResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether table status metadata can be returned.
func (e ClientTableStatusExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts table status diagnostics into protocol-facing errors.
func (e ClientTableStatusExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking table status error, if any.
func (e ClientTableStatusExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientTableStatusExchange) tableStatusResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     tableStatusResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.tableStatusResultRows(),
		Final: true,
	})
}

func tableStatusResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Schema", Type: DataTypeString},
		{Name: "Name", Type: DataTypeString},
		{Name: "Engine", Type: DataTypeString, Nullable: true},
		{Name: "Index", Type: DataTypeString, Nullable: true},
		{Name: "Partitioned", Type: DataTypeBool},
		{Name: "Searchable", Type: DataTypeBool},
		{Name: "Replicated", Type: DataTypeBool},
		{Name: "Fields", Type: DataTypeInt},
		{Name: "Relationships", Type: DataTypeInt},
	}
}

func (e ClientTableStatusExchange) tableStatusResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Tables))
	for _, table := range e.Tables {
		rows = append(rows, ResultRow{
			metadataStringCell(table.Schema),
			metadataStringCell(table.Name),
			metadataStringCell(table.Engine),
			metadataStringCell(table.Index),
			metadataBoolCell(table.Partitioned),
			metadataBoolCell(table.Searchable),
			metadataBoolCell(table.Replicated),
			metadataIntCell(table.Fields),
			metadataIntCell(table.Relationships),
		})
	}
	return rows
}

func summarizeTableStatusRows(rows []ClientTableStatus) ClientTableStatusSummary {
	summary := ClientTableStatusSummary{TableCount: len(rows)}
	engines := make(map[string]struct{})
	indexes := make(map[string]struct{})
	for _, row := range rows {
		if row.Partitioned {
			summary.PartitionedCount++
		}
		if row.Searchable {
			summary.SearchableCount++
		}
		if row.Replicated {
			summary.ReplicatedCount++
		}
		summary.FieldCount += row.Fields
		summary.RelationshipCount += row.Relationships
		if row.Engine != "" {
			engines[row.Engine] = struct{}{}
		}
		if row.Index != "" {
			indexes[row.Index] = struct{}{}
		}
	}
	summary.DistinctEngineCount = len(engines)
	summary.DistinctIndexCount = len(indexes)
	return summary
}

func tableStatusRows(tables []TableDefinition, pattern string) []ClientTableStatus {
	rows := make([]ClientTableStatus, 0, len(tables))
	for _, table := range tables {
		if pattern != "" && pattern != "*" && pattern != "%" && !catalogFieldPatternMatch(pattern, table.Name) {
			continue
		}
		rows = append(rows, ClientTableStatus{
			Schema:        table.Schema,
			Name:          table.Name,
			Engine:        table.Storage.Engine,
			Index:         string(table.Storage.Index),
			Partitioned:   table.Storage.Partitioned,
			Searchable:    table.Storage.Searchable,
			Replicated:    table.Storage.Replicated,
			Fields:        len(table.Fields),
			Relationships: len(table.Relationships),
		})
	}
	return rows
}
