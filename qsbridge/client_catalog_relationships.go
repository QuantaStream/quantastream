package qsbridge

import "sort"

// ClientCatalogRelationship describes one adapter-visible table relationship.
type ClientCatalogRelationship struct {
	Schema           string
	Table            string
	Name             string
	ColumnName       string
	ReferencedTable  string
	ReferencedColumn string
	Direction        JoinDirection
	Cardinality      string
	Encoding         RelationshipEncodingKind
	Capabilities     RelationshipCapabilities
}

// ClientCatalogRelationshipExchange is client-facing relationship metadata for one table.
type ClientCatalogRelationshipExchange struct {
	Connection    ConnectionContext
	Schema        string
	Table         string
	Relationships []ClientCatalogRelationship
	Result        ExecutionResult
	ResultSchema  ProtocolResultSchema
	Diagnostics   DiagnosticSet
}

// ListClientCatalogRelationships returns relationship metadata for adapters.
func (s PlanningService) ListClientCatalogRelationships(connection ConnectionContext, catalog Catalog, schema string, table string) ClientCatalogRelationshipExchange {
	_ = s
	schema = effectiveClientMetadataSchema(connection, schema)
	exchange := ClientCatalogRelationshipExchange{
		Connection:  cloneConnectionContext(connection),
		Schema:      schema,
		Table:       table,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if catalog == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, catalogMetadataUnsupportedDiagnostics())
		exchange.Result = exchange.catalogRelationshipResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if schema == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "relationship metadata requires a schema name or selected schema"),
		})
		exchange.Result = exchange.catalogRelationshipResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if table == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "relationship metadata requires a table name"),
		})
		exchange.Result = exchange.catalogRelationshipResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	definition, diagnostics := catalog.Table(schema, table)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	if !exchange.Diagnostics.BlocksNative() {
		exchange.Relationships = tableCatalogRelationships(definition)
	}
	exchange.Result = exchange.catalogRelationshipResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether relationship metadata can be returned.
func (e ClientCatalogRelationshipExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts relationship metadata diagnostics into protocol-facing errors.
func (e ClientCatalogRelationshipExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking relationship metadata error, if any.
func (e ClientCatalogRelationshipExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCatalogRelationshipExchange) catalogRelationshipResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     catalogRelationshipResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.catalogRelationshipRows(),
		Final: true,
	})
}

func catalogRelationshipResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Schema", Type: DataTypeString},
		{Name: "Table", Type: DataTypeString},
		{Name: "Constraint_name", Type: DataTypeString},
		{Name: "Column_name", Type: DataTypeString},
		{Name: "Referenced_table", Type: DataTypeString},
		{Name: "Referenced_column", Type: DataTypeString},
		{Name: "Direction", Type: DataTypeString},
		{Name: "Cardinality", Type: DataTypeString, Nullable: true},
		{Name: "Encoding", Type: DataTypeString, Nullable: true},
		{Name: "Capabilities", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientCatalogRelationshipExchange) catalogRelationshipRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Relationships))
	for _, relationship := range e.Relationships {
		rows = append(rows, ResultRow{
			metadataStringCell(relationship.Schema),
			metadataStringCell(relationship.Table),
			metadataStringCell(relationship.Name),
			metadataStringCell(relationship.ColumnName),
			metadataStringCell(relationship.ReferencedTable),
			metadataStringCell(relationship.ReferencedColumn),
			metadataStringCell(string(relationship.Direction)),
			metadataStringCell(relationship.Cardinality),
			metadataStringCell(string(relationship.Encoding)),
			metadataStringCell(joinRelationshipCapabilities(relationship.Capabilities)),
		})
	}
	return rows
}

func tableCatalogRelationships(table TableDefinition) []ClientCatalogRelationship {
	relationships := make([]ClientCatalogRelationship, 0, len(table.Relationships))
	for _, relationship := range table.Relationships {
		relationships = append(relationships, ClientCatalogRelationship{
			Schema:           table.Schema,
			Table:            table.Name,
			Name:             relationship.Name,
			ColumnName:       relationship.FromField,
			ReferencedTable:  relationship.ToTable,
			ReferencedColumn: relationship.ToField,
			Direction:        relationship.Direction,
			Cardinality:      relationship.Cardinality,
			Encoding:         relationship.Encoding.Kind,
			Capabilities:     append(RelationshipCapabilities(nil), relationship.Encoding.Capabilities...),
		})
	}
	sort.SliceStable(relationships, func(i, j int) bool {
		if relationships[i].Name != relationships[j].Name {
			return relationships[i].Name < relationships[j].Name
		}
		if relationships[i].ColumnName != relationships[j].ColumnName {
			return relationships[i].ColumnName < relationships[j].ColumnName
		}
		return relationships[i].ReferencedTable < relationships[j].ReferencedTable
	})
	return relationships
}

func joinRelationshipCapabilities(capabilities RelationshipCapabilities) string {
	if len(capabilities) == 0 {
		return ""
	}
	values := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		values = append(values, string(capability))
	}
	sort.Strings(values)
	joined := ""
	for i, value := range values {
		if i > 0 {
			joined += ","
		}
		joined += value
	}
	return joined
}
