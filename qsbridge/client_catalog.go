package qsbridge

import "strings"

// ClientCatalogMetadataKind identifies a catalog metadata request shape.
type ClientCatalogMetadataKind string

const (
	// ClientCatalogSchemas lists schema/database names.
	ClientCatalogSchemas ClientCatalogMetadataKind = "schemas"
	// ClientCatalogTables lists tables for a schema.
	ClientCatalogTables ClientCatalogMetadataKind = "tables"
	// ClientCatalogColumns lists columns for a table.
	ClientCatalogColumns ClientCatalogMetadataKind = "columns"
)

// ClientCatalogMetadataExchange is client-facing catalog metadata.
type ClientCatalogMetadataExchange struct {
	Connection   ConnectionContext
	Kind         ClientCatalogMetadataKind
	Schema       string
	Table        string
	Pattern      string
	Schemas      []CatalogSchemaDefinition
	Tables       []TableDefinition
	Columns      []FieldDefinition
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientCatalogSchemas returns schema metadata for adapters.
func (s PlanningService) ListClientCatalogSchemas(connection ConnectionContext, catalog CatalogMetadata) ClientCatalogMetadataExchange {
	_ = s
	exchange := ClientCatalogMetadataExchange{
		Connection:  cloneConnectionContext(connection),
		Kind:        ClientCatalogSchemas,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if catalog == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, catalogMetadataUnsupportedDiagnostics())
		exchange.Result = exchange.catalogMetadataResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	schemas, diagnostics := catalog.ListSchemas()
	exchange.Schemas = cloneCatalogSchemas(schemas)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	exchange.Result = exchange.catalogMetadataResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// ListClientCatalogTables returns table metadata for adapters.
func (s PlanningService) ListClientCatalogTables(connection ConnectionContext, catalog CatalogMetadata, schema string) ClientCatalogMetadataExchange {
	_ = s
	schema = effectiveClientMetadataSchema(connection, schema)
	exchange := ClientCatalogMetadataExchange{
		Connection:  cloneConnectionContext(connection),
		Kind:        ClientCatalogTables,
		Schema:      schema,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if catalog == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, catalogMetadataUnsupportedDiagnostics())
		exchange.Result = exchange.catalogMetadataResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if schema == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "table metadata requires a schema name or selected schema"),
		})
		exchange.Result = exchange.catalogMetadataResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	tables, diagnostics := catalog.ListTables(schema)
	exchange.Tables = cloneTableDefinitions(tables)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	exchange.Result = exchange.catalogMetadataResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// ListClientCatalogColumns returns column metadata for adapters.
func (s PlanningService) ListClientCatalogColumns(connection ConnectionContext, catalog CatalogMetadata, schema string, table string) ClientCatalogMetadataExchange {
	_ = s
	schema = effectiveClientMetadataSchema(connection, schema)
	exchange := ClientCatalogMetadataExchange{
		Connection:  cloneConnectionContext(connection),
		Kind:        ClientCatalogColumns,
		Schema:      schema,
		Table:       table,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if catalog == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, catalogMetadataUnsupportedDiagnostics())
		exchange.Result = exchange.catalogMetadataResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if schema == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "column metadata requires a schema name or selected schema"),
		})
		exchange.Result = exchange.catalogMetadataResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if table == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "column metadata requires a table name"),
		})
		exchange.Result = exchange.catalogMetadataResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	columns, diagnostics := catalog.ListColumns(schema, table)
	exchange.Columns = cloneFieldDefinitions(columns)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	exchange.Result = exchange.catalogMetadataResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// ListClientTableFields returns table field metadata with an optional protocol wildcard.
func (s PlanningService) ListClientTableFields(connection ConnectionContext, catalog CatalogMetadata, schema string, table string, pattern string) ClientCatalogMetadataExchange {
	exchange := s.ListClientCatalogColumns(connection, catalog, schema, table)
	exchange.Pattern = pattern
	if exchange.Diagnostics.BlocksNative() {
		return exchange
	}
	exchange.Columns = filterCatalogFields(exchange.Columns, pattern)
	exchange.Result = exchange.catalogMetadataResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether catalog metadata can be returned.
func (e ClientCatalogMetadataExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts catalog metadata diagnostics into protocol-facing errors.
func (e ClientCatalogMetadataExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking catalog metadata error, if any.
func (e ClientCatalogMetadataExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func effectiveClientMetadataSchema(connection ConnectionContext, schema string) string {
	if schema != "" {
		return schema
	}
	return connection.Session.CurrentSchema
}

func (e ClientCatalogMetadataExchange) catalogMetadataResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     catalogMetadataResultColumns(e.Kind),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
		Complete:    false,
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.catalogMetadataRows(),
		Final: true,
	})
}

func catalogMetadataResultColumns(kind ClientCatalogMetadataKind) []ResultColumn {
	switch kind {
	case ClientCatalogSchemas:
		return []ResultColumn{{Name: "Database", Type: DataTypeString}}
	case ClientCatalogTables:
		return []ResultColumn{
			{Name: "Schema", Type: DataTypeString},
			{Name: "Name", Type: DataTypeString},
			{Name: "Engine", Type: DataTypeString, Nullable: true},
			{Name: "Partitioned", Type: DataTypeBool},
			{Name: "Searchable", Type: DataTypeBool},
			{Name: "Replicated", Type: DataTypeBool},
		}
	case ClientCatalogColumns:
		return []ResultColumn{
			{Name: "Field", Type: DataTypeString},
			{Name: "Type", Type: DataTypeString},
			{Name: "Null", Type: DataTypeBool},
			{Name: "Key", Type: DataTypeString},
			{Name: "PhysicalName", Type: DataTypeString, Nullable: true},
			{Name: "Index", Type: DataTypeString, Nullable: true},
			{Name: "Searchable", Type: DataTypeBool},
		}
	default:
		return nil
	}
}

func (e ClientCatalogMetadataExchange) catalogMetadataRows() []ResultRow {
	switch e.Kind {
	case ClientCatalogSchemas:
		rows := make([]ResultRow, 0, len(e.Schemas))
		for _, schema := range e.Schemas {
			rows = append(rows, ResultRow{
				metadataStringCell(schema.Name),
			})
		}
		return rows
	case ClientCatalogTables:
		rows := make([]ResultRow, 0, len(e.Tables))
		for _, table := range e.Tables {
			rows = append(rows, ResultRow{
				metadataStringCell(table.Schema),
				metadataStringCell(table.Name),
				metadataStringCell(table.Storage.Engine),
				metadataBoolCell(table.Storage.Partitioned),
				metadataBoolCell(table.Storage.Searchable),
				metadataBoolCell(table.Storage.Replicated),
			})
		}
		return rows
	case ClientCatalogColumns:
		rows := make([]ResultRow, 0, len(e.Columns))
		for _, column := range e.Columns {
			rows = append(rows, ResultRow{
				metadataStringCell(column.Name),
				metadataStringCell(string(column.Type)),
				metadataBoolCell(column.Nullable),
				metadataStringCell(catalogColumnKey(column)),
				metadataStringCell(column.PhysicalName),
				metadataStringCell(string(column.Index)),
				metadataBoolCell(column.Searchable),
			})
		}
		return rows
	default:
		return nil
	}
}

func metadataStringCell(value string) ResultCell {
	return ResultCell{Kind: ValueString, Value: value}
}

func metadataBoolCell(value bool) ResultCell {
	return ResultCell{Kind: ValueBool, Value: value}
}

func catalogColumnKey(column FieldDefinition) string {
	if column.PrimaryKey {
		return "PRI"
	}
	return ""
}

func filterCatalogFields(fields []FieldDefinition, pattern string) []FieldDefinition {
	if pattern == "" || pattern == "*" || pattern == "%" {
		return cloneFieldDefinitions(fields)
	}
	filtered := make([]FieldDefinition, 0, len(fields))
	for _, field := range fields {
		if catalogFieldPatternMatch(pattern, field.Name) || (field.PhysicalName != "" && catalogFieldPatternMatch(pattern, field.PhysicalName)) {
			filtered = append(filtered, cloneFieldDefinition(field))
		}
	}
	return filtered
}

func catalogFieldPatternMatch(pattern string, value string) bool {
	return catalogWildcardMatch(strings.ToLower(pattern), strings.ToLower(value))
}

func catalogWildcardMatch(pattern string, value string) bool {
	if pattern == "" {
		return value == ""
	}
	if pattern == "%" || pattern == "*" {
		return true
	}
	return catalogWildcardMatchRunes([]rune(pattern), []rune(value), 0, 0)
}

func catalogWildcardMatchRunes(pattern []rune, value []rune, pi int, vi int) bool {
	for pi < len(pattern) {
		switch pattern[pi] {
		case '%', '*':
			for pi < len(pattern) && (pattern[pi] == '%' || pattern[pi] == '*') {
				pi++
			}
			if pi == len(pattern) {
				return true
			}
			for vi <= len(value) {
				if catalogWildcardMatchRunes(pattern, value, pi, vi) {
					return true
				}
				if vi == len(value) {
					break
				}
				vi++
			}
			return false
		case '_', '?':
			if vi >= len(value) {
				return false
			}
			pi++
			vi++
		default:
			if vi >= len(value) || pattern[pi] != value[vi] {
				return false
			}
			pi++
			vi++
		}
	}
	return vi == len(value)
}
