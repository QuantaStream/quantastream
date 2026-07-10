package qsbridge

import (
	"sort"
	"strings"
)

// ClientCatalogSummary describes aggregate catalog shape for one schema.
type ClientCatalogSummary struct {
	Schema           string
	Tables           int
	Columns          int
	Relationships    int
	DictionaryFields int
	StringEnums      int
	SearchableFields int
}

// ClientCatalogSummaryExchange is client-facing aggregate catalog metadata.
type ClientCatalogSummaryExchange struct {
	Connection   ConnectionContext
	Schema       string
	Summaries    []ClientCatalogSummary
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientCatalogSummary returns aggregate catalog metadata grouped by schema.
func (s PlanningService) ListClientCatalogSummary(connection ConnectionContext, catalog CatalogMetadata, schema string) ClientCatalogSummaryExchange {
	_ = s
	exchange := ClientCatalogSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Schema:      schema,
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return exchange
	}
	if catalog == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, catalogMetadataUnsupportedDiagnostics())
		exchange.Result = exchange.catalogSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	tables, diagnostics := catalog.ListTables(schema)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	if !exchange.Diagnostics.BlocksNative() {
		schemas, schemaDiagnostics := catalog.ListSchemas()
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, schemaDiagnostics)
		if !exchange.Diagnostics.BlocksNative() {
			exchange.Summaries = catalogSummariesWithSchemas(schemas, tables, schema)
		}
	}
	exchange.Result = exchange.catalogSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether catalog summary metadata can be returned.
func (e ClientCatalogSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts catalog summary diagnostics into protocol-facing errors.
func (e ClientCatalogSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking catalog summary error, if any.
func (e ClientCatalogSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCatalogSummaryExchange) catalogSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     catalogSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.catalogSummaryRows(),
		Final: true,
	})
}

func catalogSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Schema", Type: DataTypeString},
		{Name: "Tables", Type: DataTypeInt},
		{Name: "Columns", Type: DataTypeInt},
		{Name: "Relationships", Type: DataTypeInt},
		{Name: "Dictionary_fields", Type: DataTypeInt},
		{Name: "String_enums", Type: DataTypeInt},
		{Name: "Searchable_fields", Type: DataTypeInt},
	}
}

func (e ClientCatalogSummaryExchange) catalogSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Summaries))
	for _, summary := range e.Summaries {
		rows = append(rows, ResultRow{
			metadataStringCell(summary.Schema),
			metadataIntCell(summary.Tables),
			metadataIntCell(summary.Columns),
			metadataIntCell(summary.Relationships),
			metadataIntCell(summary.DictionaryFields),
			metadataIntCell(summary.StringEnums),
			metadataIntCell(summary.SearchableFields),
		})
	}
	return rows
}

func catalogSummariesWithSchemas(schemas []CatalogSchemaDefinition, tables []TableDefinition, requestedSchema string) []ClientCatalogSummary {
	summaries := catalogSummariesBySchema(tables)
	seen := make(map[string]struct{}, len(summaries))
	for _, summary := range summaries {
		seen[strings.ToLower(summary.Schema)] = struct{}{}
	}
	for _, schema := range schemas {
		if schema.Name == "" {
			continue
		}
		if requestedSchema != "" && !strings.EqualFold(schema.Name, requestedSchema) {
			continue
		}
		key := strings.ToLower(schema.Name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		summaries = append(summaries, ClientCatalogSummary{Schema: schema.Name})
	}
	sort.Slice(summaries, func(i, j int) bool {
		return strings.ToLower(summaries[i].Schema) < strings.ToLower(summaries[j].Schema)
	})
	return summaries
}

func catalogSummariesBySchema(tables []TableDefinition) []ClientCatalogSummary {
	bySchema := make(map[string]*ClientCatalogSummary)
	displayNames := make(map[string]string)
	for _, table := range tables {
		key := strings.ToLower(table.Schema)
		if _, ok := displayNames[key]; !ok {
			displayNames[key] = table.Schema
		}
		summary := bySchema[key]
		if summary == nil {
			summary = &ClientCatalogSummary{Schema: table.Schema}
			bySchema[key] = summary
		}
		summary.Tables++
		summary.Columns += len(table.Fields)
		summary.Relationships += len(table.Relationships)
		for _, field := range table.Fields {
			if dictionaryCatalogVisible(field.Dictionary) {
				summary.DictionaryFields++
			}
			if field.Index == IndexStringEnum || field.Encoding.Kind == EncodingStringEnum {
				summary.StringEnums++
			}
			if field.Searchable || field.Encoding.Searchable() {
				summary.SearchableFields++
			}
		}
	}

	keys := make([]string, 0, len(bySchema))
	for key := range bySchema {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	summaries := make([]ClientCatalogSummary, 0, len(keys))
	for _, key := range keys {
		summary := *bySchema[key]
		if displayNames[key] != "" {
			summary.Schema = displayNames[key]
		}
		summaries = append(summaries, summary)
	}
	return summaries
}
