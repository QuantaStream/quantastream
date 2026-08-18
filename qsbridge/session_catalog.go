package qsbridge

import (
	"sort"
	"strings"
)

// NewSessionCatalog overlays connection-local metadata on top of the base catalog.
func NewSessionCatalog(base Catalog, session SessionContext, defaultSchema string) Catalog {
	if len(session.TemporaryTables) == 0 {
		return base
	}
	return SessionCatalog{
		Base:          base,
		Session:       session.Clone(),
		DefaultSchema: defaultSchema,
	}
}

// SessionCatalog exposes session-local temporary tables while delegating durable metadata.
type SessionCatalog struct {
	Base          Catalog
	Session       SessionContext
	DefaultSchema string
}

// Table resolves temporary tables before durable catalog tables.
func (c SessionCatalog) Table(schema string, name string) (TableDefinition, DiagnosticSet) {
	if table, ok := c.temporaryTable(schema, name); ok {
		return table, nil
	}
	if c.Base == nil {
		return TableDefinition{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "table not found: "+qualifiedCatalogName(schema, name)),
		}
	}
	return c.Base.Table(schema, name)
}

// View delegates durable views unless a temporary table shadows the view name.
func (c SessionCatalog) View(schema string, name string) (SQLViewDefinition, DiagnosticSet) {
	if _, ok := c.temporaryTable(schema, name); ok {
		return SQLViewDefinition{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogViewNotFound, PhaseBind, "view not found: "+qualifiedCatalogName(schema, name)),
		}
	}
	viewCatalog, ok := c.Base.(ViewCatalog)
	if !ok {
		return SQLViewDefinition{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogViewNotFound, PhaseBind, "view not found: "+qualifiedCatalogName(schema, name)),
		}
	}
	return viewCatalog.View(schema, name)
}

// Relationship delegates relationship metadata to the durable catalog.
func (c SessionCatalog) Relationship(name string) (RelationshipDefinition, DiagnosticSet) {
	if c.Base == nil {
		return RelationshipDefinition{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogRelationshipNotFound, PhaseBind, "relationship not found: "+name),
		}
	}
	return c.Base.Relationship(name)
}

// DependentRelationships delegates durable dependency metadata.
func (c SessionCatalog) DependentRelationships(schema string, parentTable string) ([]RelationshipDefinition, DiagnosticSet) {
	dependencyCatalog, ok := c.Base.(DependentRelationshipCatalog)
	if !ok {
		return nil, catalogMetadataUnsupportedDiagnostics()
	}
	return dependencyCatalog.DependentRelationships(schema, parentTable)
}

// Function delegates function metadata to the durable catalog.
func (c SessionCatalog) Function(name string) (FunctionDefinition, DiagnosticSet) {
	if c.Base == nil {
		return FunctionDefinition{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogFunctionNotFound, PhaseBind, "function not found: "+name),
		}
	}
	return c.Base.Function(name)
}

// ListSchemas merges durable schemas with schemas that hold temporary tables.
func (c SessionCatalog) ListSchemas() ([]CatalogSchemaDefinition, DiagnosticSet) {
	seen := make(map[string]string)
	if metadata, ok := c.Base.(CatalogMetadata); ok {
		schemas, diagnostics := metadata.ListSchemas()
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		for _, schema := range schemas {
			if schema.Name == "" {
				continue
			}
			seen[strings.ToLower(schema.Name)] = schema.Name
		}
	}
	for _, table := range c.Session.TemporaryTables {
		schemaName := strings.TrimSpace(table.Schema)
		if schemaName == "" {
			schemaName = c.Session.EffectiveSchema(c.DefaultSchema)
		}
		if schemaName == "" {
			continue
		}
		seen[strings.ToLower(schemaName)] = schemaName
	}
	keys := sortedStringKeys(seen)
	schemas := make([]CatalogSchemaDefinition, 0, len(keys))
	for _, key := range keys {
		schemas = append(schemas, CatalogSchemaDefinition{Name: seen[key]})
	}
	return schemas, nil
}

// ListTables merges durable tables with session-local temporary tables, letting temps shadow durable names.
func (c SessionCatalog) ListTables(schema string) ([]TableDefinition, DiagnosticSet) {
	tablesByKey := make(map[string]TableDefinition)
	for _, table := range c.temporaryTablesForSchema(schema) {
		tablesByKey[temporaryTableKey(table.Schema, table.Name)] = table
	}
	metadata, hasMetadata := c.Base.(CatalogMetadata)
	if hasMetadata {
		tables, diagnostics := metadata.ListTables(schema)
		if diagnostics.BlocksNative() && len(tablesByKey) == 0 {
			return nil, diagnostics
		}
		for _, table := range tables {
			schemaName := strings.TrimSpace(table.Schema)
			if schemaName == "" {
				schemaName = schema
			}
			key := temporaryTableKey(schemaName, table.Name)
			if _, shadowed := tablesByKey[key]; shadowed {
				continue
			}
			table.Schema = schemaName
			tablesByKey[key] = cloneTableDefinition(table)
		}
	} else if len(tablesByKey) == 0 {
		return nil, catalogMetadataUnsupportedDiagnostics()
	}
	tables := make([]TableDefinition, 0, len(tablesByKey))
	for _, table := range tablesByKey {
		tables = append(tables, cloneTableDefinition(table))
	}
	sort.Slice(tables, func(i, j int) bool {
		if !strings.EqualFold(tables[i].Schema, tables[j].Schema) {
			return strings.ToLower(tables[i].Schema) < strings.ToLower(tables[j].Schema)
		}
		return strings.ToLower(tables[i].Name) < strings.ToLower(tables[j].Name)
	})
	return tables, nil
}

// ListColumns resolves temporary-table fields before durable catalog fields.
func (c SessionCatalog) ListColumns(schema string, table string) ([]FieldDefinition, DiagnosticSet) {
	if definition, ok := c.temporaryTable(schema, table); ok {
		return cloneFieldDefinitions(definition.Fields), nil
	}
	metadata, ok := c.Base.(CatalogMetadata)
	if !ok {
		return nil, catalogMetadataUnsupportedDiagnostics()
	}
	return metadata.ListColumns(schema, table)
}

// ListViews delegates durable views and removes names shadowed by temporary tables.
func (c SessionCatalog) ListViews(schema string) ([]SQLViewDefinition, DiagnosticSet) {
	metadata, ok := c.Base.(CatalogViewMetadata)
	if !ok {
		return nil, catalogMetadataUnsupportedDiagnostics()
	}
	views, diagnostics := metadata.ListViews(schema)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	filtered := make([]SQLViewDefinition, 0, len(views))
	for _, view := range views {
		schemaName := strings.TrimSpace(view.Schema)
		if schemaName == "" {
			schemaName = schema
		}
		if _, ok := c.temporaryTable(schemaName, view.Name); ok {
			continue
		}
		filtered = append(filtered, cloneSQLViewDefinition(view))
	}
	return filtered, nil
}

// ListFunctions delegates function enumeration to the durable catalog.
func (c SessionCatalog) ListFunctions() ([]FunctionDefinition, DiagnosticSet) {
	metadata, ok := c.Base.(CatalogFunctionMetadata)
	if !ok {
		return nil, catalogMetadataUnsupportedDiagnostics()
	}
	return metadata.ListFunctions()
}

func (c SessionCatalog) temporaryTable(schema string, name string) (TableDefinition, bool) {
	return sessionTemporaryTable(c.Session, c.DefaultSchema, schema, name)
}

func (c SessionCatalog) temporaryTablesForSchema(schema string) []TableDefinition {
	tables := make([]TableDefinition, 0)
	effectiveSchema := strings.TrimSpace(schema)
	if effectiveSchema == "" {
		effectiveSchema = c.Session.EffectiveSchema(c.DefaultSchema)
	}
	for _, table := range c.Session.TemporaryTables {
		tableSchema := strings.TrimSpace(table.Schema)
		if tableSchema == "" {
			tableSchema = c.Session.EffectiveSchema(c.DefaultSchema)
		}
		if effectiveSchema != "" && !strings.EqualFold(tableSchema, effectiveSchema) {
			continue
		}
		table.Schema = tableSchema
		tables = append(tables, cloneTableDefinition(table))
	}
	return tables
}

func sessionTemporaryTable(session SessionContext, defaultSchema string, schema string, name string) (TableDefinition, bool) {
	if len(session.TemporaryTables) == 0 {
		return TableDefinition{}, false
	}
	schema = strings.TrimSpace(schema)
	if schema == "" {
		schema = session.EffectiveSchema(defaultSchema)
	}
	table, ok := session.TemporaryTables[temporaryTableKey(schema, name)]
	if !ok {
		return TableDefinition{}, false
	}
	table.Schema = schema
	return cloneTableDefinition(table), true
}

func temporaryTableKey(schema string, name string) string {
	return strings.ToLower(strings.TrimSpace(schema)) + "\x00" + strings.ToLower(strings.TrimSpace(name))
}
