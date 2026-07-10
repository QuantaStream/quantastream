package qsbridge

import (
	"sort"
	"strings"
)

// CatalogSchemaDefinition describes one schema/database namespace.
type CatalogSchemaDefinition struct {
	Name string
}

// CatalogMetadata optionally exposes enumerable catalog metadata for adapters.
type CatalogMetadata interface {
	ListSchemas() ([]CatalogSchemaDefinition, DiagnosticSet)
	ListTables(schema string) ([]TableDefinition, DiagnosticSet)
	ListColumns(schema string, table string) ([]FieldDefinition, DiagnosticSet)
}

// CatalogFunctionMetadata optionally exposes enumerable function metadata for adapters.
type CatalogFunctionMetadata interface {
	ListFunctions() ([]FunctionDefinition, DiagnosticSet)
}

// ListSchemas returns distinct schema names known to this in-memory catalog.
func (c MemoryCatalog) ListSchemas() ([]CatalogSchemaDefinition, DiagnosticSet) {
	seen := make(map[string]string)
	for _, schema := range c.Schemas {
		if schema.Name == "" {
			continue
		}
		key := strings.ToLower(schema.Name)
		if _, ok := seen[key]; !ok {
			seen[key] = schema.Name
		}
	}
	for _, table := range c.Tables {
		if table.Schema == "" {
			continue
		}
		key := strings.ToLower(table.Schema)
		if _, ok := seen[key]; !ok {
			seen[key] = table.Schema
		}
	}
	keys := sortedStringKeys(seen)
	schemas := make([]CatalogSchemaDefinition, 0, len(keys))
	for _, key := range keys {
		schemas = append(schemas, CatalogSchemaDefinition{Name: seen[key]})
	}
	return schemas, nil
}

// ListTables returns table definitions for one schema.
func (c MemoryCatalog) ListTables(schema string) ([]TableDefinition, DiagnosticSet) {
	tables := make([]TableDefinition, 0)
	schemaKnown := schema == "" || c.schemaExists(schema)
	for _, table := range c.Tables {
		if schema != "" && !strings.EqualFold(table.Schema, schema) {
			continue
		}
		schemaKnown = true
		tables = append(tables, cloneTableDefinition(table))
	}
	sort.Slice(tables, func(i, j int) bool {
		if !strings.EqualFold(tables[i].Schema, tables[j].Schema) {
			return strings.ToLower(tables[i].Schema) < strings.ToLower(tables[j].Schema)
		}
		return strings.ToLower(tables[i].Name) < strings.ToLower(tables[j].Name)
	})
	if schema != "" && !schemaKnown {
		return nil, DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogSchemaNotFound, PhaseBind, "schema not found: "+schema),
		}
	}
	return tables, nil
}

func (c MemoryCatalog) schemaExists(schema string) bool {
	for _, candidate := range c.Schemas {
		if strings.EqualFold(candidate.Name, schema) {
			return true
		}
	}
	return false
}

// ListColumns returns field definitions for one table.
func (c MemoryCatalog) ListColumns(schema string, table string) ([]FieldDefinition, DiagnosticSet) {
	definition, diagnostics := c.Table(schema, table)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	fields := make([]FieldDefinition, 0, len(definition.Fields))
	for _, field := range definition.Fields {
		fields = append(fields, cloneFieldDefinition(field))
	}
	return fields, nil
}

// ListFunctions returns sorted function definitions known to this in-memory catalog.
func (c MemoryCatalog) ListFunctions() ([]FunctionDefinition, DiagnosticSet) {
	functions := make([]FunctionDefinition, 0, len(c.Functions))
	for _, function := range c.Functions {
		functions = append(functions, cloneFunctionDefinition(function))
	}
	sort.Slice(functions, func(i, j int) bool {
		return strings.ToLower(functions[i].Name) < strings.ToLower(functions[j].Name)
	})
	return functions, nil
}

// ListSchemas delegates schema enumeration to a metadata-capable backend.
func (c *CachedCatalog) ListSchemas() ([]CatalogSchemaDefinition, DiagnosticSet) {
	metadata, ok := c.backend.(CatalogMetadata)
	if !ok {
		return nil, catalogMetadataUnsupportedDiagnostics()
	}
	schemas, diagnostics := metadata.ListSchemas()
	return cloneCatalogSchemas(schemas), cloneDiagnosticSet(diagnostics)
}

// ListTables delegates table enumeration to a metadata-capable backend.
func (c *CachedCatalog) ListTables(schema string) ([]TableDefinition, DiagnosticSet) {
	metadata, ok := c.backend.(CatalogMetadata)
	if !ok {
		return nil, catalogMetadataUnsupportedDiagnostics()
	}
	tables, diagnostics := metadata.ListTables(schema)
	return cloneTableDefinitions(tables), cloneDiagnosticSet(diagnostics)
}

// ListColumns delegates column enumeration to a metadata-capable backend.
func (c *CachedCatalog) ListColumns(schema string, table string) ([]FieldDefinition, DiagnosticSet) {
	metadata, ok := c.backend.(CatalogMetadata)
	if !ok {
		return nil, catalogMetadataUnsupportedDiagnostics()
	}
	fields, diagnostics := metadata.ListColumns(schema, table)
	return cloneFieldDefinitions(fields), cloneDiagnosticSet(diagnostics)
}

// ListFunctions delegates function enumeration to a metadata-capable backend.
func (c *CachedCatalog) ListFunctions() ([]FunctionDefinition, DiagnosticSet) {
	metadata, ok := c.backend.(CatalogFunctionMetadata)
	if !ok {
		return nil, catalogMetadataUnsupportedDiagnostics()
	}
	functions, diagnostics := metadata.ListFunctions()
	return cloneFunctionDefinitions(functions), cloneDiagnosticSet(diagnostics)
}

func catalogMetadataUnsupportedDiagnostics() DiagnosticSet {
	return DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "catalog metadata enumeration is not supported by this catalog"),
	}
}

func cloneCatalogSchemas(schemas []CatalogSchemaDefinition) []CatalogSchemaDefinition {
	return append([]CatalogSchemaDefinition(nil), schemas...)
}

func cloneTableDefinitions(tables []TableDefinition) []TableDefinition {
	if len(tables) == 0 {
		return nil
	}
	cloned := make([]TableDefinition, 0, len(tables))
	for _, table := range tables {
		cloned = append(cloned, cloneTableDefinition(table))
	}
	return cloned
}

func cloneFieldDefinitions(fields []FieldDefinition) []FieldDefinition {
	if len(fields) == 0 {
		return nil
	}
	cloned := make([]FieldDefinition, 0, len(fields))
	for _, field := range fields {
		cloned = append(cloned, cloneFieldDefinition(field))
	}
	return cloned
}

func cloneFunctionDefinitions(functions []FunctionDefinition) []FunctionDefinition {
	if len(functions) == 0 {
		return nil
	}
	cloned := make([]FunctionDefinition, 0, len(functions))
	for _, function := range functions {
		cloned = append(cloned, cloneFunctionDefinition(function))
	}
	return cloned
}

func sortedStringKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
