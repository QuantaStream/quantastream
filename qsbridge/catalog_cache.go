package qsbridge

import "strings"

// This file keeps catalog metadata caching process-local and backend-neutral.
// Store adapters should sit behind Catalog rather than leaking into binding.

// CachedCatalog wraps another Catalog with explicit process-local caching.
//
// CachedCatalog is intentionally small. It gives binding and planning code a
// stable cache boundary without choosing Consul, KVStore, files, or another
// backend as the metadata persistence mechanism.
type CachedCatalog struct {
	backend       Catalog
	tables        *shardedValueCache
	views         *shardedValueCache
	relationships *shardedValueCache
	functions     *shardedValueCache
}

// NewCachedCatalog creates a process-local cache in front of backend.
func NewCachedCatalog(backend Catalog) *CachedCatalog {
	return &CachedCatalog{
		backend:       backend,
		tables:        newShardedValueCache(),
		views:         newShardedValueCache(),
		relationships: newShardedValueCache(),
		functions:     newShardedValueCache(),
	}
}

// Table looks up a table definition through the cache.
func (c *CachedCatalog) Table(schema string, name string) (TableDefinition, DiagnosticSet) {
	key := catalogTableCacheKey(schema, name)
	if value, ok := c.tables.Get(key); ok {
		cached := value.(cachedTable)
		return cloneTableDefinition(cached.value), cloneDiagnosticSet(cached.diagnostics)
	}

	table, diagnostics := c.backend.Table(schema, name)
	c.tables.Set(key, cachedTable{value: cloneTableDefinition(table), diagnostics: cloneDiagnosticSet(diagnostics)})
	return cloneTableDefinition(table), cloneDiagnosticSet(diagnostics)
}

// View looks up a logical view definition through the cache when the backend supports views.
func (c *CachedCatalog) View(schema string, name string) (SQLViewDefinition, DiagnosticSet) {
	backend, ok := c.backend.(ViewCatalog)
	if !ok {
		return SQLViewDefinition{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogViewNotFound, PhaseBind, "view not found: "+qualifiedCatalogName(schema, name)),
		}
	}
	key := catalogTableCacheKey(schema, name)
	if value, ok := c.views.Get(key); ok {
		cached := value.(cachedView)
		return cloneSQLViewDefinition(cached.value), cloneDiagnosticSet(cached.diagnostics)
	}

	view, diagnostics := backend.View(schema, name)
	if !diagnostics.BlocksNative() {
		c.views.Set(key, cachedView{value: cloneSQLViewDefinition(view), diagnostics: cloneDiagnosticSet(diagnostics)})
	}
	return cloneSQLViewDefinition(view), cloneDiagnosticSet(diagnostics)
}

// Relationship looks up a relationship definition through the cache.
func (c *CachedCatalog) Relationship(name string) (RelationshipDefinition, DiagnosticSet) {
	key := catalogNameCacheKey(name)
	if value, ok := c.relationships.Get(key); ok {
		cached := value.(cachedRelationship)
		return cached.value, cloneDiagnosticSet(cached.diagnostics)
	}

	relationship, diagnostics := c.backend.Relationship(name)
	c.relationships.Set(key, cachedRelationship{value: relationship, diagnostics: cloneDiagnosticSet(diagnostics)})
	return relationship, cloneDiagnosticSet(diagnostics)
}

// Function looks up a function definition through the cache.
func (c *CachedCatalog) Function(name string) (FunctionDefinition, DiagnosticSet) {
	key := catalogNameCacheKey(name)
	if value, ok := c.functions.Get(key); ok {
		cached := value.(cachedFunction)
		return cloneFunctionDefinition(cached.value), cloneDiagnosticSet(cached.diagnostics)
	}

	function, diagnostics := c.backend.Function(name)
	c.functions.Set(key, cachedFunction{value: cloneFunctionDefinition(function), diagnostics: cloneDiagnosticSet(diagnostics)})
	return cloneFunctionDefinition(function), cloneDiagnosticSet(diagnostics)
}

// DependentRelationships returns child dependencies from a backend that can enumerate them.
func (c *CachedCatalog) DependentRelationships(schema string, parentTable string) ([]RelationshipDefinition, DiagnosticSet) {
	backend, ok := c.backend.(DependentRelationshipCatalog)
	if !ok {
		return nil, nil
	}
	relationships, diagnostics := backend.DependentRelationships(schema, parentTable)
	return cloneRelationshipDefinitions(relationships), cloneDiagnosticSet(diagnostics)
}

// InvalidateTable removes one cached table entry.
func (c *CachedCatalog) InvalidateTable(schema string, name string) {
	c.tables.Delete(catalogTableCacheKey(schema, name))
}

// InvalidateView removes one cached view entry.
func (c *CachedCatalog) InvalidateView(schema string, name string) {
	c.views.Delete(catalogTableCacheKey(schema, name))
}

// InvalidateRelationship removes one cached relationship entry.
func (c *CachedCatalog) InvalidateRelationship(name string) {
	c.relationships.Delete(catalogNameCacheKey(name))
}

// InvalidateFunction removes one cached function entry.
func (c *CachedCatalog) InvalidateFunction(name string) {
	c.functions.Delete(catalogNameCacheKey(name))
}

// Clear removes all cached metadata.
func (c *CachedCatalog) Clear() {
	c.tables.Clear()
	c.views.Clear()
	c.relationships.Clear()
	c.functions.Clear()
}

type cachedTable struct {
	value       TableDefinition
	diagnostics DiagnosticSet
}

type cachedView struct {
	value       SQLViewDefinition
	diagnostics DiagnosticSet
}

type cachedRelationship struct {
	value       RelationshipDefinition
	diagnostics DiagnosticSet
}

type cachedFunction struct {
	value       FunctionDefinition
	diagnostics DiagnosticSet
}

// catalogTableCacheKey normalizes table lookup identity across case-insensitive SQL names.
func catalogTableCacheKey(schema string, name string) string {
	return strings.ToLower(schema) + "\x00" + strings.ToLower(name)
}

// catalogNameCacheKey normalizes single-name catalog lookups across case-insensitive SQL names.
func catalogNameCacheKey(name string) string {
	return strings.ToLower(name)
}

// cloneTableDefinition returns table metadata that callers may mutate without poisoning the cache.
func cloneTableDefinition(table TableDefinition) TableDefinition {
	cloned := table
	cloned.Fields = make([]FieldDefinition, 0, len(table.Fields))
	for _, field := range table.Fields {
		cloned.Fields = append(cloned.Fields, cloneFieldDefinition(field))
	}
	cloned.Relationships = cloneRelationshipDefinitions(table.Relationships)
	return cloned
}

// cloneSQLViewDefinition returns view metadata that callers may mutate safely.
func cloneSQLViewDefinition(view SQLViewDefinition) SQLViewDefinition {
	return view
}

// cloneFunctionDefinition returns function metadata with independent slice fields.
func cloneFunctionDefinition(function FunctionDefinition) FunctionDefinition {
	cloned := function
	cloned.Contexts = append([]FunctionBindingContext(nil), function.Contexts...)
	cloned.Arguments = append([]DataType(nil), function.Arguments...)
	cloned.Aliases = append([]string(nil), function.Aliases...)
	return cloned
}

// cloneFieldDefinition returns field metadata with independent dictionary metadata.
func cloneFieldDefinition(field FieldDefinition) FieldDefinition {
	cloned := field
	cloned.Dictionary = cloneDictionaryDefinition(field.Dictionary)
	return cloned
}

// cloneDiagnosticSet returns diagnostics with independent field-reference slices.
func cloneDiagnosticSet(diagnostics DiagnosticSet) DiagnosticSet {
	if len(diagnostics) == 0 {
		return nil
	}
	cloned := make(DiagnosticSet, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		diagnostic.Fields = append([]FieldRef(nil), diagnostic.Fields...)
		cloned = append(cloned, diagnostic)
	}
	return cloned
}
