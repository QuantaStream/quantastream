package qsbridge

import (
	"fmt"
	"strings"
)

// UnboundTable describes a table reference before catalog binding.
type UnboundTable struct {
	Schema string
	Name   string
	Alias  string
	Role   string
}

// BoundTable records one query-local table instance and its catalog metadata.
type BoundTable struct {
	Instance   TableInstance
	Definition TableDefinition
}

// BindContext holds catalog-backed name resolution state for one statement.
type BindContext struct {
	Catalog       Catalog
	DefaultSchema string

	tables      []BoundTable
	diagnostics DiagnosticSet
	nextTableID int
}

// NewBindContext creates a binding context for one statement.
func NewBindContext(catalog Catalog, defaultSchema string) *BindContext {
	return &BindContext{
		Catalog:       catalog,
		DefaultSchema: defaultSchema,
		tables:        make([]BoundTable, 0),
	}
}

// Diagnostics returns diagnostics accumulated by the bind context.
func (c *BindContext) Diagnostics() DiagnosticSet {
	if c == nil {
		return nil
	}
	return append(DiagnosticSet(nil), c.diagnostics...)
}

// Tables returns the currently bound table instances in binding order.
func (c *BindContext) Tables() []BoundTable {
	if c == nil {
		return nil
	}
	return append([]BoundTable(nil), c.tables...)
}

// Sources returns the query-local table instances in binding order.
func (c *BindContext) Sources() []TableInstance {
	if c == nil {
		return nil
	}
	sources := make([]TableInstance, 0, len(c.tables))
	for _, table := range c.tables {
		sources = append(sources, table.Instance)
	}
	return sources
}

// AddTable binds a table reference against the catalog and records its alias.
func (c *BindContext) AddTable(table UnboundTable) (BoundTable, DiagnosticSet) {
	if c == nil {
		return BoundTable{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	if c.Catalog == nil {
		return BoundTable{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "catalog is nil"),
		})
	}
	if table.Name == "" {
		return BoundTable{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "table name is empty"),
		})
	}
	if c.hasTableRef(tableRefName(table.Name, table.Alias)) {
		return BoundTable{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticDuplicateTableAlias, PhaseBind, "duplicate table reference: "+tableRefName(table.Name, table.Alias)),
		})
	}

	schema := table.Schema
	if schema == "" {
		schema = c.DefaultSchema
	}
	definition, diagnostics := c.Catalog.Table(schema, table.Name)
	if diagnostics.BlocksNative() {
		if virtual, ok := InformationSchemaTableDefinition(schema, table.Name); ok {
			definition = virtual
			diagnostics = nil
		} else {
			return BoundTable{}, c.record(diagnostics)
		}
	}

	c.nextTableID++
	instance := definition.Instance(TableInstanceID(fmt.Sprintf("%s_%d", table.Name, c.nextTableID)), table.Alias)
	instance.Role = table.Role
	bound := BoundTable{
		Instance:   instance,
		Definition: definition,
	}
	c.tables = append(c.tables, bound)
	return bound, nil
}

// ResolveTable resolves a table name or alias in the current binding context.
func (c *BindContext) ResolveTable(ref string) (BoundTable, DiagnosticSet) {
	if c == nil {
		return BoundTable{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	for _, table := range c.tables {
		if strings.EqualFold(table.Instance.RefName(), ref) {
			return table, nil
		}
	}
	return BoundTable{}, c.record(DiagnosticSet{
		ErrorDiagnostic(DiagnosticTableAliasNotFound, PhaseBind, "table reference not found: "+ref),
	})
}

// ResolveField resolves a field reference and returns a query-local FieldRef.
func (c *BindContext) ResolveField(qualifier string, name string, roles FieldRole) (FieldRef, DiagnosticSet) {
	if c == nil {
		return FieldRef{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	if bindIsRownumPseudoField(name) {
		return c.resolveRownumPseudoField(qualifier, name, roles)
	}
	if qualifier != "" {
		table, diagnostics := c.ResolveTable(qualifier)
		if diagnostics.BlocksNative() {
			return FieldRef{}, diagnostics
		}
		return c.resolveFieldInTable(table, name, roles)
	}

	matches := make([]FieldRef, 0, 1)
	for _, table := range c.tables {
		field, ok := table.Definition.Field(name)
		if !ok {
			continue
		}
		matches = append(matches, field.Ref(table.Instance, roles))
	}
	switch len(matches) {
	case 0:
		return FieldRef{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogFieldNotFound, PhaseBind, "field not found: "+name),
		})
	case 1:
		return matches[0], nil
	default:
		return FieldRef{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticAmbiguousField, PhaseBind, "ambiguous field reference: "+name),
		})
	}
}

// ResolveFunction resolves a SQL function name against the catalog.
func (c *BindContext) ResolveFunction(name string) (FunctionDefinition, DiagnosticSet) {
	if c == nil {
		return FunctionDefinition{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	if c.Catalog == nil {
		return FunctionDefinition{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "catalog is nil"),
		})
	}
	function, diagnostics := c.Catalog.Function(name)
	if diagnostics.BlocksNative() {
		return FunctionDefinition{}, c.record(diagnostics)
	}
	return function, nil
}

func (c *BindContext) resolveFieldInTable(table BoundTable, name string, roles FieldRole) (FieldRef, DiagnosticSet) {
	field, ok := table.Definition.Field(name)
	if !ok {
		return FieldRef{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogFieldNotFound, PhaseBind, "field not found: "+table.Instance.RefName()+"."+name),
		})
	}
	return field.Ref(table.Instance, roles), nil
}

func (c *BindContext) resolveRownumPseudoField(qualifier string, name string, roles FieldRole) (FieldRef, DiagnosticSet) {
	if qualifier != "" {
		table, diagnostics := c.ResolveTable(qualifier)
		if diagnostics.BlocksNative() {
			return FieldRef{}, diagnostics
		}
		return bindRownumPseudoField(table.Instance, name, roles), nil
	}
	switch len(c.tables) {
	case 0:
		return FieldRef{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogFieldNotFound, PhaseBind, "field not found: "+name),
		})
	case 1:
		return bindRownumPseudoField(c.tables[0].Instance, name, roles), nil
	default:
		return FieldRef{}, c.record(DiagnosticSet{
			ErrorDiagnostic(DiagnosticAmbiguousField, PhaseBind, "ambiguous field reference: "+name),
		})
	}
}

func bindRownumPseudoField(table TableInstance, name string, roles FieldRole) FieldRef {
	return FieldRef{
		Table:        table,
		Name:         name,
		PhysicalName: "@rownum",
		Type:         DataTypeInt,
		Index:        IndexBitmap,
		Roles:        roles,
		Encoding:     LegacyEncodingProfile("IntDirect", LegacyEncodingOptions{}),
	}
}

func bindIsRownumPseudoField(name string) bool {
	return strings.EqualFold(strings.TrimSpace(name), "@rownum")
}

func (c *BindContext) hasTableRef(ref string) bool {
	for _, table := range c.tables {
		if strings.EqualFold(table.Instance.RefName(), ref) {
			return true
		}
	}
	return false
}

func (c *BindContext) record(diagnostics DiagnosticSet) DiagnosticSet {
	c.diagnostics = append(c.diagnostics, diagnostics...)
	return diagnostics
}

func tableRefName(name string, alias string) string {
	if alias != "" {
		return alias
	}
	return name
}
