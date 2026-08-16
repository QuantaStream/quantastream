package qsbridge

import "strings"

// CatalogName identifies a logical database or schema namespace.
type CatalogName string

// DataType identifies the SQL-facing type category for a field or function.
type DataType string

const (
	// DataTypeUnknown means the type has not been bound.
	DataTypeUnknown DataType = ""
	// DataTypeBool identifies a boolean value.
	DataTypeBool DataType = "bool"
	// DataTypeInt identifies an integer value.
	DataTypeInt DataType = "int"
	// DataTypeFloat identifies a floating-point or decimal value.
	DataTypeFloat DataType = "float"
	// DataTypeString identifies a string value.
	DataTypeString DataType = "string"
	// DataTypeTime identifies a date, time, or timestamp value.
	DataTypeTime DataType = "time"
)

// FieldDefinition describes one catalog field before query binding.
type FieldDefinition struct {
	Name         string
	PhysicalName string
	Type         DataType
	Index        IndexKind
	Nullable     bool
	PrimaryKey   bool
	Searchable   bool
	Storage      StorageProfile
	Encoding     EncodingProfile
	Dictionary   DictionaryDefinition
}

// Ref binds the field definition to one table instance.
func (f FieldDefinition) Ref(table TableInstance, roles FieldRole) FieldRef {
	return FieldRef{
		Table:        table,
		Name:         f.Name,
		PhysicalName: f.PhysicalName,
		Type:         f.Type,
		Index:        f.Index,
		Nullable:     f.Nullable,
		PrimaryKey:   f.PrimaryKey,
		Roles:        roles,
		Encoding:     cloneEncodingProfile(f.Encoding),
		Dictionary:   cloneDictionaryDefinition(f.Dictionary),
	}
}

// TableDefinition describes one table known to the catalog.
type TableDefinition struct {
	Schema        string
	Name          string
	Fields        []FieldDefinition
	Relationships []RelationshipDefinition
	Storage       StorageProfile
}

// Field looks up a field by logical or physical name.
func (t TableDefinition) Field(name string) (FieldDefinition, bool) {
	for _, field := range t.Fields {
		if strings.EqualFold(field.Name, name) || (field.PhysicalName != "" && strings.EqualFold(field.PhysicalName, name)) {
			return field, true
		}
	}
	return FieldDefinition{}, false
}

// Instance creates a query-local table instance for this table definition.
func (t TableDefinition) Instance(id TableInstanceID, alias string) TableInstance {
	return TableInstance{
		ID:     id,
		Schema: t.Schema,
		Table:  t.Name,
		Alias:  alias,
	}
}

// SQLViewDefinition describes one logical, non-materialized SQL view.
type SQLViewDefinition struct {
	Schema       string
	Name         string
	SQL          string
	CanonicalSQL string
}

// RelationshipDefinition describes a catalog relationship between two fields.
type RelationshipDefinition struct {
	Name        string
	FromTable   string
	FromField   string
	ToTable     string
	ToField     string
	Direction   JoinDirection
	Cardinality string
	Encoding    RelationshipEncodingProfile
}

// ParentTable returns the referenced side of the relationship.
func (r RelationshipDefinition) ParentTable() string {
	switch r.Direction {
	case JoinParentToChild:
		return r.FromTable
	case JoinChildToParent:
		return r.ToTable
	default:
		if r.ToTable != "" {
			return r.ToTable
		}
		return r.FromTable
	}
}

// ChildTable returns the dependent side of the relationship.
func (r RelationshipDefinition) ChildTable() string {
	switch r.Direction {
	case JoinParentToChild:
		return r.ToTable
	case JoinChildToParent:
		return r.FromTable
	default:
		if r.FromTable != "" {
			return r.FromTable
		}
		return r.ToTable
	}
}

// ReferencesParentTable reports whether relationship depends on parentTable.
func (r RelationshipDefinition) ReferencesParentTable(parentTable string) bool {
	return strings.EqualFold(r.ParentTable(), parentTable)
}

// Edge binds the relationship definition to query-local field references.
func (r RelationshipDefinition) Edge(left FieldRef, right FieldRef) JoinEdge {
	return JoinEdge{
		Left:        left,
		Right:       right,
		Kind:        JoinKindInner,
		Nulls:       NullExtensionNone,
		On:          nil,
		Direction:   r.Direction,
		Cardinality: r.Cardinality,
		Encoding:    cloneRelationshipEncodingProfile(r.Encoding),
		Legal:       true,
	}
}

// StorageProfile describes physical storage traits without importing runtime storage types.
type StorageProfile struct {
	Engine      string
	Index       IndexKind
	Partitioned bool
	Searchable  bool
	Replicated  bool
}

// FunctionKind classifies a registered SQL function.
type FunctionKind string

const (
	// FunctionScalar identifies a scalar function.
	FunctionScalar FunctionKind = "scalar"
	// FunctionAggregate identifies an aggregate function.
	FunctionAggregate FunctionKind = "aggregate"
	// FunctionTable identifies a table-valued function.
	FunctionTable FunctionKind = "table"
)

// FunctionOrigin identifies where a registered SQL function comes from.
type FunctionOrigin string

const (
	// FunctionOriginUnknown means function provenance has not been classified.
	FunctionOriginUnknown FunctionOrigin = ""
	// FunctionOriginMySQLCompatible identifies functions intended to match MySQL semantics.
	FunctionOriginMySQLCompatible FunctionOrigin = "mysql_compatible"
	// FunctionOriginQuantaCustom identifies supported Quanta-specific SQL extensions.
	FunctionOriginQuantaCustom FunctionOrigin = "quanta_custom"
	// FunctionOriginLegacyCustom identifies legacy custom functions that should be reviewed.
	FunctionOriginLegacyCustom FunctionOrigin = "legacy_custom"
	// FunctionOriginAdapter identifies functions supplied by an embedding adapter.
	FunctionOriginAdapter FunctionOrigin = "adapter"
)

// FunctionPlacement identifies where a function may appear in SQL planning.
type FunctionPlacement string

const (
	// FunctionPlacementUnknown means placement has not been classified.
	FunctionPlacementUnknown FunctionPlacement = ""
	// FunctionPlacementExpression means the function behaves like a normal scalar expression.
	FunctionPlacementExpression FunctionPlacement = "expression"
	// FunctionPlacementAggregate means the function produces an aggregate slot.
	FunctionPlacementAggregate FunctionPlacement = "aggregate"
	// FunctionPlacementPredicate means the function is meaningful only as predicate/filter metadata.
	FunctionPlacementPredicate FunctionPlacement = "predicate"
	// FunctionPlacementTable means the function produces table-shaped data.
	FunctionPlacementTable FunctionPlacement = "table"
)

// FunctionBindingContext identifies where a function is allowed to bind.
type FunctionBindingContext string

const (
	// FunctionContextUnknown means the binding context has not been classified.
	FunctionContextUnknown FunctionBindingContext = ""
	// FunctionContextSQLExpression permits normal SQL scalar expression usage.
	FunctionContextSQLExpression FunctionBindingContext = "sql_expression"
	// FunctionContextSQLAggregate permits SQL aggregate usage.
	FunctionContextSQLAggregate FunctionBindingContext = "sql_aggregate"
	// FunctionContextCatalogDefault permits catalog default-value expressions.
	FunctionContextCatalogDefault FunctionBindingContext = "catalog_default"
	// FunctionContextTableSelector permits streaming/catalog table selector expressions.
	FunctionContextTableSelector FunctionBindingContext = "table_selector"
)

// FunctionDefinition describes a function the binder can resolve.
type FunctionDefinition struct {
	Name          string
	Kind          FunctionKind
	Origin        FunctionOrigin
	Placement     FunctionPlacement
	Contexts      []FunctionBindingContext
	Arguments     []DataType
	ReturnType    DataType
	Aliases       []string
	Native        bool
	Deterministic bool
}

// Matches reports whether name resolves to this function or one of its aliases.
func (f FunctionDefinition) Matches(name string) bool {
	if strings.EqualFold(f.Name, name) {
		return true
	}
	for _, alias := range f.Aliases {
		if strings.EqualFold(alias, name) {
			return true
		}
	}
	return false
}

// SupportsContext reports whether the function may be bound in context.
func (f FunctionDefinition) SupportsContext(context FunctionBindingContext) bool {
	if context == FunctionContextUnknown {
		return true
	}
	if len(f.Contexts) == 0 {
		return true
	}
	for _, candidate := range f.Contexts {
		if candidate == context {
			return true
		}
	}
	return false
}

// EffectivePlacement returns explicit placement or the default implied by function kind.
func (f FunctionDefinition) EffectivePlacement() FunctionPlacement {
	if f.Placement != FunctionPlacementUnknown {
		return f.Placement
	}
	switch f.Kind {
	case FunctionAggregate:
		return FunctionPlacementAggregate
	case FunctionTable:
		return FunctionPlacementTable
	case FunctionScalar:
		return FunctionPlacementExpression
	default:
		return FunctionPlacementUnknown
	}
}

// Catalog provides schema metadata required by binding.
type Catalog interface {
	Table(schema string, name string) (TableDefinition, DiagnosticSet)
	Relationship(name string) (RelationshipDefinition, DiagnosticSet)
	Function(name string) (FunctionDefinition, DiagnosticSet)
}

// ViewCatalog can resolve logical, non-materialized SQL views for planning.
type ViewCatalog interface {
	View(schema string, name string) (SQLViewDefinition, DiagnosticSet)
}

// DependentRelationshipCatalog can enumerate child relationships for parent-table operations.
type DependentRelationshipCatalog interface {
	DependentRelationships(schema string, parentTable string) ([]RelationshipDefinition, DiagnosticSet)
}

// MemoryCatalog is a small in-memory Catalog useful for tests and scaffolding.
type MemoryCatalog struct {
	Schemas       []CatalogSchemaDefinition
	Tables        []TableDefinition
	Views         []SQLViewDefinition
	Relationships []RelationshipDefinition
	Functions     []FunctionDefinition
}

// Table looks up a table definition by schema and name.
func (c MemoryCatalog) Table(schema string, name string) (TableDefinition, DiagnosticSet) {
	for _, table := range c.Tables {
		if schema != "" && !strings.EqualFold(table.Schema, schema) {
			continue
		}
		if strings.EqualFold(table.Name, name) {
			return table, nil
		}
	}
	return TableDefinition{}, DiagnosticSet{
		ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "table not found: "+qualifiedCatalogName(schema, name)),
	}
}

// View looks up a logical view definition by schema and name.
func (c MemoryCatalog) View(schema string, name string) (SQLViewDefinition, DiagnosticSet) {
	for _, view := range c.Views {
		if schema != "" && view.Schema != "" && !strings.EqualFold(view.Schema, schema) {
			continue
		}
		if strings.EqualFold(view.Name, name) {
			return cloneSQLViewDefinition(view), nil
		}
	}
	return SQLViewDefinition{}, DiagnosticSet{
		ErrorDiagnostic(DiagnosticCatalogViewNotFound, PhaseBind, "view not found: "+qualifiedCatalogName(schema, name)),
	}
}

// Relationship looks up a relationship definition by name.
func (c MemoryCatalog) Relationship(name string) (RelationshipDefinition, DiagnosticSet) {
	for _, relationship := range c.Relationships {
		if strings.EqualFold(relationship.Name, name) {
			return cloneRelationshipDefinition(relationship), nil
		}
	}
	return RelationshipDefinition{}, DiagnosticSet{
		ErrorDiagnostic(DiagnosticCatalogRelationshipNotFound, PhaseBind, "relationship not found: "+name),
	}
}

// DependentRelationships returns relationships whose dependent child points at parentTable.
func (c MemoryCatalog) DependentRelationships(schema string, parentTable string) ([]RelationshipDefinition, DiagnosticSet) {
	seen := make(map[string]struct{})
	dependencies := make([]RelationshipDefinition, 0)
	add := func(relationship RelationshipDefinition) {
		if !relationship.ReferencesParentTable(parentTable) {
			return
		}
		key := strings.ToLower(relationship.Name) + "\x00" + strings.ToLower(relationship.ChildTable()) + "\x00" + strings.ToLower(relationship.ParentTable())
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		dependencies = append(dependencies, cloneRelationshipDefinition(relationship))
	}
	for _, relationship := range c.Relationships {
		add(relationship)
	}
	for _, table := range c.Tables {
		if schema != "" && table.Schema != "" && !strings.EqualFold(table.Schema, schema) {
			continue
		}
		for _, relationship := range table.Relationships {
			add(relationship)
		}
	}
	return dependencies, nil
}

// Function looks up a function definition by name or alias.
func (c MemoryCatalog) Function(name string) (FunctionDefinition, DiagnosticSet) {
	for _, function := range c.Functions {
		if function.Matches(name) {
			return function, nil
		}
	}
	return FunctionDefinition{}, DiagnosticSet{
		ErrorDiagnostic(DiagnosticCatalogFunctionNotFound, PhaseBind, "function not found: "+name),
	}
}

func qualifiedCatalogName(schema string, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

// cloneRelationshipDefinition copies nested relationship encoding state for safe catalog lookups.
func cloneRelationshipDefinition(relationship RelationshipDefinition) RelationshipDefinition {
	cloned := relationship
	cloned.Encoding = cloneRelationshipEncodingProfile(relationship.Encoding)
	return cloned
}

// cloneRelationshipDefinitions copies relationship metadata for caller-owned mutation.
func cloneRelationshipDefinitions(relationships []RelationshipDefinition) []RelationshipDefinition {
	cloned := make([]RelationshipDefinition, 0, len(relationships))
	for _, relationship := range relationships {
		cloned = append(cloned, cloneRelationshipDefinition(relationship))
	}
	return cloned
}
