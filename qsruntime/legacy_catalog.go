package qsruntime

import (
	"context"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/hashicorp/consul/api"
)

// LegacyTableLoader loads a missing core table into the shared table cache.
type LegacyTableLoader func(tableCache *core.TableCacheStruct, name string) (*core.Table, error)

// LegacyTableCacheCatalog adapts the legacy core table cache to qsbridge.Catalog.
//
// Legacy schema loading may still be backed by Consul, but this adapter only
// receives a table-loader callback so Consul and KVStore details stay in the
// runtime adapter island.
type LegacyTableCacheCatalog struct {
	TableCache *core.TableCacheStruct
	LoadTable  LegacyTableLoader
	Functions  []qsbridge.FunctionDefinition
	ConfigDir  string
	Consul     *api.Client
}

// LegacyTableCacheCatalogFactory builds cached catalogs from the legacy table cache.
type LegacyTableCacheCatalogFactory struct {
	TableCache *core.TableCacheStruct
	LoadTable  LegacyTableLoader
	Functions  []qsbridge.FunctionDefinition
}

// NewRuntimeCatalog returns a process-local cached catalog over the legacy table cache.
func (f LegacyTableCacheCatalogFactory) NewRuntimeCatalog(ctx context.Context, config DirectRuntimeConfig) (qsbridge.Catalog, qsbridge.DiagnosticSet, error) {
	if f.TableCache == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseBind,
				"legacy table cache catalog factory has no table cache",
			),
		}, nil
	}
	loader := f.LoadTable
	if loader == nil && strings.TrimSpace(config.BaseDir) != "" {
		baseDir := config.BaseDir
		loader = func(tableCache *core.TableCacheStruct, name string) (*core.Table, error) {
			return core.LoadTable(tableCache, baseDir, nil, name, nil)
		}
	}
	functions := append([]qsbridge.FunctionDefinition(nil), f.Functions...)
	if len(functions) == 0 {
		functions = qsbridge.BuiltinSQLFunctionDefinitions()
	}
	var consul *api.Client
	if strings.TrimSpace(config.ConsulAddress) != "" {
		client, err := api.NewClient(&api.Config{Address: strings.TrimSpace(config.ConsulAddress)})
		if err != nil {
			return nil, nil, err
		}
		consul = client
	}
	return qsbridge.NewCachedCatalog(LegacyTableCacheCatalog{
		TableCache: f.TableCache,
		LoadTable:  loader,
		Functions:  functions,
		ConfigDir:  strings.TrimSpace(config.BaseDir),
		Consul:     consul,
	}), nil, nil
}

// Table looks up a table definition from the legacy cache.
func (c LegacyTableCacheCatalog) Table(schema string, name string) (qsbridge.TableDefinition, qsbridge.DiagnosticSet) {
	table, ok := c.legacyTable(name)
	if !ok {
		loaded, err := c.loadLegacyTable(name)
		if err != nil {
			return qsbridge.TableDefinition{}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCatalogTableNotFound, qsbridge.PhaseBind, "table not found: "+qualifiedCatalogName(schema, name)+" ("+err.Error()+")"),
			}
		}
		if loaded != nil {
			table = loaded
			ok = true
		}
	}
	if !ok {
		return qsbridge.TableDefinition{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCatalogTableNotFound, qsbridge.PhaseBind, "table not found: "+qualifiedCatalogName(schema, name)),
		}
	}
	return c.tableDefinition(schema, table), nil
}

// View looks up a logical view definition from Consul or the file-backed catalog.
func (c LegacyTableCacheCatalog) View(schema string, name string) (qsbridge.SQLViewDefinition, qsbridge.DiagnosticSet) {
	if c.Consul != nil {
		view, err := shared.LoadViewDefinitionConsul(c.Consul, name)
		if err != nil {
			return qsbridge.SQLViewDefinition{}, legacyCatalogViewDiagnostic(schema, name, err)
		}
		return legacyCatalogSQLViewDefinition(schema, view), nil
	}
	configDir := strings.TrimSpace(c.ConfigDir)
	if configDir == "" {
		return qsbridge.SQLViewDefinition{}, legacyCatalogViewDiagnostic(schema, name, os.ErrNotExist)
	}
	if shared.CatalogObjectsFileExists(configDir) {
		active, err := shared.CatalogViewActive(configDir, schema, name)
		if err != nil {
			return qsbridge.SQLViewDefinition{}, legacyCatalogViewDiagnostic(schema, name, err)
		}
		if !active {
			return qsbridge.SQLViewDefinition{}, legacyCatalogViewDiagnostic(schema, name, os.ErrNotExist)
		}
	}
	view, err := shared.LoadViewDefinition(configDir, name)
	if err != nil {
		return qsbridge.SQLViewDefinition{}, legacyCatalogViewDiagnostic(schema, name, err)
	}
	return legacyCatalogSQLViewDefinition(schema, view), nil
}

// ListSchemas returns schemas visible through the legacy adapter.
func (c LegacyTableCacheCatalog) ListSchemas() ([]qsbridge.CatalogSchemaDefinition, qsbridge.DiagnosticSet) {
	seen := map[string]string{"quanta": "quanta"}
	for _, table := range c.cachedTables() {
		_ = table
		seen["quanta"] = "quanta"
	}
	if strings.TrimSpace(c.ConfigDir) != "" {
		catalog, err := shared.LoadCatalogObjectsFile(c.ConfigDir)
		if err != nil {
			return nil, legacyCatalogMetadataDiagnostic("list schemas", err)
		}
		for _, object := range catalog.Objects {
			schema := strings.TrimSpace(object.SchemaName)
			if schema == "" {
				continue
			}
			seen[strings.ToLower(schema)] = schema
		}
	}
	names := make([]string, 0, len(seen))
	for key := range seen {
		names = append(names, key)
	}
	sort.Strings(names)
	schemas := make([]qsbridge.CatalogSchemaDefinition, 0, len(names))
	for _, key := range names {
		schemas = append(schemas, qsbridge.CatalogSchemaDefinition{Name: seen[key]})
	}
	return schemas, nil
}

// ListTables returns loaded or file-catalog-active tables for one schema.
func (c LegacyTableCacheCatalog) ListTables(schema string) ([]qsbridge.TableDefinition, qsbridge.DiagnosticSet) {
	schema = strings.TrimSpace(schema)
	seen := make(map[string]qsbridge.TableDefinition)
	for _, table := range c.cachedTables() {
		if table == nil || strings.TrimSpace(table.Name) == "" {
			continue
		}
		seen[strings.ToLower(table.Name)] = c.tableDefinition(schema, table)
	}
	if strings.TrimSpace(c.ConfigDir) != "" {
		names, err := shared.ActiveCatalogTables(c.ConfigDir, schema)
		if err != nil {
			return nil, legacyCatalogMetadataDiagnostic("list tables", err)
		}
		for _, name := range names {
			key := strings.ToLower(strings.TrimSpace(name))
			if key == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			table, diagnostics := c.Table(schema, name)
			if diagnostics.BlocksNative() {
				continue
			}
			seen[key] = table
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	tables := make([]qsbridge.TableDefinition, 0, len(keys))
	for _, key := range keys {
		tables = append(tables, seen[key])
	}
	return tables, nil
}

// ListColumns returns field metadata for a table.
func (c LegacyTableCacheCatalog) ListColumns(schema string, table string) ([]qsbridge.FieldDefinition, qsbridge.DiagnosticSet) {
	definition, diagnostics := c.Table(schema, table)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	fields := make([]qsbridge.FieldDefinition, 0, len(definition.Fields))
	for _, field := range definition.Fields {
		fields = append(fields, field)
	}
	return fields, nil
}

// ListViews returns active logical views for one schema.
func (c LegacyTableCacheCatalog) ListViews(schema string) ([]qsbridge.SQLViewDefinition, qsbridge.DiagnosticSet) {
	if c.Consul != nil {
		pairs, _, err := c.Consul.KV().List(shared.ConsulCatalogViewsPrefix+"/", nil)
		if err != nil {
			return nil, legacyCatalogMetadataDiagnostic("list views", err)
		}
		seen := make(map[string]struct{})
		views := make([]qsbridge.SQLViewDefinition, 0)
		for _, pair := range pairs {
			if pair == nil || path.Base(pair.Key) != "definition.yaml" {
				continue
			}
			name := path.Base(path.Dir(pair.Key))
			key := strings.ToLower(strings.TrimSpace(name))
			if key == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			view, err := shared.LoadViewDefinitionConsul(c.Consul, name)
			if err != nil {
				return nil, legacyCatalogMetadataDiagnostic("list views", err)
			}
			if schema != "" && view.SchemaName != "" && !strings.EqualFold(view.SchemaName, schema) {
				continue
			}
			views = append(views, legacyCatalogSQLViewDefinition(schema, view))
		}
		sort.Slice(views, func(i, j int) bool {
			return strings.ToLower(views[i].Name) < strings.ToLower(views[j].Name)
		})
		return views, nil
	}
	configDir := strings.TrimSpace(c.ConfigDir)
	if configDir == "" {
		return nil, nil
	}
	names, err := shared.ActiveCatalogViews(configDir, schema)
	if err != nil {
		return nil, legacyCatalogMetadataDiagnostic("list views", err)
	}
	views := make([]qsbridge.SQLViewDefinition, 0, len(names))
	for _, name := range names {
		view, err := shared.LoadViewDefinition(configDir, name)
		if err != nil {
			return nil, legacyCatalogMetadataDiagnostic("list views", err)
		}
		views = append(views, legacyCatalogSQLViewDefinition(schema, view))
	}
	sort.Slice(views, func(i, j int) bool {
		return strings.ToLower(views[i].Name) < strings.ToLower(views[j].Name)
	})
	return views, nil
}

// Relationship looks up a relationship by name from cached table metadata.
func (c LegacyTableCacheCatalog) Relationship(name string) (qsbridge.RelationshipDefinition, qsbridge.DiagnosticSet) {
	for _, table := range c.cachedTables() {
		for _, relationship := range c.relationshipsForTable(table) {
			if strings.EqualFold(relationship.Name, name) {
				return relationship, nil
			}
		}
	}
	return qsbridge.RelationshipDefinition{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCatalogRelationshipNotFound, qsbridge.PhaseBind, "relationship not found: "+name),
	}
}

// DependentRelationships returns child relationships that reference parentTable.
func (c LegacyTableCacheCatalog) DependentRelationships(schema string, parentTable string) ([]qsbridge.RelationshipDefinition, qsbridge.DiagnosticSet) {
	_ = schema
	seen := make(map[string]struct{})
	dependencies := make([]qsbridge.RelationshipDefinition, 0)
	for _, table := range c.cachedTables() {
		for _, relationship := range c.relationshipsForTable(table) {
			if !relationship.ReferencesParentTable(parentTable) {
				continue
			}
			key := strings.ToLower(relationship.Name) + "\x00" + strings.ToLower(relationship.ChildTable()) + "\x00" + strings.ToLower(relationship.ParentTable())
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			dependencies = append(dependencies, relationship)
		}
	}
	return dependencies, nil
}

// Function looks up an adapter-supplied function definition.
func (c LegacyTableCacheCatalog) Function(name string) (qsbridge.FunctionDefinition, qsbridge.DiagnosticSet) {
	for _, function := range c.Functions {
		if function.Matches(name) {
			return function, nil
		}
	}
	return qsbridge.FunctionDefinition{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCatalogFunctionNotFound, qsbridge.PhaseBind, "function not found: "+name),
	}
}

func legacyCatalogSQLViewDefinition(schema string, view shared.ViewDefinition) qsbridge.SQLViewDefinition {
	schemaName := strings.TrimSpace(view.SchemaName)
	if schemaName == "" {
		schemaName = strings.TrimSpace(schema)
	}
	sql := strings.TrimSpace(view.SQL)
	canonical := strings.TrimSpace(view.CanonicalSQL)
	if sql == "" {
		sql = canonical
	}
	return qsbridge.SQLViewDefinition{
		Schema:       schemaName,
		Name:         strings.TrimSpace(view.ViewName),
		SQL:          sql,
		CanonicalSQL: canonical,
	}
}

func legacyCatalogViewDiagnostic(schema string, name string, err error) qsbridge.DiagnosticSet {
	message := "view not found: " + qualifiedCatalogName(schema, name)
	if err != nil && !os.IsNotExist(err) {
		message += " (" + err.Error() + ")"
	}
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCatalogViewNotFound, qsbridge.PhaseBind, message),
	}
}

func legacyCatalogMetadataDiagnostic(action string, err error) qsbridge.DiagnosticSet {
	message := action
	if err != nil {
		message += ": " + err.Error()
	}
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseBind, message),
	}
}

func (c LegacyTableCacheCatalog) loadLegacyTable(name string) (*core.Table, error) {
	if c.TableCache == nil || c.LoadTable == nil {
		return nil, nil
	}
	return c.LoadTable(c.TableCache, name)
}

func (c LegacyTableCacheCatalog) legacyTable(name string) (*core.Table, bool) {
	if c.TableCache == nil {
		return nil, false
	}
	c.TableCache.TableCacheLock.RLock()
	defer c.TableCache.TableCacheLock.RUnlock()
	for tableName, table := range c.TableCache.TableCache {
		if strings.EqualFold(tableName, name) || (table != nil && strings.EqualFold(table.Name, name)) {
			return table, table != nil
		}
	}
	return nil, false
}

func (c LegacyTableCacheCatalog) cachedTables() []*core.Table {
	if c.TableCache == nil {
		return nil
	}
	c.TableCache.TableCacheLock.RLock()
	defer c.TableCache.TableCacheLock.RUnlock()
	tables := make([]*core.Table, 0, len(c.TableCache.TableCache))
	for _, table := range c.TableCache.TableCache {
		if table != nil {
			tables = append(tables, table)
		}
	}
	return tables
}

func (c LegacyTableCacheCatalog) tableDefinition(schema string, table *core.Table) qsbridge.TableDefinition {
	definition := qsbridge.TableDefinition{
		Schema: schema,
		Name:   table.Name,
		Storage: qsbridge.StorageProfile{
			Engine:      "legacy_quanta",
			Partitioned: table.TimeQuantumType != "",
		},
	}
	definition.Fields = make([]qsbridge.FieldDefinition, 0, len(table.Attributes))
	for _, attribute := range legacyCatalogOrderedAttributes(table.Attributes) {
		if attribute.FieldName == "" && attribute.SourceName == "" {
			continue
		}
		field := legacyFieldDefinition(schema, table, attribute)
		definition.Fields = append(definition.Fields, field)
	}
	definition.Relationships = c.relationshipsForTable(table)
	return definition
}

func legacyCatalogOrderedAttributes(attributes []core.Attribute) []core.Attribute {
	type orderedAttribute struct {
		attribute core.Attribute
		position  int
	}
	ordered := make([]orderedAttribute, 0, len(attributes))
	for position, attribute := range attributes {
		ordered = append(ordered, orderedAttribute{attribute: attribute, position: position})
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		leftOrdinal := ordered[i].attribute.SourceOrdinal
		rightOrdinal := ordered[j].attribute.SourceOrdinal
		switch {
		case leftOrdinal > 0 && rightOrdinal > 0 && leftOrdinal != rightOrdinal:
			return leftOrdinal < rightOrdinal
		case leftOrdinal > 0 && rightOrdinal <= 0:
			return true
		case leftOrdinal <= 0 && rightOrdinal > 0:
			return false
		default:
			return ordered[i].position < ordered[j].position
		}
	})
	result := make([]core.Attribute, 0, len(ordered))
	for _, item := range ordered {
		if item.attribute.System {
			continue
		}
		result = append(result, item.attribute)
	}
	return result
}

func legacyFieldDefinition(schema string, table *core.Table, attribute core.Attribute) qsbridge.FieldDefinition {
	name := attribute.FieldName
	if name == "" {
		name = attribute.SourceName
	}
	encoding := qsbridge.LegacyEncodingProfile(attribute.MappingStrategy, qsbridge.LegacyEncodingOptions{
		NonExclusive:   attribute.NonExclusive,
		Searchable:     attribute.Searchable,
		Scale:          attribute.Scale,
		Granularity:    LegacyTimeBSIGranularity(attribute.MapperConfig),
		PrefixLength:   legacyStringLexBSIPrefixLength(attribute.MapperConfig),
		MaxLength:      legacyStringLexBSIMaxLength(attribute),
		RemainderStore: "kv",
	})
	field := qsbridge.FieldDefinition{
		Name:         name,
		PhysicalName: legacyPhysicalFieldName(name, attribute.SourceName),
		Type:         legacyDataType(attribute.Type),
		Index:        legacyIndexKind(encoding),
		Nullable:     !attribute.Required,
		PrimaryKey:   legacyPrimaryKeyContains(table.PrimaryKey, name) || strings.EqualFold(table.TimeQuantumField, name),
		Searchable:   attribute.Searchable,
		Storage: qsbridge.StorageProfile{
			Engine:     "legacy_quanta",
			Index:      legacyIndexKind(encoding),
			Searchable: attribute.Searchable,
		},
		Encoding: encoding,
	}
	if encoding.Kind == qsbridge.EncodingStringEnum {
		field.Dictionary = qsbridge.DictionaryDefinition{
			Ref: qsbridge.DictionaryRef{
				Schema: schema,
				Table:  table.Name,
				Field:  name,
			},
			Cardinality: uint64(len(attribute.Values)),
			UpdateMode:  qsbridge.DictionaryUpdateAppendOnly,
			Consistency: qsbridge.DictionaryConsistencyVersionedDistributed,
			Capabilities: qsbridge.DictionaryCapabilities{
				qsbridge.DictionaryCapabilityStableIDs,
				qsbridge.DictionaryCapabilityPrefixMatch,
				qsbridge.DictionaryCapabilityMutable,
			},
		}
	}
	return field
}

func legacyPhysicalFieldName(fieldName string, sourceName string) string {
	sourceName = strings.TrimSpace(sourceName)
	if sourceName == "" || strings.HasPrefix(sourceName, "/") {
		return fieldName
	}
	return sourceName
}

func (c LegacyTableCacheCatalog) relationshipsForTable(table *core.Table) []qsbridge.RelationshipDefinition {
	if table == nil {
		return nil
	}
	relationships := make([]qsbridge.RelationshipDefinition, 0)
	for _, attribute := range table.Attributes {
		if !strings.EqualFold(attribute.MappingStrategy, "ParentRelation") || attribute.ForeignKey == "" {
			continue
		}
		toTable, toField := parseLegacyForeignKey(attribute.ForeignKey)
		if toTable == "" {
			continue
		}
		fromField := attribute.FieldName
		if fromField == "" {
			fromField = attribute.SourceName
		}
		name := table.Name + "." + fromField + "->" + toTable
		if toField != "" {
			name += "." + toField
		}
		relationships = append(relationships, qsbridge.RelationshipDefinition{
			Name:      name,
			FromTable: table.Name,
			FromField: fromField,
			ToTable:   toTable,
			ToField:   toField,
			Direction: qsbridge.JoinChildToParent,
			Encoding: qsbridge.RelationshipEncodingProfile{
				Kind:       qsbridge.RelationshipEncodingVector,
				LegacyName: "ParentRelation",
				Capabilities: qsbridge.RelationshipCapabilities{
					qsbridge.RelationshipCapabilityParentLookup,
					qsbridge.RelationshipCapabilityJoinReduction,
					qsbridge.RelationshipCapabilitySemiJoin,
					qsbridge.RelationshipCapabilityAntiJoinDifference,
				},
			},
		})
		if attribute.RelationshipArtifacts.ParentToChild {
			last := &relationships[len(relationships)-1]
			last.Encoding.Capabilities = appendRelationshipCapabilityOnce(last.Encoding.Capabilities, qsbridge.RelationshipCapabilityChildExpansion)
		}
	}
	return relationships
}

func appendRelationshipCapabilityOnce(capabilities qsbridge.RelationshipCapabilities, capability qsbridge.RelationshipCapability) qsbridge.RelationshipCapabilities {
	if capabilities.Has(capability) {
		return capabilities
	}
	return append(capabilities, capability)
}

func legacyDataType(legacyType string) qsbridge.DataType {
	switch strings.ToLower(strings.TrimSpace(legacyType)) {
	case "integer", "int":
		return qsbridge.DataTypeInt
	case "float", "number", "decimal":
		return qsbridge.DataTypeFloat
	case "date", "datetime", "time":
		return qsbridge.DataTypeTime
	case "boolean", "bool":
		return qsbridge.DataTypeBool
	case "string":
		return qsbridge.DataTypeString
	default:
		return qsbridge.DataTypeUnknown
	}
}

func legacyStringLexBSIPrefixLength(config map[string]string) int {
	for _, key := range []string{"length", "prefixLength", "chars", "characters"} {
		if raw, ok := config[key]; ok {
			value, err := strconv.Atoi(strings.TrimSpace(raw))
			if err == nil {
				return value
			}
			return 0
		}
	}
	return 0
}

func legacyStringLexBSIMaxLength(attribute core.Attribute) int {
	if !strings.EqualFold(strings.TrimSpace(attribute.MappingStrategy), "StringLexBSI") {
		return 0
	}
	prefixLength := legacyStringLexBSIPrefixLength(attribute.MapperConfig)
	if prefixLength <= 0 {
		return 0
	}
	if attribute.Size > 0 {
		return attribute.Size
	}
	return -1
}

// LegacyTimeBSIGranularity reads the catalog granularity option for TimestampBSI fields.
func LegacyTimeBSIGranularity(config map[string]string) qsbridge.TimeGranularity {
	for _, key := range []string{"granularity", "precision", "unit"} {
		if raw, ok := config[key]; ok {
			switch strings.ToLower(strings.TrimSpace(raw)) {
			case "s", "sec", "second", "seconds":
				return qsbridge.TimeGranularitySecond
			case "ms", "milli", "millisecond", "milliseconds":
				return qsbridge.TimeGranularityMillisecond
			case "us", "micro", "microsecond", "microseconds":
				return qsbridge.TimeGranularityMicrosecond
			case "ns", "nano", "nanosecond", "nanoseconds":
				return qsbridge.TimeGranularityNanosecond
			}
			return qsbridge.TimeGranularityUnknown
		}
	}
	return qsbridge.TimeGranularityUnknown
}

func legacyIndexKind(encoding qsbridge.EncodingProfile) qsbridge.IndexKind {
	switch encoding.Kind {
	case qsbridge.EncodingStringEnum:
		return qsbridge.IndexStringEnum
	case qsbridge.EncodingBackingString:
		return qsbridge.IndexBackingString
	case qsbridge.EncodingNumericBSI, qsbridge.EncodingStringLexBSI, qsbridge.EncodingUUIDBSI, qsbridge.EncodingRelation:
		return qsbridge.IndexBSI
	case qsbridge.EncodingTimeBSI:
		return qsbridge.IndexDateTime
	case qsbridge.EncodingBitmap:
		return qsbridge.IndexBitmap
	default:
		return qsbridge.IndexUnknown
	}
}

func legacyPrimaryKeyContains(primaryKey string, fieldName string) bool {
	for _, part := range strings.Split(primaryKey, "+") {
		if strings.EqualFold(strings.TrimSpace(part), fieldName) {
			return true
		}
	}
	return false
}

func parseLegacyForeignKey(foreignKey string) (string, string) {
	parts := strings.SplitN(strings.TrimSpace(foreignKey), ".", 2)
	if len(parts) == 0 {
		return "", ""
	}
	table := strings.TrimSpace(parts[0])
	if len(parts) == 1 {
		return table, ""
	}
	return table, strings.TrimSpace(parts[1])
}

func qualifiedCatalogName(schema string, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

// LegacyTableCacheDictionaryResolver resolves mutable legacy StringEnum labels from a live table cache.
//
// StringEnum dictionaries are KVStore-backed in legacy Quanta and table caches
// can learn new labels as mutations flow through a running source. This resolver
// intentionally reads the cache on each lookup so qsbridge lowering observes
// labels inserted after SQLRuntime construction.
type LegacyTableCacheDictionaryResolver struct {
	TableCache          *core.TableCacheStruct
	FallbackTableCaches []*core.TableCacheStruct
	Schema              string
}

// Dictionary returns mutable StringEnum dictionary metadata for a field.
func (r LegacyTableCacheDictionaryResolver) Dictionary(ref qsbridge.DictionaryRef) (qsbridge.DictionaryDefinition, qsbridge.DiagnosticSet) {
	attribute, ok := r.attribute(ref)
	if !ok {
		return qsbridge.DictionaryDefinition{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticDictionaryNotFound, qsbridge.PhaseBind, "dictionary not found: "+ref.QualifiedName()),
		}
	}
	_ = attribute.RefreshStringEnumValues()
	return qsbridge.DictionaryDefinition{
		Ref:         r.normalizedRef(ref),
		Version:     qsbridge.DictionaryVersion("legacy-live-table-cache"),
		Cardinality: uint64(len(attribute.Values)),
		UpdateMode:  qsbridge.DictionaryUpdateAppendOnly,
		Consistency: qsbridge.DictionaryConsistencyVersionedDistributed,
		Capabilities: qsbridge.DictionaryCapabilities{
			qsbridge.DictionaryCapabilityStableIDs,
			qsbridge.DictionaryCapabilityPrefixMatch,
			qsbridge.DictionaryCapabilityMutable,
		},
	}, nil
}

// LookupLabel resolves a StringEnum label from live legacy table metadata.
func (r LegacyTableCacheDictionaryResolver) LookupLabel(ref qsbridge.DictionaryRef, label string) (qsbridge.DictionaryEntry, qsbridge.DiagnosticSet) {
	attributes := r.attributes(ref)
	if len(attributes) == 0 {
		return qsbridge.DictionaryEntry{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticDictionaryNotFound, qsbridge.PhaseBind, "dictionary not found: "+ref.QualifiedName()),
		}
	}
	if entry, ok := r.lookupLabelInAttributes(ref, attributes, label); ok {
		return entry, nil
	}
	for _, attribute := range attributes {
		_ = attribute.RefreshStringEnumValues()
	}
	if entry, ok := r.lookupLabelInAttributes(ref, attributes, label); ok {
		return entry, nil
	}
	return qsbridge.DictionaryEntry{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticDictionaryLabelNotFound, qsbridge.PhaseBind, "dictionary label not found: "+ref.QualifiedName()),
	}
}

// LookupID resolves an encoded StringEnum id from live legacy table metadata.
func (r LegacyTableCacheDictionaryResolver) LookupID(ref qsbridge.DictionaryRef, id qsbridge.StringEnumID) (qsbridge.DictionaryEntry, qsbridge.DiagnosticSet) {
	attributes := r.attributes(ref)
	if len(attributes) == 0 {
		return qsbridge.DictionaryEntry{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticDictionaryNotFound, qsbridge.PhaseBind, "dictionary not found: "+ref.QualifiedName()),
		}
	}
	if entry, ok := r.lookupIDInAttributes(ref, attributes, id); ok {
		return entry, nil
	}
	for _, attribute := range attributes {
		_ = attribute.RefreshStringEnumValues()
	}
	if entry, ok := r.lookupIDInAttributes(ref, attributes, id); ok {
		return entry, nil
	}
	return qsbridge.DictionaryEntry{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticDictionaryIDNotFound, qsbridge.PhaseBind, "dictionary id not found: "+ref.QualifiedName()),
	}
}

// LookupPrefix resolves all StringEnum ids whose labels share a prefix from live legacy metadata.
func (r LegacyTableCacheDictionaryResolver) LookupPrefix(ref qsbridge.DictionaryRef, prefix string) ([]qsbridge.DictionaryEntry, qsbridge.DiagnosticSet) {
	attributes := r.attributes(ref)
	if len(attributes) == 0 {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticDictionaryNotFound, qsbridge.PhaseBind, "dictionary not found: "+ref.QualifiedName()),
		}
	}
	for _, attribute := range attributes {
		_ = attribute.RefreshStringEnumValues()
	}
	matches := make([]qsbridge.DictionaryEntry, 0)
	seen := make(map[qsbridge.StringEnumID]struct{})
	for _, attribute := range attributes {
		for _, value := range attribute.Values {
			label := fmt.Sprint(value.Value)
			if !strings.HasPrefix(label, prefix) {
				continue
			}
			id := qsbridge.StringEnumID(value.RowID)
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			matches = append(matches, qsbridge.DictionaryEntry{
				Ref:     r.normalizedRef(ref),
				Label:   label,
				ID:      id,
				Version: qsbridge.DictionaryVersion("legacy-live-table-cache"),
			})
		}
	}
	return matches, nil
}

func (r LegacyTableCacheDictionaryResolver) lookupLabelInAttributes(ref qsbridge.DictionaryRef, attributes []*core.Attribute, label string) (qsbridge.DictionaryEntry, bool) {
	for _, attribute := range attributes {
		for _, value := range attribute.Values {
			if fmt.Sprint(value.Value) == label {
				return qsbridge.DictionaryEntry{
					Ref:     r.normalizedRef(ref),
					Label:   label,
					ID:      qsbridge.StringEnumID(value.RowID),
					Version: qsbridge.DictionaryVersion("legacy-live-table-cache"),
				}, true
			}
		}
	}
	return qsbridge.DictionaryEntry{}, false
}

func (r LegacyTableCacheDictionaryResolver) lookupIDInAttributes(ref qsbridge.DictionaryRef, attributes []*core.Attribute, id qsbridge.StringEnumID) (qsbridge.DictionaryEntry, bool) {
	for _, attribute := range attributes {
		for _, value := range attribute.Values {
			if qsbridge.StringEnumID(value.RowID) == id {
				return qsbridge.DictionaryEntry{
					Ref:     r.normalizedRef(ref),
					Label:   fmt.Sprint(value.Value),
					ID:      id,
					Version: qsbridge.DictionaryVersion("legacy-live-table-cache"),
				}, true
			}
		}
	}
	return qsbridge.DictionaryEntry{}, false
}

func (r LegacyTableCacheDictionaryResolver) attribute(ref qsbridge.DictionaryRef) (*core.Attribute, bool) {
	attributes := r.attributes(ref)
	if len(attributes) == 0 {
		return nil, false
	}
	return attributes[0], true
}

func (r LegacyTableCacheDictionaryResolver) attributes(ref qsbridge.DictionaryRef) []*core.Attribute {
	var attributes []*core.Attribute
	for _, tableCache := range r.tableCaches() {
		if tableCache == nil {
			continue
		}
		tableCache.TableCacheLock.RLock()
		for tableName, table := range tableCache.TableCache {
			if table == nil || (!strings.EqualFold(tableName, ref.Table) && !strings.EqualFold(table.Name, ref.Table)) {
				continue
			}
			for i := range table.Attributes {
				attribute := &table.Attributes[i]
				fieldName := attribute.FieldName
				if fieldName == "" {
					fieldName = attribute.SourceName
				}
				if strings.EqualFold(fieldName, ref.Field) && qsbridge.LegacyEncodingProfile(attribute.MappingStrategy, qsbridge.LegacyEncodingOptions{}).Kind == qsbridge.EncodingStringEnum {
					attributes = append(attributes, attribute)
				}
			}
		}
		tableCache.TableCacheLock.RUnlock()
	}
	return attributes
}

func (r LegacyTableCacheDictionaryResolver) tableCaches() []*core.TableCacheStruct {
	if len(r.FallbackTableCaches) == 0 {
		return []*core.TableCacheStruct{r.TableCache}
	}
	caches := make([]*core.TableCacheStruct, 0, 1+len(r.FallbackTableCaches))
	caches = append(caches, r.TableCache)
	caches = append(caches, r.FallbackTableCaches...)
	return caches
}

func (r LegacyTableCacheDictionaryResolver) normalizedRef(ref qsbridge.DictionaryRef) qsbridge.DictionaryRef {
	if ref.Schema != "" {
		return ref
	}
	ref.Schema = r.Schema
	return ref
}
