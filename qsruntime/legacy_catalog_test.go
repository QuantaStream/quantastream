package qsruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
)

func TestLegacyTableCacheDictionaryResolverSeesLiveStringEnumLabels(t *testing.T) {
	cache := legacyCatalogTestCache()
	resolver := LegacyTableCacheDictionaryResolver{TableCache: cache, Schema: "quanta"}
	ref := qsbridge.DictionaryRef{Schema: "quanta", Table: "customers_qa", Field: "phoneType"}

	cache.TableCacheLock.Lock()
	for _, table := range cache.TableCache {
		if table.Name != "customers_qa" {
			continue
		}
		for i := range table.Attributes {
			if table.Attributes[i].FieldName == "phoneType" {
				table.Attributes[i].Values = append(table.Attributes[i].Values, shared.Value{Value: "business", RowID: 3})
			}
		}
	}
	cache.TableCacheLock.Unlock()

	entry, diagnostics := resolver.LookupLabel(ref, "business")
	if diagnostics.BlocksNative() {
		t.Fatalf("lookup diagnostics = %#v", diagnostics)
	}
	if entry.ID != 3 || entry.Label != "business" {
		t.Fatalf("entry = %#v, want business id 3", entry)
	}
}

func TestLegacyTableCacheDictionaryResolverFallsBackToPreloadedCatalogCache(t *testing.T) {
	live := legacyCatalogTestCache()
	catalog := legacyCatalogTestCache()
	ref := qsbridge.DictionaryRef{Schema: "quanta", Table: "region", Field: "r_name"}
	resolver := LegacyTableCacheDictionaryResolver{
		TableCache:          live,
		FallbackTableCaches: []*core.TableCacheStruct{catalog},
		Schema:              "quanta",
	}

	catalog.TableCache["region"] = &core.Table{
		BasicTable: &shared.BasicTable{Name: "region"},
		Attributes: []core.Attribute{{
			BasicAttribute: &shared.BasicAttribute{
				FieldName:       "r_name",
				Type:            "String",
				MappingStrategy: "StringEnum",
				Values: []shared.Value{
					{Value: "ASIA", RowID: 4},
					{Value: "EUROPE", RowID: 5},
				},
			},
		}},
	}

	entry, diagnostics := resolver.LookupLabel(ref, "EUROPE")
	if diagnostics.BlocksNative() {
		t.Fatalf("lookup diagnostics = %#v", diagnostics)
	}
	if entry.ID != 5 || entry.Label != "EUROPE" {
		t.Fatalf("entry = %#v, want EUROPE id 5", entry)
	}

	byID, diagnostics := resolver.LookupID(ref, 4)
	if diagnostics.BlocksNative() {
		t.Fatalf("lookup id diagnostics = %#v", diagnostics)
	}
	if byID.Label != "ASIA" {
		t.Fatalf("entry = %#v, want ASIA", byID)
	}
}
func TestLegacyTableCacheCatalogFactoryReturnsCachedCatalog(t *testing.T) {
	cache := legacyCatalogTestCache()
	factory := LegacyTableCacheCatalogFactory{TableCache: cache}

	catalog, diagnostics, err := factory.NewRuntimeCatalog(context.Background(), NewDirectRuntimeConfig("", "", 0, 0))

	if err != nil {
		t.Fatalf("new runtime catalog: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if _, ok := catalog.(*qsbridge.CachedCatalog); !ok {
		t.Fatalf("catalog = %T, want *qsbridge.CachedCatalog", catalog)
	}
	table, tableDiagnostics := catalog.Table("quanta", "orders")
	if tableDiagnostics.BlocksNative() {
		t.Fatalf("table diagnostics = %#v, want none", tableDiagnostics)
	}
	if table.Name != "orders" {
		t.Fatalf("table name = %q, want orders", table.Name)
	}
}

func TestLegacyTableCacheCatalogFactoryDefaultsSQLBuiltins(t *testing.T) {
	catalog, diagnostics, err := LegacyTableCacheCatalogFactory{TableCache: legacyCatalogTestCache()}.NewRuntimeCatalog(
		context.Background(),
		NewDirectRuntimeConfig("", "", 0, 0),
	)
	if err != nil {
		t.Fatalf("new runtime catalog: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	for _, name := range []string{"count", "todate"} {
		function, functionDiagnostics := catalog.Function(name)
		if functionDiagnostics.BlocksNative() {
			t.Fatalf("Function(%q) diagnostics = %#v, want none", name, functionDiagnostics)
		}
		if !function.Matches(name) {
			t.Fatalf("Function(%q) = %#v", name, function)
		}
	}
}

func TestLegacyTableCacheCatalogFactoryReportsMissingCache(t *testing.T) {
	catalog, diagnostics, err := LegacyTableCacheCatalogFactory{}.NewRuntimeCatalog(context.Background(), DirectRuntimeConfig{})

	if err != nil {
		t.Fatalf("new runtime catalog: %v", err)
	}
	if catalog != nil {
		t.Fatalf("catalog = %#v, want nil", catalog)
	}
	assertRuntimeDiagnosticCode(t, diagnostics, qsbridge.DiagnosticInternalInvariant)
}

func TestLegacyTableCacheCatalogFactoryLoadsMissingTableFromConfigPath(t *testing.T) {
	baseDir := t.TempDir()
	tableDir := filepath.Join(baseDir, "catalog_load_qa")
	if err := os.Mkdir(tableDir, 0755); err != nil {
		t.Fatalf("mkdir table fixture: %v", err)
	}
	schema := []byte(`tableName: catalog_load_qa
primaryKey: id
attributes:
- fieldName: id
  sourceName: id
  mappingStrategy: IntBSI
  type: Integer
- fieldName: city
  sourceName: city
  mappingStrategy: StringEnum
  type: String
`)
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), schema, 0644); err != nil {
		t.Fatalf("write schema fixture: %v", err)
	}
	cache := core.NewTableCacheStruct()
	catalog, diagnostics, err := LegacyTableCacheCatalogFactory{TableCache: cache}.NewRuntimeCatalog(
		context.Background(),
		NewDirectRuntimeConfig(baseDir, "", 0, 0),
	)
	if err != nil {
		t.Fatalf("new runtime catalog: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("new runtime catalog diagnostics = %#v", diagnostics)
	}

	table, tableDiagnostics := catalog.Table("quanta", "catalog_load_qa")
	if tableDiagnostics.BlocksNative() {
		t.Fatalf("table diagnostics = %#v", tableDiagnostics)
	}
	if table.Name != "catalog_load_qa" {
		t.Fatalf("table name = %q, want catalog_load_qa", table.Name)
	}
	if _, ok := table.Field("city"); !ok {
		t.Fatalf("loaded table fields = %#v, want city", table.Fields)
	}
	if _, ok := cache.TableCache["catalog_load_qa"]; !ok {
		t.Fatalf("catalog load-on-miss did not populate legacy table cache")
	}
}

func TestLegacyTableCacheCatalogLoadsTableDefinitionsFromLegacyCache(t *testing.T) {
	cache := legacyCatalogTestCache()
	catalog := LegacyTableCacheCatalog{TableCache: cache}

	table, diagnostics := catalog.Table("quanta", "orders")

	if diagnostics.BlocksNative() {
		t.Fatalf("Table diagnostics = %#v, want none", diagnostics)
	}
	if table.Schema != "quanta" || table.Name != "orders" {
		t.Fatalf("table identity = %s.%s, want quanta.orders", table.Schema, table.Name)
	}
	if !table.Storage.Partitioned {
		t.Fatal("expected legacy time quantum table to map as partitioned")
	}
	orderDate, ok := table.Field("o_orderdate")
	if !ok {
		t.Fatal("missing o_orderdate field")
	}
	if orderDate.Type != qsbridge.DataTypeTime || orderDate.Index != qsbridge.IndexDateTime ||
		orderDate.Encoding.Kind != qsbridge.EncodingTimeBSI || orderDate.Encoding.Granularity != qsbridge.TimeGranularityMillisecond {
		t.Fatalf("o_orderdate = %#v, want millis time BSI", orderDate)
	}
	orderPriority, ok := table.Field("o_orderpriority")
	if !ok {
		t.Fatal("missing o_orderpriority field")
	}
	if orderPriority.Index != qsbridge.IndexStringEnum || orderPriority.Dictionary.Ref.Table != "orders" ||
		orderPriority.Dictionary.Cardinality != 2 || !orderPriority.Dictionary.AllowsMutation() {
		t.Fatalf("o_orderpriority dictionary = %#v, want mutable StringEnum metadata", orderPriority.Dictionary)
	}
}

func TestLegacyTableCacheCatalogPreservesSetValuedStringEnumMultiplicity(t *testing.T) {
	cache := legacyCatalogTestCache()
	catalog := LegacyTableCacheCatalog{TableCache: cache}

	table, diagnostics := catalog.Table("quanta", "customers_qa")

	if diagnostics.BlocksNative() {
		t.Fatalf("Table diagnostics = %#v, want none", diagnostics)
	}
	phoneType, ok := table.Field("phoneType")
	if !ok {
		t.Fatal("missing phoneType field")
	}
	if phoneType.Encoding.Kind != qsbridge.EncodingStringEnum ||
		phoneType.Encoding.EffectiveMultiplicity() != qsbridge.MultiplicitySet ||
		phoneType.Dictionary.Ref.QualifiedName() != "quanta.customers_qa.phoneType" {
		t.Fatalf("phoneType = %#v, want set-valued StringEnum dictionary", phoneType)
	}
}

func TestLegacyTableCacheCatalogDoesNotExposeBackingPathAsPhysicalField(t *testing.T) {
	cache := legacyCatalogTestCache()
	cache.TableCache["orders"].Attributes[0].SourceName = "/data/o_orderkey"
	catalog := LegacyTableCacheCatalog{TableCache: cache}

	table, diagnostics := catalog.Table("quanta", "orders")
	if diagnostics.BlocksNative() {
		t.Fatalf("Table diagnostics = %#v, want none", diagnostics)
	}
	orderKey, ok := table.Field("o_orderkey")
	if !ok {
		t.Fatal("missing o_orderkey field")
	}
	if orderKey.PhysicalName != "o_orderkey" {
		t.Fatalf("physical name = %q, want o_orderkey", orderKey.PhysicalName)
	}
}

func TestLegacyTableCacheCatalogDerivesParentRelationMetadata(t *testing.T) {
	catalog := LegacyTableCacheCatalog{TableCache: legacyCatalogTestCache()}

	table, diagnostics := catalog.Table("", "lineitem")

	if diagnostics.BlocksNative() {
		t.Fatalf("Table diagnostics = %#v, want none", diagnostics)
	}
	relationship, ok := legacyCatalogRelationshipByField(table.Relationships, "l_orderkey")
	if !ok {
		t.Fatalf("relationships = %#v, missing lineitem.l_orderkey relation", table.Relationships)
	}
	if relationship.FromTable != "lineitem" || relationship.FromField != "l_orderkey" ||
		relationship.ToTable != "orders" || relationship.ToField != "o_orderkey" ||
		relationship.Direction != qsbridge.JoinChildToParent {
		t.Fatalf("relationship = %#v, want lineitem.l_orderkey -> orders.o_orderkey", relationship)
	}
	if !relationship.Encoding.Supports(qsbridge.RelationshipCapabilityAntiJoinDifference) {
		t.Fatalf("relationship encoding = %#v, want anti-join difference capability", relationship.Encoding)
	}

	byName, relDiagnostics := catalog.Relationship(relationship.Name)
	if relDiagnostics.BlocksNative() {
		t.Fatalf("Relationship diagnostics = %#v, want none", relDiagnostics)
	}
	if byName.Name != relationship.Name {
		t.Fatalf("Relationship(%q) = %#v", relationship.Name, byName)
	}
}

func TestLegacyTableCacheCatalogFindsDependentRelationshipsForParent(t *testing.T) {
	catalog := LegacyTableCacheCatalog{TableCache: legacyCatalogTestCache()}

	relationships, diagnostics := catalog.DependentRelationships("quanta", "orders")

	if diagnostics.BlocksNative() {
		t.Fatalf("dependent relationship diagnostics = %#v, want none", diagnostics)
	}
	if len(relationships) != 1 {
		t.Fatalf("relationships = %#v, want one lineitem dependency", relationships)
	}
	relationship := relationships[0]
	if relationship.ChildTable() != "lineitem" || relationship.ParentTable() != "orders" || relationship.FromField != "l_orderkey" {
		t.Fatalf("relationship = %#v, want lineitem child of orders", relationship)
	}
}

func TestLegacyTableCacheCatalogReportsCatalogMisses(t *testing.T) {
	catalog := LegacyTableCacheCatalog{}

	_, tableDiagnostics := catalog.Table("quanta", "missing")
	assertRuntimeDiagnosticCode(t, tableDiagnostics, qsbridge.DiagnosticCatalogTableNotFound)

	_, relationshipDiagnostics := catalog.Relationship("missing")
	assertRuntimeDiagnosticCode(t, relationshipDiagnostics, qsbridge.DiagnosticCatalogRelationshipNotFound)

	_, functionDiagnostics := catalog.Function("missing")
	assertRuntimeDiagnosticCode(t, functionDiagnostics, qsbridge.DiagnosticCatalogFunctionNotFound)
}

func legacyCatalogTestCache() *core.TableCacheStruct {
	cache := core.NewTableCacheStruct()
	cache.TableCache["orders"] = &core.Table{
		BasicTable: &shared.BasicTable{
			Name:             "orders",
			PrimaryKey:       "o_orderkey",
			TimeQuantumType:  "YMD",
			TimeQuantumField: "o_orderdate",
		},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "o_orderkey", Type: "Integer", MappingStrategy: "IntBSI", Required: true}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "o_orderdate", Type: "DateTime", MappingStrategy: "SysMillisBSI"}},
			{BasicAttribute: &shared.BasicAttribute{
				FieldName:       "o_orderpriority",
				Type:            "String",
				MappingStrategy: "StringEnum",
				Values: []shared.Value{
					{Value: "1-URGENT", RowID: 1},
					{Value: "5-LOW", RowID: 5},
				},
			}},
		},
	}

	cache.TableCache["part"] = &core.Table{
		BasicTable: &shared.BasicTable{Name: "part", PrimaryKey: "p_partkey"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "p_partkey", Type: "Integer", MappingStrategy: "IntBSI", Required: true}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "p_brand", Type: "String", MappingStrategy: "StringHashBSI"}},
			{BasicAttribute: &shared.BasicAttribute{
				FieldName:       "p_name",
				Type:            "String",
				MappingStrategy: "StringLexBSI",
				Size:            55,
				MapperConfig:    map[string]string{"length": "8"},
			}},
			{BasicAttribute: &shared.BasicAttribute{
				FieldName:       "p_container",
				Type:            "String",
				MappingStrategy: "StringEnum",
				Values: []shared.Value{
					{Value: "MED JAR", RowID: 1},
					{Value: "MED BOX", RowID: 2},
				},
			}},
		},
	}
	cache.TableCache["lineitem"] = &core.Table{
		BasicTable: &shared.BasicTable{Name: "lineitem"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_orderkey", Type: "Integer", MappingStrategy: "ParentRelation", ForeignKey: "orders.o_orderkey"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_partkey", Type: "Integer", MappingStrategy: "ParentRelation", ForeignKey: "part.p_partkey"}},
			{BasicAttribute: &shared.BasicAttribute{FieldName: "l_quantity", Type: "Integer", MappingStrategy: "IntBSI"}},
		},
	}
	cache.TableCache["customers_qa"] = &core.Table{
		BasicTable: &shared.BasicTable{Name: "customers_qa"},
		Attributes: []core.Attribute{
			{BasicAttribute: &shared.BasicAttribute{
				FieldName:       "phoneType",
				Type:            "String",
				MappingStrategy: "StringEnum",
				NonExclusive:    true,
				Values: []shared.Value{
					{Value: "cell", RowID: 1},
					{Value: "home", RowID: 2},
				},
			}},
		},
	}
	return cache
}

func TestLegacyTableCacheCatalogMapsStringLexBSI(t *testing.T) {
	cache := legacyCatalogTestCache()
	catalog := LegacyTableCacheCatalog{TableCache: cache}

	table, diagnostics := catalog.Table("quanta", "part")

	if diagnostics.BlocksNative() {
		t.Fatalf("Table diagnostics = %#v, want none", diagnostics)
	}
	partName, ok := table.Field("p_name")
	if !ok {
		t.Fatal("missing p_name field")
	}
	if partName.Index != qsbridge.IndexBSI || partName.Encoding.Kind != qsbridge.EncodingStringLexBSI {
		t.Fatalf("p_name = %#v, want BSI-backed StringLexBSI", partName)
	}
	if partName.Encoding.PrefixLength != 8 || partName.Encoding.MaxLength != 55 ||
		!partName.Encoding.NeedsStringRemainderLookup() {
		t.Fatalf("p_name encoding = %#v, want eight-byte prefix with bounded remainder", partName.Encoding)
	}
}

func legacyCatalogRelationshipByField(relationships []qsbridge.RelationshipDefinition, field string) (qsbridge.RelationshipDefinition, bool) {
	for _, relationship := range relationships {
		if relationship.FromField == field {
			return relationship, true
		}
	}
	return qsbridge.RelationshipDefinition{}, false
}

func assertRuntimeDiagnosticCode(t *testing.T, diagnostics qsbridge.DiagnosticSet, want qsbridge.DiagnosticCode) {
	t.Helper()
	if len(diagnostics) != 1 || diagnostics[0].Code != want {
		t.Fatalf("diagnostics = %#v, want single %s", diagnostics, want)
	}
}
