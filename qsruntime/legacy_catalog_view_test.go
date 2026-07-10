package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestLegacyCatalogViewAdapterBuildsNodeViewFromLegacyCache(t *testing.T) {
	adapter := LegacyCatalogViewAdapter{
		Catalog: LegacyTableCacheCatalog{TableCache: legacyCatalogTestCache()},
	}

	view, diagnostics := adapter.NodeCatalogView("quanta", "orders", "lineitem")

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(view.Tables) != 2 {
		t.Fatalf("tables = %#v, want orders and lineitem", view.Tables)
	}
	orders := view.Tables[0]
	if orders.Name != "orders" || !orders.Storage.Partitioned {
		t.Fatalf("orders node view = %#v, want partitioned physical table metadata", orders)
	}
	priority := orders.Fields[2]
	if priority.Name != "o_orderpriority" || priority.Encoding.Kind != qsbridge.EncodingStringEnum {
		t.Fatalf("priority node field = %#v, want physical StringEnum encoding", priority)
	}
	if !legacyCatalogNodeViewHasRelationship(view, "lineitem", "l_orderkey", "orders", "o_orderkey") {
		t.Fatalf("relationships = %#v, want lineitem relationship vector", view.Relationships)
	}
}

func TestLegacyCatalogViewAdapterBuildsQueryViewWithDictionaryMetadata(t *testing.T) {
	adapter := LegacyCatalogViewAdapter{
		Catalog: LegacyTableCacheCatalog{
			TableCache: legacyCatalogTestCache(),
			Functions: []qsbridge.FunctionDefinition{{
				Name:   "topn",
				Kind:   qsbridge.FunctionAggregate,
				Origin: qsbridge.FunctionOriginQuantaCustom,
			}},
		},
	}

	view, diagnostics := adapter.QueryCatalogView("quanta", "orders")

	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(view.Tables) != 1 {
		t.Fatalf("tables = %#v, want orders", view.Tables)
	}
	priority := view.Tables[0].Fields[2]
	if priority.Dictionary.Ref.Table != "orders" || priority.Dictionary.Cardinality != 2 ||
		!priority.Dictionary.AllowsMutation() {
		t.Fatalf("priority dictionary = %#v, want semantic legacy StringEnum dictionary metadata", priority.Dictionary)
	}
	if len(view.Functions) != 1 || view.Functions[0].Name != "topn" {
		t.Fatalf("functions = %#v, want adapter-supplied function metadata", view.Functions)
	}
}

func TestLegacyCatalogViewAdapterReportsMissingTables(t *testing.T) {
	adapter := LegacyCatalogViewAdapter{
		Catalog: LegacyTableCacheCatalog{TableCache: legacyCatalogTestCache()},
	}

	_, diagnostics := adapter.NodeCatalogView("quanta", "missing")

	assertRuntimeDiagnosticCode(t, diagnostics, qsbridge.DiagnosticCatalogTableNotFound)
}

func legacyCatalogNodeViewHasRelationship(view qsbridge.NodeCatalogView, fromTable string, fromField string, toTable string, toField string) bool {
	for _, relationship := range view.Relationships {
		if relationship.FromTable == fromTable && relationship.FromField == fromField &&
			relationship.ToTable == toTable && relationship.ToField == toField {
			return true
		}
	}
	return false
}
