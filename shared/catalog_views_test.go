package shared

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCatalogObjectsTrackViewsSeparatelyFromTables(t *testing.T) {
	configDir := t.TempDir()
	now := time.Date(2026, 8, 16, 9, 0, 0, 0, time.UTC)

	if err := ActivateCatalogTable(configDir, "quanta", "customers", now); err != nil {
		t.Fatalf("ActivateCatalogTable() error = %v", err)
	}
	if err := ActivateCatalogView(configDir, "quanta", "active_customers", now); err != nil {
		t.Fatalf("ActivateCatalogView() error = %v", err)
	}

	tables, err := ActiveCatalogTables(configDir, "quanta")
	if err != nil {
		t.Fatalf("ActiveCatalogTables() error = %v", err)
	}
	if len(tables) != 1 || tables[0] != "customers" {
		t.Fatalf("tables = %#v, want customers only", tables)
	}
	views, err := ActiveCatalogViews(configDir, "quanta")
	if err != nil {
		t.Fatalf("ActiveCatalogViews() error = %v", err)
	}
	if len(views) != 1 || views[0] != "active_customers" {
		t.Fatalf("views = %#v, want active_customers only", views)
	}
	tableActive, err := CatalogTableActive(configDir, "quanta", "active_customers")
	if err != nil {
		t.Fatalf("CatalogTableActive(view) error = %v", err)
	}
	if tableActive {
		t.Fatalf("view should not be active as a table")
	}
	viewActive, err := CatalogViewActive(configDir, "quanta", "active_customers")
	if err != nil {
		t.Fatalf("CatalogViewActive() error = %v", err)
	}
	if !viewActive {
		t.Fatalf("active_customers should be active as a view")
	}

	if err := RemoveCatalogView(configDir, "quanta", "active_customers"); err != nil {
		t.Fatalf("RemoveCatalogView() error = %v", err)
	}
	tables, err = ActiveCatalogTables(configDir, "quanta")
	if err != nil {
		t.Fatalf("ActiveCatalogTables() after remove error = %v", err)
	}
	if len(tables) != 1 || tables[0] != "customers" {
		t.Fatalf("removing view should not remove table, got %#v", tables)
	}
}

func TestViewDefinitionRoundTrip(t *testing.T) {
	configDir := t.TempDir()
	now := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	view := ViewDefinition{
		SchemaName:   "quanta",
		ViewName:     "active_customers",
		SQL:          "select c_custkey, c_name from customer where c_mktsegment = 'BUILDING'",
		CanonicalSQL: "select c_custkey, c_name from customer where c_mktsegment = ?",
		Columns: []ViewColumnDefinition{
			{Name: "c_custkey", Type: "BIGINT"},
			{Name: "c_name", Type: "VARCHAR"},
		},
		Dependencies: []ViewDependency{
			{SchemaName: "quanta", ObjectName: "customer", ObjectType: CatalogObjectTypeTable},
		},
		CreationDate:     now,
		ModificationDate: now,
	}

	if err := SaveViewDefinition(configDir, view); err != nil {
		t.Fatalf("SaveViewDefinition() error = %v", err)
	}
	if !ViewDefinitionExists(configDir, "active_customers") {
		t.Fatalf("view definition should exist")
	}
	if _, err := os.Stat(filepath.Join(configDir, CatalogViewsDirName, "active_customers.yaml")); err != nil {
		t.Fatalf("view definition file missing: %v", err)
	}

	loaded, err := LoadViewDefinition(configDir, "active_customers")
	if err != nil {
		t.Fatalf("LoadViewDefinition() error = %v", err)
	}
	if loaded.SchemaName != view.SchemaName || loaded.ViewName != view.ViewName || loaded.SQL != view.SQL || loaded.CanonicalSQL != view.CanonicalSQL {
		t.Fatalf("loaded view mismatch: %#v", loaded)
	}
	if len(loaded.Columns) != 2 || loaded.Columns[0].Name != "c_custkey" || loaded.Columns[1].Type != "VARCHAR" {
		t.Fatalf("columns = %#v", loaded.Columns)
	}
	if len(loaded.Dependencies) != 1 || loaded.Dependencies[0].ObjectName != "customer" {
		t.Fatalf("dependencies = %#v", loaded.Dependencies)
	}

	if err := RemoveViewDefinition(configDir, "active_customers"); err != nil {
		t.Fatalf("RemoveViewDefinition() error = %v", err)
	}
	if ViewDefinitionExists(configDir, "active_customers") {
		t.Fatalf("view definition should be removed")
	}
}

func TestViewDefinitionRejectsUnsafeFileNames(t *testing.T) {
	configDir := t.TempDir()
	err := SaveViewDefinition(configDir, ViewDefinition{
		ViewName: "../escape",
		SQL:      "select 1",
	})
	if err == nil {
		t.Fatalf("SaveViewDefinition() should reject path-like names")
	}
}
