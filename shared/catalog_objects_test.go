package shared

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCatalogObjectsFileActivatesAndRemovesTables(t *testing.T) {
	configDir := t.TempDir()
	now := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)

	if err := ActivateCatalogTable(configDir, "quanta", "customers", now); err != nil {
		t.Fatalf("ActivateCatalogTable() error = %v", err)
	}
	active, err := CatalogTableActive(configDir, "quanta", "customers")
	if err != nil {
		t.Fatalf("CatalogTableActive() error = %v", err)
	}
	if !active {
		t.Fatalf("customers should be active")
	}
	tables, err := ActiveCatalogTables(configDir, "quanta")
	if err != nil {
		t.Fatalf("ActiveCatalogTables() error = %v", err)
	}
	if len(tables) != 1 || tables[0] != "customers" {
		t.Fatalf("tables = %#v, want customers", tables)
	}

	if err := RemoveCatalogTable(configDir, "quanta", "customers"); err != nil {
		t.Fatalf("RemoveCatalogTable() error = %v", err)
	}
	active, err = CatalogTableActive(configDir, "quanta", "customers")
	if err != nil {
		t.Fatalf("CatalogTableActive() after remove error = %v", err)
	}
	if active {
		t.Fatalf("customers should not be active")
	}
}

func TestActiveOrDiscoveredSchemaTablesPrefersCatalogObjects(t *testing.T) {
	configDir := t.TempDir()
	writeSharedCatalogSchema(t, configDir, "active", "")
	writeSharedCatalogSchema(t, configDir, "draft", "")
	if err := ActivateCatalogTable(configDir, "quanta", "active", time.Now().UTC()); err != nil {
		t.Fatalf("ActivateCatalogTable() error = %v", err)
	}

	tables, err := ActiveOrDiscoveredSchemaTables(configDir, "quanta")
	if err != nil {
		t.Fatalf("ActiveOrDiscoveredSchemaTables() error = %v", err)
	}
	if len(tables) != 1 || tables[0] != "active" {
		t.Fatalf("tables = %#v, want only active manifest entry", tables)
	}
}

func TestViewOnlyCatalogManifestFallsBackToDiscoveredTables(t *testing.T) {
	configDir := t.TempDir()
	writeSharedCatalogSchema(t, configDir, "customer", "")
	writeSharedCatalogSchema(t, configDir, "orders", "")
	if err := ActivateCatalogView(configDir, "quanta", "customer_orders", time.Now().UTC()); err != nil {
		t.Fatalf("ActivateCatalogView() error = %v", err)
	}

	tables, err := ActiveCatalogTables(configDir, "quanta")
	if err != nil {
		t.Fatalf("ActiveCatalogTables() error = %v", err)
	}
	if len(tables) != 2 || tables[0] != "customer" || tables[1] != "orders" {
		t.Fatalf("tables = %#v, want discovered customer/orders", tables)
	}
	active, err := CatalogTableActive(configDir, "quanta", "customer")
	if err != nil {
		t.Fatalf("CatalogTableActive() error = %v", err)
	}
	if !active {
		t.Fatalf("customer should be active through discovered-schema fallback")
	}
}

func TestFileCatalogParentAndChildRelationChecks(t *testing.T) {
	configDir := t.TempDir()
	writeSharedCatalogSchema(t, configDir, "customers", "")
	writeSharedCatalogSchema(t, configDir, "orders", "customers.cust_id")
	if err := ActivateCatalogTable(configDir, "quanta", "customers", time.Now().UTC()); err != nil {
		t.Fatalf("activate customers: %v", err)
	}

	orders, err := LoadSchema(configDir, "orders", nil)
	if err != nil {
		t.Fatalf("LoadSchema(orders) error = %v", err)
	}
	ok, err := CheckParentRelationInCatalog(configDir, "quanta", orders)
	if err != nil {
		t.Fatalf("CheckParentRelationInCatalog() error = %v", err)
	}
	if !ok {
		t.Fatalf("orders parent customers should be active")
	}

	if err := ActivateCatalogTable(configDir, "quanta", "orders", time.Now().UTC()); err != nil {
		t.Fatalf("activate orders: %v", err)
	}
	dependencies, err := CheckChildRelationInCatalog(configDir, "quanta", "customers")
	if err != nil {
		t.Fatalf("CheckChildRelationInCatalog() error = %v", err)
	}
	if len(dependencies) != 1 || dependencies[0] != "orders" {
		t.Fatalf("dependencies = %#v, want orders", dependencies)
	}
}

func TestFileCatalogParentRelationRequiresActiveParent(t *testing.T) {
	configDir := t.TempDir()
	writeSharedCatalogSchema(t, configDir, "customers", "")
	writeSharedCatalogSchema(t, configDir, "orders", "customers.cust_id")
	orders, err := LoadSchema(configDir, "orders", nil)
	if err != nil {
		t.Fatalf("LoadSchema(orders) error = %v", err)
	}

	ok, err := CheckParentRelationInCatalog(configDir, "quanta", orders)

	if err != nil {
		t.Fatalf("CheckParentRelationInCatalog() error = %v", err)
	}
	if ok {
		t.Fatalf("parent should not be valid until customers is active")
	}
}

func writeSharedCatalogSchema(t *testing.T, configDir string, table string, foreignKey string) {
	t.Helper()
	tableDir := filepath.Join(configDir, table)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		t.Fatalf("mkdir schema dir: %v", err)
	}
	fkLine := ""
	if foreignKey != "" {
		fkLine = "  foreignKey: " + foreignKey + "\n"
	}
	schema := "tableName: " + table + `
primaryKey: id
attributes:
- fieldName: id
  sourceName: /id
  mappingStrategy: IntBSI
  type: Integer
` + fkLine
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
}
