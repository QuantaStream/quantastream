package qsruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/shared"
)

func TestLegacySchemaMutationHandleCreatesAndDropsFileCatalogView(t *testing.T) {
	configDir := t.TempDir()
	handle := LegacyQuantaSessionHandle{
		TableName: "customer_names",
		Session:   &core.Session{BasePath: configDir},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:    qsbridge.MutationCreateView,
			Target:  qsbridge.TableInstance{Schema: "quanta", Table: "customer_names"},
			ViewSQL: "select c_custkey, c_name from customer",
			ViewDependencies: []qsbridge.TableInstance{
				{Schema: "quanta", Table: "customer"},
			},
		},
	}

	statement, diagnostics, err := handle.CreateView(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateView() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("CreateView() diagnostics = %#v", diagnostics)
	}
	if statement.Status != "View customer_names created" {
		t.Fatalf("status = %q", statement.Status)
	}
	active, err := shared.CatalogViewActive(configDir, "quanta", "customer_names")
	if err != nil {
		t.Fatalf("CatalogViewActive() error = %v", err)
	}
	if !active {
		t.Fatalf("view should be active")
	}
	view, err := shared.LoadViewDefinition(configDir, "customer_names")
	if err != nil {
		t.Fatalf("LoadViewDefinition() error = %v", err)
	}
	if view.SQL != request.Mutation.ViewSQL {
		t.Fatalf("view SQL = %q, want %q", view.SQL, request.Mutation.ViewSQL)
	}
	if len(view.Dependencies) != 1 || view.Dependencies[0].ObjectName != "customer" {
		t.Fatalf("dependencies = %#v, want customer", view.Dependencies)
	}
	catalogView, catalogDiagnostics := (LegacyTableCacheCatalog{ConfigDir: configDir}).View("quanta", "customer_names")
	if catalogDiagnostics.BlocksNative() {
		t.Fatalf("catalog view diagnostics = %#v", catalogDiagnostics)
	}
	if catalogView.Name != "customer_names" || catalogView.SQL != request.Mutation.ViewSQL {
		t.Fatalf("catalog view = %#v", catalogView)
	}

	dropRequest := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationDropView,
			Target: qsbridge.TableInstance{Schema: "quanta", Table: "customer_names"},
		},
	}
	statement, diagnostics, err = handle.DropView(context.Background(), dropRequest)
	if err != nil {
		t.Fatalf("DropView() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropView() diagnostics = %#v", diagnostics)
	}
	if statement.Status != "View customer_names dropped" {
		t.Fatalf("drop status = %q", statement.Status)
	}
	active, err = shared.CatalogViewActive(configDir, "quanta", "customer_names")
	if err != nil {
		t.Fatalf("CatalogViewActive() after drop error = %v", err)
	}
	if active {
		t.Fatalf("view should not be active after drop")
	}
	if shared.ViewDefinitionExists(configDir, "customer_names") {
		t.Fatalf("view definition should be removed")
	}
}

func TestLegacySchemaMutationHandleDropViewIfExistsIgnoresMissingFileCatalogView(t *testing.T) {
	configDir := t.TempDir()
	handle := LegacyQuantaSessionHandle{
		TableName: "missing_view",
		Session:   &core.Session{BasePath: configDir},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationDropView,
			Target: qsbridge.TableInstance{Schema: "quanta", Table: "missing_view"},
		},
	}
	_, diagnostics, err := handle.DropView(context.Background(), request)
	if err == nil {
		t.Fatalf("DropView() error = nil, want missing-view error")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropView() diagnostics = %#v", diagnostics)
	}

	request.Mutation.IfExists = true
	statement, diagnostics, err := handle.DropView(context.Background(), request)
	if err != nil {
		t.Fatalf("DropView(if exists) error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropView(if exists) diagnostics = %#v", diagnostics)
	}
	if statement.Status != "View missing_view dropped" {
		t.Fatalf("status = %q, want missing_view dropped", statement.Status)
	}
}

func TestLegacySchemaMutationHandleDropTableIfExistsIgnoresMissingFileCatalogTable(t *testing.T) {
	configDir := t.TempDir()
	handle := LegacyQuantaSessionHandle{
		TableName: "missing_table",
		Session:   &core.Session{BasePath: configDir, BitIndex: &shared.BitmapIndex{}},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:   qsbridge.MutationDropTable,
			Target: qsbridge.TableInstance{Schema: "quanta", Table: "missing_table"},
		},
	}
	_, diagnostics, err := handle.DropTable(context.Background(), request)
	if err == nil {
		t.Fatalf("DropTable() error = nil, want missing-table error")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropTable() diagnostics = %#v", diagnostics)
	}

	request.Mutation.IfExists = true
	statement, diagnostics, err := handle.DropTable(context.Background(), request)
	if err != nil {
		t.Fatalf("DropTable(if exists) error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropTable(if exists) diagnostics = %#v", diagnostics)
	}
	if statement.Status != "Table missing_table dropped" {
		t.Fatalf("status = %q, want missing_table dropped", statement.Status)
	}
}

func TestLegacySchemaMutationHandleDropTableRejectsActiveDependentView(t *testing.T) {
	configDir := t.TempDir()
	now := testSchemaMutationTime()
	if err := shared.ActivateCatalogTable(configDir, "quanta", "customer", now); err != nil {
		t.Fatalf("ActivateCatalogTable() error = %v", err)
	}
	if err := shared.SaveViewDefinition(configDir, shared.ViewDefinition{
		SchemaName: "quanta",
		ViewName:   "customer_projection",
		SQL:        "select c_custkey from customer",
		Dependencies: []shared.ViewDependency{
			{SchemaName: "quanta", ObjectName: "customer", ObjectType: shared.CatalogObjectTypeTable},
		},
		CreationDate:     now,
		ModificationDate: now,
	}); err != nil {
		t.Fatalf("SaveViewDefinition() error = %v", err)
	}
	if err := shared.ActivateCatalogView(configDir, "quanta", "customer_projection", now); err != nil {
		t.Fatalf("ActivateCatalogView() error = %v", err)
	}
	handle := LegacyQuantaSessionHandle{
		TableName: "customer",
		Session:   &core.Session{BasePath: configDir, BitIndex: &shared.BitmapIndex{}},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:     qsbridge.MutationDropTable,
			Target:   qsbridge.TableInstance{Schema: "quanta", Table: "customer"},
			IfExists: true,
		},
	}

	_, diagnostics, err := handle.DropTable(context.Background(), request)
	if err == nil {
		t.Fatalf("DropTable() error = nil, want dependent-view error")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropTable() diagnostics = %#v", diagnostics)
	}
	if got := err.Error(); got != "cannot drop table referenced by views: customer_projection" {
		t.Fatalf("error = %q, want dependent-view error", got)
	}
}

func TestLegacySchemaMutationHandleDropTableRejectsActiveChildTable(t *testing.T) {
	configDir := t.TempDir()
	now := testSchemaMutationTime()
	writeSchemaMutationCatalogSchema(t, configDir, "customers", "")
	writeSchemaMutationCatalogSchema(t, configDir, "orders", "customers.id")
	if err := shared.ActivateCatalogTable(configDir, "quanta", "customers", now); err != nil {
		t.Fatalf("ActivateCatalogTable(customers) error = %v", err)
	}
	if err := shared.ActivateCatalogTable(configDir, "quanta", "orders", now); err != nil {
		t.Fatalf("ActivateCatalogTable(orders) error = %v", err)
	}
	handle := LegacyQuantaSessionHandle{
		TableName: "customers",
		Session:   &core.Session{BasePath: configDir, BitIndex: &shared.BitmapIndex{}},
	}
	request := ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:     qsbridge.MutationDropTable,
			Target:   qsbridge.TableInstance{Schema: "quanta", Table: "customers"},
			IfExists: true,
		},
	}

	_, diagnostics, err := handle.DropTable(context.Background(), request)
	if err == nil {
		t.Fatalf("DropTable() error = nil, want child-table dependency error")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("DropTable() diagnostics = %#v", diagnostics)
	}
	if got := err.Error(); got != "cannot drop table with dependencies: orders" {
		t.Fatalf("error = %q, want child-table dependency error", got)
	}
}

func TestLegacySchemaMutationHandleCreateViewRejectsTableNameCollision(t *testing.T) {
	configDir := t.TempDir()
	if err := shared.ActivateCatalogTable(configDir, "quanta", "customer_names", testSchemaMutationTime()); err != nil {
		t.Fatalf("ActivateCatalogTable() error = %v", err)
	}
	handle := LegacyQuantaSessionHandle{
		TableName: "customer_names",
		Session:   &core.Session{BasePath: configDir},
	}

	_, diagnostics, err := handle.CreateView(context.Background(), ExecutionRequest{
		Mutation: qsbridge.MutationShape{
			Kind:    qsbridge.MutationCreateView,
			Target:  qsbridge.TableInstance{Schema: "quanta", Table: "customer_names"},
			ViewSQL: "select c_custkey from customer",
		},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("CreateView() diagnostics = %#v", diagnostics)
	}
	if err == nil {
		t.Fatalf("CreateView() should reject an active table with the same name")
	}
}

func testSchemaMutationTime() time.Time {
	return time.Date(2026, 8, 16, 11, 0, 0, 0, time.UTC)
}

func writeSchemaMutationCatalogSchema(t *testing.T, configDir string, table string, foreignKey string) {
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
