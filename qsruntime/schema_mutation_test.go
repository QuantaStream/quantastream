package qsruntime

import (
	"context"
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
