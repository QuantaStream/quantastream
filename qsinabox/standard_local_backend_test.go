package qsinabox

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/shared"
)

func TestMountStandardLocalBackendStagesConfigAndMountsReadServices(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")

	backend, err := MountStandardLocalBackend(StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}, nil)
	if err != nil {
		t.Fatalf("MountStandardLocalBackend() error = %v", err)
	}
	defer backend.Close()

	readiness := backend.Services.Readiness()
	if !readiness.Ready {
		t.Fatalf("readiness = %+v, want ready read services", readiness)
	}
	if !readiness.BitmapIndex || !readiness.KVStore || !readiness.StringSearch {
		t.Fatalf("readiness = %+v, want bitmap, kv, and string search mounted", readiness)
	}
	if len(readiness.StreamingRisks) != 0 {
		t.Fatalf("streaming risks = %+v, want none with local string search mounted", readiness.StreamingRisks)
	}
	if backend.Adapter.BitmapIndex == nil || backend.Adapter.BitmapIndex.GetTable("sample") == nil {
		t.Fatalf("sample schema was not loaded into local BitmapIndex")
	}
	if _, err := os.Stat(filepath.Join(root, "data", "config")); err != nil {
		t.Fatalf("data/config was not staged: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "data", "config", "sample", "schema.yaml")); err != nil {
		t.Fatalf("sample schema was not staged: %v", err)
	}
}

func TestMountStandardLocalBackendRefreshesExistingStagedConfig(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	dataConfigDir := filepath.Join(root, "data", "config")
	if err := os.MkdirAll(dataConfigDir, 0755); err != nil {
		t.Fatalf("mkdir existing data/config: %v", err)
	}

	backend, err := MountStandardLocalBackend(StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}, nil)
	if err != nil {
		t.Fatalf("MountStandardLocalBackend() error = %v", err)
	}
	defer backend.Close()

	if _, err := os.Stat(filepath.Join(dataConfigDir, "sample", "schema.yaml")); err != nil {
		t.Fatalf("existing data/config was not refreshed with sample schema: %v", err)
	}
}

func TestStandardLocalBackendBuildsLocalSessionPool(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}

	backend, err := MountStandardLocalBackend(config, nil)
	if err != nil {
		t.Fatalf("MountStandardLocalBackend() error = %v", err)
	}
	defer backend.Close()

	conn := backend.NewLocalConnection()
	if conn.ServicePort != 0 || !conn.IsLocalCluster {
		t.Fatalf("local connection = %+v, want in-process local connection", conn)
	}
	if conn.LocalNodeServices.BitmapIndex == nil || conn.LocalNodeServices.KVStore == nil || conn.LocalNodeServices.StringSearch == nil {
		t.Fatalf("local services = %+v, want bitmap, kv, and string search facades", conn.LocalNodeServices)
	}

	pool := backend.NewSessionPool(config, nil, 1)
	session, err := pool.Borrow("sample")
	if err != nil {
		t.Fatalf("Borrow(sample) error = %v", err)
	}
	if session.BitIndex == nil || session.KVStore == nil || session.StringIndex == nil {
		t.Fatalf("session = %+v, want bitmap, kv, and string search clients", session)
	}
	pool.Return("sample", session)
	pool.Shutdown()
}

func TestStandardLocalBackendDirectRuntimeWiresFilterDictionaryResolver(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}

	backend, err := MountStandardLocalBackend(config, nil)
	if err != nil {
		t.Fatalf("MountStandardLocalBackend() error = %v", err)
	}
	defer backend.Close()
	mount := backend.NewDirectRuntime(config, nil, 1)
	defer mount.Close()

	adapter, ok := mount.Runtime.FilterAdapter.(qsruntime.DirectBitmapFilterTreeAdapter)
	if !ok {
		t.Fatalf("FilterAdapter = %T, want DirectBitmapFilterTreeAdapter", mount.Runtime.FilterAdapter)
	}
	if adapter.DictionaryResolver == nil {
		t.Fatalf("DictionaryResolver = nil, want inabox-standard string-enum filter dictionary lookup")
	}
}

func TestStandardDirectSessionProviderQueriesLocalBitmapIndex(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}

	backend, err := MountStandardLocalBackend(config, nil)
	if err != nil {
		t.Fatalf("MountStandardLocalBackend() error = %v", err)
	}
	defer backend.Close()
	pool := backend.NewSessionPool(config, nil, 1)
	defer pool.Shutdown()

	request := qsruntime.NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "sample",
			Field:     "id",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	handle, diagnostics, err := StandardDirectSessionProvider{Pool: pool}.BorrowDirectSession(context.Background(), request)
	if err != nil {
		t.Fatalf("BorrowDirectSession() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("BorrowDirectSession() diagnostics = %#v, want non-blocking", diagnostics)
	}
	if _, ok := handle.(qsruntime.DirectCandidateBitmapSessionHandle); !ok {
		t.Fatalf("BorrowDirectSession() handle = %T, want candidate-aware bitmap query handle", handle)
	}
	result, queryDiagnostics, err := handle.QueryBitmap(context.Background(), request)
	if err != nil {
		t.Fatalf("QueryBitmap() error = %v", err)
	}
	if queryDiagnostics.BlocksNative() {
		t.Fatalf("QueryBitmap() diagnostics = %#v, want non-blocking", queryDiagnostics)
	}
	if !result.Success {
		t.Fatalf("QueryBitmap() result = %#v, want success", result)
	}
	handle.Release(context.Background())
}

func TestStandardDirectSessionProviderCreatesViewWithSyntheticHandle(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}

	backend, err := MountStandardLocalBackend(config, nil)
	if err != nil {
		t.Fatalf("MountStandardLocalBackend() error = %v", err)
	}
	defer backend.Close()
	pool := backend.NewSessionPool(config, nil, 1)
	defer pool.Shutdown()

	request := qsruntime.NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Mutation = qsbridge.MutationShape{
		Kind:    qsbridge.MutationCreateView,
		Target:  qsbridge.TableInstance{Schema: "quanta", Table: "sample_view"},
		ViewSQL: "select id, city from sample",
		ViewDependencies: []qsbridge.TableInstance{{
			Schema: "quanta",
			Table:  "sample",
		}},
	}

	provider := StandardDirectSessionProvider{
		Pool:      pool,
		SchemaDir: backend.ConfigBaseDir(config),
		Conn:      backend.NewLocalConnection(),
	}
	handle, diagnostics, err := provider.BorrowDirectSession(context.Background(), request)
	if err != nil {
		t.Fatalf("BorrowDirectSession() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("BorrowDirectSession() diagnostics = %#v, want non-blocking", diagnostics)
	}
	standardHandle, ok := handle.(StandardDirectSessionHandle)
	if !ok {
		t.Fatalf("BorrowDirectSession() handle = %T, want StandardDirectSessionHandle", handle)
	}
	if !standardHandle.Synthetic {
		t.Fatalf("BorrowDirectSession() Synthetic = false, want true for CREATE VIEW")
	}

	statement, mutationDiagnostics, err := handle.ExecuteMutation(context.Background(), request)
	if err != nil {
		t.Fatalf("ExecuteMutation() error = %v", err)
	}
	if mutationDiagnostics.BlocksNative() {
		t.Fatalf("ExecuteMutation() diagnostics = %#v, want non-blocking", mutationDiagnostics)
	}
	if statement.Status != "View sample_view created" {
		t.Fatalf("ExecuteMutation() status = %q, want view created", statement.Status)
	}
	if _, err := os.Stat(filepath.Join(backend.ConfigBaseDir(config), "views", "sample_view.yaml")); err != nil {
		t.Fatalf("view definition was not written under staged config: %v", err)
	}
	if active, err := shared.CatalogViewActive(backend.ConfigBaseDir(config), "quanta", "sample_view"); err != nil {
		t.Fatalf("CatalogViewActive() error = %v", err)
	} else if !active {
		t.Fatalf("CatalogViewActive() = false, want true")
	}
	if releaseDiagnostics := handle.Release(context.Background()); releaseDiagnostics.BlocksNative() {
		t.Fatalf("Release() diagnostics = %#v, want non-blocking", releaseDiagnostics)
	}
}

func TestStandardDirectRuntimeExecutesFlatBitmapReadLocally(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}

	backend, err := MountStandardLocalBackend(config, nil)
	if err != nil {
		t.Fatalf("MountStandardLocalBackend() error = %v", err)
	}
	defer backend.Close()
	runtime := backend.NewDirectRuntime(config, nil, 1)
	defer runtime.Close()

	request := qsruntime.NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "sample",
			Field:     "id",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	result, err := runtime.Runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("ExecuteDirect() error = %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("ExecuteDirect() diagnostics = %#v, want non-blocking", result.Diagnostics)
	}
	if result.Count != 0 {
		t.Fatalf("ExecuteDirect() count = %d, want empty local table count", result.Count)
	}
}

func writeStandardTestSchema(t *testing.T, configDir, table string) {
	t.Helper()
	writeStandardDraftTestSchema(t, configDir, table)
	if err := shared.ActivateCatalogTable(configDir, "quanta", table, time.Now().UTC()); err != nil {
		t.Fatalf("activate catalog object: %v", err)
	}
}

func writeStandardDraftTestSchema(t *testing.T, configDir, table string) {
	t.Helper()
	tableDir := filepath.Join(configDir, table)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		t.Fatalf("mkdir schema dir: %v", err)
	}
	schema := `tableName: ` + table + `
primaryKey: id
attributes:
- fieldName: id
  sourceName: /id
  mappingStrategy: IntBSI
  type: Integer
- fieldName: city
  sourceName: /city
  mappingStrategy: StringLexBSI
  configuration:
    length: "8"
  type: String
`
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
}
