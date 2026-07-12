package qsinabox

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
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
	if !readiness.BitmapIndex || !readiness.KVStore {
		t.Fatalf("readiness = %+v, want bitmap and kv mounted", readiness)
	}
	if backend.Adapter.BitmapIndex == nil || backend.Adapter.BitmapIndex.GetTable("sample") == nil {
		t.Fatalf("sample schema was not loaded into local BitmapIndex")
	}
	if _, err := os.Stat(filepath.Join(root, "data", "config")); err != nil {
		t.Fatalf("data/config was not staged: %v", err)
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
	if conn.LocalNodeServices.BitmapIndex == nil || conn.LocalNodeServices.KVStore == nil {
		t.Fatalf("local services = %+v, want bitmap and kv facades", conn.LocalNodeServices)
	}

	pool := backend.NewSessionPool(config, nil, 1)
	session, err := pool.Borrow("sample")
	if err != nil {
		t.Fatalf("Borrow(sample) error = %v", err)
	}
	if session.BitIndex == nil || session.KVStore == nil {
		t.Fatalf("session = %+v, want bitmap and kv clients", session)
	}
	pool.Return("sample", session)
	pool.Shutdown()
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
	tableDir := filepath.Join(configDir, table)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		t.Fatalf("mkdir schema dir: %v", err)
	}
	schema := `tableName: sample
primaryKey: id
attributes:
- fieldName: id
  sourceName: /id
  mappingStrategy: IntBSI
  type: Integer
- fieldName: city
  sourceName: /city
  mappingStrategy: StringHashBSI
  type: String
`
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
}
