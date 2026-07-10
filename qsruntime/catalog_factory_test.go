package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestRuntimeCatalogBuilderValidatesConfigBeforeFactory(t *testing.T) {
	builder := RuntimeCatalogBuilder{
		Config: NewDirectRuntimeConfig("", "", -1, 0),
		Factory: RuntimeCatalogFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (qsbridge.Catalog, qsbridge.DiagnosticSet, error) {
			t.Fatalf("factory should not be called for invalid config")
			return nil, nil, nil
		}),
	}

	catalog, diagnostics, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if catalog != nil {
		t.Fatalf("catalog = %#v, want nil", catalog)
	}
	assertRuntimeDiagnosticCode(t, diagnostics, qsbridge.DiagnosticInvalidExecutionOption)
}

func TestRuntimeCatalogBuilderReportsMissingFactory(t *testing.T) {
	builder := RuntimeCatalogBuilder{Config: NewDirectRuntimeConfig("", "", 0, 0)}

	catalog, diagnostics, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if catalog != nil {
		t.Fatalf("catalog = %#v, want nil", catalog)
	}
	assertRuntimeDiagnosticCode(t, diagnostics, qsbridge.DiagnosticInternalInvariant)
}

func TestRuntimeCatalogBuilderAppliesDefaultsBeforeFactory(t *testing.T) {
	var gotConfig DirectRuntimeConfig
	wantCatalog := qsbridge.MemoryCatalog{}
	builder := RuntimeCatalogBuilder{
		Config: NewDirectRuntimeConfig("/tmp/quanta", "", 0, 4),
		Factory: RuntimeCatalogFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (qsbridge.Catalog, qsbridge.DiagnosticSet, error) {
			gotConfig = config
			return wantCatalog, nil, nil
		}),
	}

	catalog, diagnostics, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if catalog == nil {
		t.Fatalf("catalog = nil, want runtime catalog")
	}
	if gotConfig.ServicePort != DefaultDirectServicePort {
		t.Fatalf("factory service port = %d, want %d", gotConfig.ServicePort, DefaultDirectServicePort)
	}
	if gotConfig.SessionPoolSize != 4 {
		t.Fatalf("factory session pool size = %d, want 4", gotConfig.SessionPoolSize)
	}
}
