package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestRuntimeEnvironmentBuilderValidatesConfigBeforeFactories(t *testing.T) {
	builder := RuntimeEnvironmentBuilder{
		Config: NewDirectRuntimeConfig("", "", -1, 0),
		CatalogFactory: RuntimeCatalogFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (qsbridge.Catalog, qsbridge.DiagnosticSet, error) {
			t.Fatalf("catalog factory should not be called for invalid config")
			return nil, nil, nil
		}),
		DirectFactory: DirectRuntimeFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
			t.Fatalf("direct factory should not be called for invalid config")
			return nil, nil, nil
		}),
	}

	environment, diagnostics, err := builder.Build(context.Background())

	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if environment.Ready() {
		t.Fatalf("environment = %#v, want not ready", environment)
	}
	assertRuntimeDiagnosticCode(t, diagnostics, qsbridge.DiagnosticInvalidExecutionOption)
}

func TestRuntimeEnvironmentBuilderComposesCatalogAndExecution(t *testing.T) {
	builder := RuntimeEnvironmentBuilder{
		Config:         NewDirectRuntimeConfig("/tmp/quanta", "", 0, 3),
		CatalogFactory: LegacyTableCacheCatalogFactory{TableCache: legacyCatalogTestCache()},
		DirectFactory: DirectRuntimeFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
			if config.ServicePort != DefaultDirectServicePort {
				t.Fatalf("service port = %d, want %d", config.ServicePort, DefaultDirectServicePort)
			}
			if config.SessionPoolSize != 3 {
				t.Fatalf("session pool size = %d, want 3", config.SessionPoolSize)
			}
			return DirectRuntimeFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
				return ExecutionResult{Count: 7}, nil
			}), nil, nil
		}),
	}

	environment, diagnostics, err := builder.Build(context.Background())

	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if !environment.Ready() {
		t.Fatalf("environment = %#v, want ready", environment)
	}
	table, tableDiagnostics := environment.Catalog.Table("quanta", "orders")
	if tableDiagnostics.BlocksNative() {
		t.Fatalf("table diagnostics = %#v, want none", tableDiagnostics)
	}
	if table.Name != "orders" {
		t.Fatalf("table name = %q, want orders", table.Name)
	}
	result, err := environment.Execute(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Count != 7 {
		t.Fatalf("count = %d, want 7", result.Count)
	}
}

func TestRuntimeEnvironmentInspectDoesNotExecute(t *testing.T) {
	builder := RuntimeEnvironmentBuilder{
		Config:         NewDirectRuntimeConfig("", "", 0, 0),
		Profile:        FixtureRuntimeProfile("unit-test-fixture"),
		CatalogFactory: LegacyTableCacheCatalogFactory{TableCache: legacyCatalogTestCache()},
		DirectFactory: DirectRuntimeFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
			return DirectRuntimeFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
				t.Fatalf("inspect should not execute direct runtime")
				return ExecutionResult{}, nil
			}), nil, nil
		}),
	}
	environment, diagnostics, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}

	inspection := environment.Inspect(NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{Index: "orders"}},
	}))

	if !inspection.Supported() {
		t.Fatalf("inspection diagnostics = %#v", inspection.Diagnostics)
	}
	if inspection.SelectedExecutor != ExecutionInspectionExecutorDirect {
		t.Fatalf("selected executor = %q, want direct", inspection.SelectedExecutor)
	}
	if inspection.RuntimeProfile.Implementation != RuntimeImplementationFixture {
		t.Fatalf("runtime implementation = %q, want fixture", inspection.RuntimeProfile.Implementation)
	}
	if inspection.RuntimeProfile.Detail != "unit-test-fixture" {
		t.Fatalf("runtime detail = %q, want unit-test-fixture", inspection.RuntimeProfile.Detail)
	}
}

func TestRuntimeEnvironmentBuilderStopsOnCatalogDiagnostics(t *testing.T) {
	directCalled := false
	builder := RuntimeEnvironmentBuilder{
		Config:         NewDirectRuntimeConfig("", "", 0, 0),
		CatalogFactory: LegacyTableCacheCatalogFactory{},
		DirectFactory: DirectRuntimeFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
			directCalled = true
			return nil, nil, nil
		}),
	}

	environment, diagnostics, err := builder.Build(context.Background())

	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if environment.Ready() {
		t.Fatalf("environment = %#v, want not ready", environment)
	}
	if directCalled {
		t.Fatal("direct factory should not be called after catalog diagnostics")
	}
	assertRuntimeDiagnosticCode(t, diagnostics, qsbridge.DiagnosticInternalInvariant)
}

func TestRuntimeEnvironmentBuilderStopsOnExecutionDiagnostics(t *testing.T) {
	builder := RuntimeEnvironmentBuilder{
		Config:         NewDirectRuntimeConfig("", "", 0, 0),
		CatalogFactory: LegacyTableCacheCatalogFactory{TableCache: legacyCatalogTestCache()},
	}

	environment, diagnostics, err := builder.Build(context.Background())

	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if environment.Ready() {
		t.Fatalf("environment = %#v, want not ready", environment)
	}
	assertRuntimeDiagnosticCode(t, diagnostics, qsbridge.DiagnosticInternalInvariant)
}
