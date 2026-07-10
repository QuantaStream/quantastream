package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// RuntimeEnvironment is the composed runtime surface used by SQL-facing adapters.
type RuntimeEnvironment struct {
	Config    DirectRuntimeConfig
	Catalog   qsbridge.Catalog
	Execution ExecutionService
	Profile   RuntimeInspectionProfile
}

// Ready reports whether the environment has the minimum runtime dependencies.
func (e RuntimeEnvironment) Ready() bool {
	return e.Catalog != nil && e.Execution.Selector.Direct != nil
}

// Execute runs a neutral execution request through the environment execution service.
func (e RuntimeEnvironment) Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	return e.Execution.Execute(ctx, request)
}

// Inspect returns routing and call-plan metadata without executing the request.
func (e RuntimeEnvironment) Inspect(request ExecutionRequest) ExecutionInspection {
	inspection := e.Execution.Inspect(request)
	inspection.RuntimeProfile = e.Profile.Effective()
	inspection.CallPlan = inspection.CallPlan.WithRuntimeProfile(inspection.RuntimeProfile)
	return inspection
}

// RuntimeEnvironmentBuilder composes catalog and execution factories into one runtime surface.
type RuntimeEnvironmentBuilder struct {
	Config         DirectRuntimeConfig
	CatalogFactory RuntimeCatalogFactory
	DirectFactory  DirectRuntimeFactory
	LegacyExecutor Executor
	Profile        RuntimeInspectionProfile
}

// Build validates shared runtime config and constructs catalog plus execution service.
func (b RuntimeEnvironmentBuilder) Build(ctx context.Context) (RuntimeEnvironment, qsbridge.DiagnosticSet, error) {
	config := b.Config.WithDefaults()
	if diagnostics := config.Validate(); diagnostics.BlocksNative() {
		return RuntimeEnvironment{}, diagnostics, nil
	}

	catalog, diagnostics, err := RuntimeCatalogBuilder{
		Config:  config,
		Factory: b.CatalogFactory,
	}.Build(ctx)
	if err != nil || diagnostics.BlocksNative() {
		return RuntimeEnvironment{}, diagnostics, err
	}

	execution, diagnostics, err := NewDirectExecutionService(ctx, DirectRuntimeBuilder{
		Config:  config,
		Factory: b.DirectFactory,
	}, b.LegacyExecutor)
	if err != nil || diagnostics.BlocksNative() {
		return RuntimeEnvironment{}, diagnostics, err
	}

	return RuntimeEnvironment{
		Config:    config,
		Catalog:   catalog,
		Execution: execution,
		Profile:   b.Profile.Effective(),
	}, nil, nil
}
