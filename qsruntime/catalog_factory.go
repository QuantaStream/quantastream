package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// RuntimeCatalogFactory builds the catalog view used by runtime-bound planning.
type RuntimeCatalogFactory interface {
	NewRuntimeCatalog(ctx context.Context, config DirectRuntimeConfig) (qsbridge.Catalog, qsbridge.DiagnosticSet, error)
}

// RuntimeCatalogFactoryFunc adapts a function to RuntimeCatalogFactory.
type RuntimeCatalogFactoryFunc func(ctx context.Context, config DirectRuntimeConfig) (qsbridge.Catalog, qsbridge.DiagnosticSet, error)

// NewRuntimeCatalog calls f(ctx, config).
func (f RuntimeCatalogFactoryFunc) NewRuntimeCatalog(ctx context.Context, config DirectRuntimeConfig) (qsbridge.Catalog, qsbridge.DiagnosticSet, error) {
	return f(ctx, config)
}

// RuntimeCatalogBuilder validates configuration before constructing a cached catalog.
type RuntimeCatalogBuilder struct {
	Config  DirectRuntimeConfig
	Factory RuntimeCatalogFactory
}

// Build validates the config and delegates catalog construction to the factory.
func (b RuntimeCatalogBuilder) Build(ctx context.Context) (qsbridge.Catalog, qsbridge.DiagnosticSet, error) {
	config := b.Config.WithDefaults()
	if diagnostics := config.Validate(); diagnostics.BlocksNative() {
		return nil, diagnostics, nil
	}
	if b.Factory == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseBind,
				"runtime catalog builder has no factory",
			),
		}, nil
	}
	return b.Factory.NewRuntimeCatalog(ctx, config)
}
