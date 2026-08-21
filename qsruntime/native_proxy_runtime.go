package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// DefaultApplyRecommendedEdgeOrder is the engine default for guarded
// dependency-ordered relationship-vector graph reduction.
const DefaultApplyRecommendedEdgeOrder = true

// NativeProxyRuntime is the SQL-facing runtime handle that a wire-protocol server owns.
type NativeProxyRuntime struct {
	Runtime SQLRuntime
}

// Ready reports whether the underlying SQL runtime has a ready execution environment.
func (r NativeProxyRuntime) Ready() bool {
	return r.Runtime.Environment.Ready()
}

// ExecuteSQL executes SQL through the native runtime facade.
func (r NativeProxyRuntime) ExecuteSQL(ctx context.Context, sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) (SQLExecutionResult, error) {
	return r.Runtime.ExecuteSQL(ctx, sql, options, values...)
}

// WithSession returns a runtime view with connection-local session metadata applied.
func (r NativeProxyRuntime) WithSession(session qsbridge.SessionContext) NativeProxyRuntime {
	r.Runtime.Session = session.Clone()
	return r
}

// WithAuthorizer returns a runtime view with an adapter-owned access policy.
func (r NativeProxyRuntime) WithAuthorizer(authorizer qsbridge.AccessAuthorizer) NativeProxyRuntime {
	r.Runtime.Authorizer = authorizer
	return r
}

// ExecuteSQLWithSession executes SQL using connection-local session metadata.
func (r NativeProxyRuntime) ExecuteSQLWithSession(ctx context.Context, session qsbridge.SessionContext, sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) (SQLExecutionResult, error) {
	return r.WithSession(session).ExecuteSQL(ctx, sql, options, values...)
}

// InspectSQL inspects SQL through the native runtime facade without executing it.
func (r NativeProxyRuntime) InspectSQL(sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) SQLInspectionResult {
	return r.Runtime.InspectSQL(sql, options, values...)
}

// PrepareSQL prepares SQL metadata without binding execute-time values.
func (r NativeProxyRuntime) PrepareSQL(sql string) qsbridge.PreparedPlan {
	return r.Runtime.Plan(sql).PreparedPlan()
}

// PrepareSQLWithSession prepares SQL using connection-local session metadata.
func (r NativeProxyRuntime) PrepareSQLWithSession(sql string, session qsbridge.SessionContext) qsbridge.PreparedPlan {
	return r.WithSession(session).PrepareSQL(sql)
}

// NativeProxyRuntimeConfig captures runtime defaults shared by future proxy/server adapters.
type NativeProxyRuntimeConfig struct {
	Direct                      DirectRuntimeConfig
	DefaultSchema               string
	SchemaDir                   string
	CatalogVersion              qsbridge.CatalogVersion
	Functions                   []qsbridge.FunctionDefinition
	Profile                     RuntimeInspectionProfile
	ContextWrapper              func(context.Context) context.Context
	EnableFilterExpressions     bool
	DisableRecommendedEdgeOrder bool
}

// WithDefaults returns a config with QIAB-first native proxy defaults applied.
func (c NativeProxyRuntimeConfig) WithDefaults() NativeProxyRuntimeConfig {
	c.Direct = c.Direct.WithDefaults()
	if c.DefaultSchema == "" {
		c.DefaultSchema = "quanta"
	}
	if c.CatalogVersion == "" {
		c.CatalogVersion = qsbridge.CatalogVersion("native-proxy-runtime")
	}
	if c.Profile.Implementation == "" {
		c.Profile = LegacyDirectRuntimeProfile()
	}
	return c
}
