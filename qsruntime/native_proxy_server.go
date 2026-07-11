package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// NativeProxyServerConfig captures server-side ownership metadata before wire protocol binding.
type NativeProxyServerConfig struct {
	Route ExecutionRoute
}

// WithDefaults returns a server config that prefers the local direct QIAB route.
func (c NativeProxyServerConfig) WithDefaults() NativeProxyServerConfig {
	if c.Route.Path == "" {
		c.Route = DirectQIABRoute()
	}
	return c
}

// NativeProxyServer is the composition point that future wire-protocol adapters will own.
type NativeProxyServer struct {
	Runtime NativeProxyRuntime
	Route   ExecutionRoute
}

// NewNativeProxyServer builds the server-side owner for an already composed native runtime.
func NewNativeProxyServer(runtime NativeProxyRuntime, config NativeProxyServerConfig) NativeProxyServer {
	config = config.WithDefaults()
	return NativeProxyServer{
		Runtime: runtime,
		Route:   config.Route,
	}
}

// Ready reports whether the server has a usable native SQL runtime.
func (s NativeProxyServer) Ready() bool {
	return s.Runtime.Ready()
}

// ExecuteSQL delegates SQL execution to the owned native runtime when ready.
func (s NativeProxyServer) ExecuteSQL(ctx context.Context, sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) (SQLExecutionResult, error) {
	if !s.Ready() {
		return SQLExecutionResult{Diagnostics: nativeProxyServerNotReadyDiagnostics()}, nil
	}
	return s.Runtime.ExecuteSQL(ctx, sql, options, values...)
}

// InspectSQL delegates SQL inspection to the owned native runtime when ready.
func (s NativeProxyServer) InspectSQL(sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) SQLInspectionResult {
	if !s.Ready() {
		return SQLInspectionResult{Diagnostics: nativeProxyServerNotReadyDiagnostics()}
	}
	return s.Runtime.InspectSQL(sql, options, values...)
}

func nativeProxyServerNotReadyDiagnostics() qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "native proxy server runtime is not ready"),
	}
}
