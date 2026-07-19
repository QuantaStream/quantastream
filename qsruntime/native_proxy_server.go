package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

// NativeProxyFrontDoorConfig captures the bounded MySQL-compatible front-door entry point.
type NativeProxyFrontDoorConfig struct {
	Server        NativeProxyServerConfig
	Protocol      qsbridge.ProtocolProfile
	BindAddress   string
	Port          int
	PacketIOReady bool
	MySQLAdapter  qsmysql.AdapterReadiness
	Authenticator qsmysql.Authenticator
}

// WithDefaults returns a MySQL/QIAB front-door config without claiming packet IO is implemented.
func (c NativeProxyFrontDoorConfig) WithDefaults() NativeProxyFrontDoorConfig {
	c.Server = c.Server.WithDefaults()
	if c.Protocol.Kind == qsbridge.ProtocolUnknown {
		c.Protocol = qsbridge.NewProtocolProfile(
			qsbridge.ProtocolMySQL,
			"mysql-wire",
			qsbridge.ProtocolCapabilityPreparedStatements,
			qsbridge.ProtocolCapabilityStatementResults,
			qsbridge.ProtocolCapabilitySessionActions,
			qsbridge.ProtocolCapabilityExplain,
		)
	}
	if c.BindAddress == "" {
		c.BindAddress = "127.0.0.1"
	}
	if c.Port == 0 {
		c.Port = 4000
	}
	if c.MySQLAdapter == (qsmysql.AdapterReadiness{}) {
		c.MySQLAdapter = qsmysql.ByteModelReadiness()
	}
	return c
}

// NativeProxyFrontDoor is a protocol-aware bootstrap view, not a network listener.
type NativeProxyFrontDoor struct {
	Server        NativeProxyServer
	Protocol      qsbridge.ProtocolProfile
	BindAddress   string
	Port          int
	PacketIOReady bool
	MySQLAdapter  qsmysql.AdapterReadiness
	Authenticator qsmysql.Authenticator
}

// NewNativeProxyFrontDoor builds a MySQL-facing front-door bootstrap wrapper around the native SQL server.
func NewNativeProxyFrontDoor(runtime NativeProxyRuntime, config NativeProxyFrontDoorConfig) NativeProxyFrontDoor {
	config = config.WithDefaults()
	authenticator := config.Authenticator
	if authenticator == nil {
		authenticator = qsmysql.PermissiveAuthenticator{}
	}
	return NativeProxyFrontDoor{
		Server:        NewNativeProxyServer(runtime, config.Server),
		Protocol:      config.Protocol.Clone(),
		BindAddress:   config.BindAddress,
		Port:          config.Port,
		PacketIOReady: config.PacketIOReady || config.MySQLAdapter.PacketIOReady(),
		MySQLAdapter:  config.MySQLAdapter,
		Authenticator: authenticator,
	}
}

// RuntimeReady reports whether SQL execution can be reached behind the front door.
func (f NativeProxyFrontDoor) RuntimeReady() bool {
	return f.Server.Ready()
}

// WireReady reports whether the network packet adapter is present.
func (f NativeProxyFrontDoor) WireReady() bool {
	return f.PacketIOReady
}

// Ready reports whether the front door is ready to accept network clients.
func (f NativeProxyFrontDoor) Ready() bool {
	return f.RuntimeReady() && f.WireReady()
}

// NativeProxyFrontDoorSummary is a small status row for the MySQL-compatible front-door milestone.
type NativeProxyFrontDoorSummary struct {
	Protocol     qsbridge.ProtocolKind
	Driver       string
	BindAddress  string
	Port         int
	Route        ExecutionPath
	RuntimeReady bool
	WireReady    bool
	Ready        bool
	AdapterReady bool
	NextStep     string
}

// Summary returns protocol-facing readiness metadata for the front door scaffold.
func (f NativeProxyFrontDoor) Summary() NativeProxyFrontDoorSummary {
	summary := NativeProxyFrontDoorSummary{
		Protocol:     f.Protocol.Kind,
		Driver:       f.Protocol.Driver,
		BindAddress:  f.BindAddress,
		Port:         f.Port,
		Route:        f.Server.Route.Path,
		RuntimeReady: f.RuntimeReady(),
		WireReady:    f.WireReady(),
		Ready:        f.Ready(),
		AdapterReady: f.MySQLAdapter.PacketCodec && f.MySQLAdapter.Handshake && f.MySQLAdapter.HandshakeResponse && f.MySQLAdapter.CommandDecoder && f.MySQLAdapter.Resultsets && f.MySQLAdapter.StatementResponses && f.MySQLAdapter.CommandResponses && f.MySQLAdapter.ConnectionState && f.MySQLAdapter.PacketLoop,
	}
	if !summary.WireReady {
		summary.NextStep = f.MySQLAdapter.NextStep()
		if summary.NextStep == "" {
			summary.NextStep = "implement MySQL packet IO adapter and command loop"
		}
	} else if !summary.RuntimeReady {
		summary.NextStep = "attach ready native SQL runtime"
	}
	return summary
}

// NativeProxyServerConfig captures server-side ownership metadata before wire protocol binding.
type NativeProxyServerConfig struct {
	Route          ExecutionRoute
	ProbeLogger    RuntimeProbeLogger
	ContextWrapper func(context.Context) context.Context
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
	Runtime        NativeProxyRuntime
	Route          ExecutionRoute
	ProbeLogger    RuntimeProbeLogger
	ContextWrapper func(context.Context) context.Context
}

// NewNativeProxyServer builds the server-side owner for an already composed native runtime.
func NewNativeProxyServer(runtime NativeProxyRuntime, config NativeProxyServerConfig) NativeProxyServer {
	config = config.WithDefaults()
	return NativeProxyServer{
		Runtime:        runtime,
		Route:          config.Route,
		ProbeLogger:    config.ProbeLogger,
		ContextWrapper: config.ContextWrapper,
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
	if s.ContextWrapper != nil {
		ctx = s.ContextWrapper(ctx)
	}
	result, err := s.Runtime.ExecuteSQL(ctx, sql, options, values...)
	s.logRuntimeProbes(result.Runtime.Probes)
	return result, err
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

// RuntimeProbeLogger receives execution probes emitted behind a protocol server.
type RuntimeProbeLogger interface {
	LogRuntimeProbe(ExecutionProbe)
}

// RuntimeProbeLoggerFunc adapts a function into RuntimeProbeLogger.
type RuntimeProbeLoggerFunc func(ExecutionProbe)

// LogRuntimeProbe logs one execution probe.
func (f RuntimeProbeLoggerFunc) LogRuntimeProbe(probe ExecutionProbe) {
	f(probe)
}

func (s NativeProxyServer) logRuntimeProbes(probes []ExecutionProbe) {
	if s.ProbeLogger == nil {
		return
	}
	for _, probe := range probes {
		s.ProbeLogger.LogRuntimeProbe(probe)
	}
}
