package qsbridge

// ClientDriverEcosystem identifies a client-driver ecosystem Quanta should support.
type ClientDriverEcosystem string

const (
	// ClientDriverEcosystemUnknown means the client ecosystem is not classified.
	ClientDriverEcosystemUnknown ClientDriverEcosystem = ""
	// ClientDriverEcosystemMySQL identifies generic MySQL protocol clients.
	ClientDriverEcosystemMySQL ClientDriverEcosystem = "mysql"
	// ClientDriverEcosystemNodeJS identifies Node.js MySQL client drivers.
	ClientDriverEcosystemNodeJS ClientDriverEcosystem = "nodejs"
	// ClientDriverEcosystemPython identifies Python MySQL client drivers.
	ClientDriverEcosystemPython ClientDriverEcosystem = "python"
	// ClientDriverEcosystemJava identifies Java JDBC/MySQL client drivers.
	ClientDriverEcosystemJava ClientDriverEcosystem = "java"
	// ClientDriverEcosystemGo identifies Go database/sql MySQL client drivers.
	ClientDriverEcosystemGo ClientDriverEcosystem = "go"
	// ClientDriverEcosystemNativeGo identifies future in-process Go APIs.
	ClientDriverEcosystemNativeGo ClientDriverEcosystem = "native_go"
	// ClientDriverEcosystemGRPC identifies future typed gRPC clients.
	ClientDriverEcosystemGRPC ClientDriverEcosystem = "grpc"
)

// ClientDriverCompatibility describes one intended client-driver compatibility target.
type ClientDriverCompatibility struct {
	Name         string
	Ecosystem    ClientDriverEcosystem
	Protocol     ProtocolKind
	Status       CompatibilityStatus
	Drivers      []string
	Capabilities ProtocolCapabilities
	AuthPlugins  []AuthenticationPlugin
	Notes        string
}

// ClientDriverCompatibilitySummary describes aggregate client-driver target metadata.
type ClientDriverCompatibilitySummary struct {
	ProfileCount            int
	MySQLProtocolCount      int
	GoProtocolCount         int
	GRPCProtocolCount       int
	BoundaryOnlyCount       int
	TypedAPICount           int
	PreparedStatementCount  int
	BatchExecutionCount     int
	StreamingResultCount    int
	CancellationCount       int
	StructuredExplainCount  int
	PlanCachePolicyCount    int
	PasswordAuthTargetCount int
	TokenAuthTargetCount    int
}

// DefaultClientDriverCompatibility returns Quanta's intended client-driver targets.
func DefaultClientDriverCompatibility() []ClientDriverCompatibility {
	return cloneClientDriverCompatibility(defaultClientDriverCompatibility)
}

// DefaultClientDriverCompatibilitySummary returns aggregate metadata for default client-driver targets.
func DefaultClientDriverCompatibilitySummary() ClientDriverCompatibilitySummary {
	return SummarizeClientDriverCompatibility(DefaultClientDriverCompatibility())
}

// SummarizeClientDriverCompatibility returns aggregate metadata for client-driver targets.
func SummarizeClientDriverCompatibility(profiles []ClientDriverCompatibility) ClientDriverCompatibilitySummary {
	summary := ClientDriverCompatibilitySummary{ProfileCount: len(profiles)}
	for _, profile := range profiles {
		switch profile.Protocol {
		case ProtocolMySQL:
			summary.MySQLProtocolCount++
		case ProtocolGo:
			summary.GoProtocolCount++
			summary.TypedAPICount++
		case ProtocolGRPC:
			summary.GRPCProtocolCount++
			summary.TypedAPICount++
		}
		if profile.Status == CompatibilityStatusBoundaryOnly {
			summary.BoundaryOnlyCount++
		}
		if profile.Capabilities.Has(ProtocolCapabilityPreparedStatements) {
			summary.PreparedStatementCount++
		}
		if profile.Capabilities.Has(ProtocolCapabilityBatchExecution) {
			summary.BatchExecutionCount++
		}
		if profile.Capabilities.Has(ProtocolCapabilityStreamingResults) {
			summary.StreamingResultCount++
		}
		if profile.Capabilities.Has(ProtocolCapabilityCancellation) {
			summary.CancellationCount++
		}
		if profile.Capabilities.Has(ProtocolCapabilityStructuredExplain) {
			summary.StructuredExplainCount++
		}
		if profile.Capabilities.Has(ProtocolCapabilityPlanCachePolicy) {
			summary.PlanCachePolicyCount++
		}
		if clientDriverAuthPluginsContainAny(profile.AuthPlugins, AuthenticationPluginCachingSHA2Password, AuthenticationPluginMySQLNativePassword, AuthenticationPluginMySQLClearPassword) {
			summary.PasswordAuthTargetCount++
		}
		if clientDriverAuthPluginsContainAny(profile.AuthPlugins, AuthenticationPluginBearerJWT, AuthenticationPluginOpenIDConnect) {
			summary.TokenAuthTargetCount++
		}
	}
	return summary
}

var defaultClientDriverCompatibility = []ClientDriverCompatibility{
	{
		Name:      "mysql_wire_protocol",
		Ecosystem: ClientDriverEcosystemMySQL,
		Protocol:  ProtocolMySQL,
		Status:    CompatibilityStatusBoundaryOnly,
		Drivers:   []string{"mysql", "mariadb-compatible"},
		Capabilities: ProtocolCapabilities{
			ProtocolCapabilityPreparedStatements,
			ProtocolCapabilityBatchExecution,
			ProtocolCapabilityStatementResults,
			ProtocolCapabilitySessionActions,
		},
		AuthPlugins: []AuthenticationPlugin{
			AuthenticationPluginCachingSHA2Password,
			AuthenticationPluginMySQLNativePassword,
		},
		Notes: "protocol adapter owns packet compatibility while qsbridge owns metadata contracts",
	},
	{
		Name:      "nodejs_mysql_clients",
		Ecosystem: ClientDriverEcosystemNodeJS,
		Protocol:  ProtocolMySQL,
		Status:    CompatibilityStatusBoundaryOnly,
		Drivers:   []string{"mysql2", "mysql"},
		Capabilities: ProtocolCapabilities{
			ProtocolCapabilityPreparedStatements,
			ProtocolCapabilityBatchExecution,
			ProtocolCapabilityStreamingResults,
		},
		AuthPlugins: []AuthenticationPlugin{
			AuthenticationPluginCachingSHA2Password,
			AuthenticationPluginMySQLNativePassword,
		},
		Notes: "target Node.js MySQL drivers through the MySQL protocol adapter",
	},
	{
		Name:      "python_mysql_clients",
		Ecosystem: ClientDriverEcosystemPython,
		Protocol:  ProtocolMySQL,
		Status:    CompatibilityStatusBoundaryOnly,
		Drivers:   []string{"mysql-connector-python", "PyMySQL", "mysqlclient"},
		Capabilities: ProtocolCapabilities{
			ProtocolCapabilityPreparedStatements,
			ProtocolCapabilityBatchExecution,
		},
		AuthPlugins: []AuthenticationPlugin{
			AuthenticationPluginCachingSHA2Password,
			AuthenticationPluginMySQLNativePassword,
		},
		Notes: "target Python MySQL drivers through the MySQL protocol adapter",
	},
	{
		Name:      "java_jdbc_clients",
		Ecosystem: ClientDriverEcosystemJava,
		Protocol:  ProtocolMySQL,
		Status:    CompatibilityStatusBoundaryOnly,
		Drivers:   []string{"mysql-connector-j", "mariadb-jdbc"},
		Capabilities: ProtocolCapabilities{
			ProtocolCapabilityPreparedStatements,
			ProtocolCapabilityBatchExecution,
			ProtocolCapabilityStatementResults,
		},
		AuthPlugins: []AuthenticationPlugin{
			AuthenticationPluginCachingSHA2Password,
			AuthenticationPluginMySQLNativePassword,
		},
		Notes: "target JDBC clients through the MySQL protocol adapter",
	},
	{
		Name:      "go_mysql_clients",
		Ecosystem: ClientDriverEcosystemGo,
		Protocol:  ProtocolMySQL,
		Status:    CompatibilityStatusBoundaryOnly,
		Drivers:   []string{"go-sql-driver/mysql"},
		Capabilities: ProtocolCapabilities{
			ProtocolCapabilityPreparedStatements,
			ProtocolCapabilityBatchExecution,
			ProtocolCapabilityCancellation,
		},
		AuthPlugins: []AuthenticationPlugin{
			AuthenticationPluginCachingSHA2Password,
			AuthenticationPluginMySQLNativePassword,
			AuthenticationPluginMySQLClearPassword,
		},
		Notes: "target database/sql MySQL clients through the MySQL protocol adapter",
	},
	{
		Name:      "native_go_api",
		Ecosystem: ClientDriverEcosystemNativeGo,
		Protocol:  ProtocolGo,
		Status:    CompatibilityStatusBoundaryOnly,
		Drivers:   []string{"in-process-go"},
		Capabilities: ProtocolCapabilities{
			ProtocolCapabilityPreparedStatements,
			ProtocolCapabilityBatchExecution,
			ProtocolCapabilityStreamingResults,
			ProtocolCapabilityCancellation,
			ProtocolCapabilityExplain,
			ProtocolCapabilityStructuredExplain,
			ProtocolCapabilityProfile,
			ProtocolCapabilityPlanCachePolicy,
		},
		AuthPlugins: []AuthenticationPlugin{
			AuthenticationPluginBearerJWT,
			AuthenticationPluginOpenIDConnect,
		},
		Notes: "future direct Go API should adapt the same engine contracts without MySQL wire semantics",
	},
	{
		Name:      "grpc_api",
		Ecosystem: ClientDriverEcosystemGRPC,
		Protocol:  ProtocolGRPC,
		Status:    CompatibilityStatusBoundaryOnly,
		Drivers:   []string{"grpc"},
		Capabilities: ProtocolCapabilities{
			ProtocolCapabilityPreparedStatements,
			ProtocolCapabilityBatchExecution,
			ProtocolCapabilityStreamingResults,
			ProtocolCapabilityCancellation,
			ProtocolCapabilityStructuredExplain,
			ProtocolCapabilityProfile,
			ProtocolCapabilityPlanCachePolicy,
		},
		AuthPlugins: []AuthenticationPlugin{
			AuthenticationPluginBearerJWT,
			AuthenticationPluginOpenIDConnect,
		},
		Notes: "future typed service API should expose the same planning and execution contracts across languages",
	},
}

func cloneClientDriverCompatibility(profiles []ClientDriverCompatibility) []ClientDriverCompatibility {
	if len(profiles) == 0 {
		return nil
	}
	cloned := make([]ClientDriverCompatibility, 0, len(profiles))
	for _, profile := range profiles {
		profile.Drivers = append([]string(nil), profile.Drivers...)
		profile.Capabilities = append(ProtocolCapabilities(nil), profile.Capabilities...)
		profile.AuthPlugins = append([]AuthenticationPlugin(nil), profile.AuthPlugins...)
		cloned = append(cloned, profile)
	}
	return cloned
}

func clientDriverAuthPluginsContainAny(plugins []AuthenticationPlugin, targets ...AuthenticationPlugin) bool {
	for _, plugin := range plugins {
		for _, target := range targets {
			if plugin == target {
				return true
			}
		}
	}
	return false
}
