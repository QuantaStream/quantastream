package main

import "strings"

type engineExecutionDescriptor struct {
	Engine              string
	ExecutionPath       string
	PlannerProcess      string
	QueryTransport      string
	UsesMySQLProxy      bool
	UsesNodeRPC         bool
	PlannerDeployTarget string
	NodeDeployScope     string
	Description         string
}

func describeEngineExecution(engine string) engineExecutionDescriptor {
	engine = strings.ToLower(strings.TrimSpace(engine))
	switch engine {
	case engineInaboxDirect:
		return engineExecutionDescriptor{
			Engine:              engine,
			ExecutionPath:       "sqlrunner_direct_cluster_rpc",
			PlannerProcess:      "sqlrunner",
			QueryTransport:      "direct_node_rpc",
			UsesMySQLProxy:      false,
			UsesNodeRPC:         true,
			PlannerDeployTarget: "bench_runner_sqlrunner",
			NodeDeployScope:     "node_code_only",
			Description:         "SQL runtime runs inside sqlrunner and talks directly to Consul-discovered quantastream-node RPC services.",
		}
	case engineProxy, engineDistributed:
		return engineExecutionDescriptor{
			Engine:              engine,
			ExecutionPath:       "mysql_proxy_endpoint",
			PlannerProcess:      "quantastream_proxy",
			QueryTransport:      "mysql_wire",
			UsesMySQLProxy:      true,
			UsesNodeRPC:         true,
			PlannerDeployTarget: "quantastream_proxy",
			NodeDeployScope:     "node_code_only",
			Description:         "sqlrunner is a MySQL client; query planning/execution happens behind the MySQL-compatible proxy endpoint.",
		}
	case engineInaboxLocal, engineInaboxStandard:
		return engineExecutionDescriptor{
			Engine:              engine,
			ExecutionPath:       "mysql_proxy_endpoint",
			PlannerProcess:      "quantastream_proxy",
			QueryTransport:      "mysql_wire",
			UsesMySQLProxy:      true,
			UsesNodeRPC:         true,
			PlannerDeployTarget: "quantastream_proxy",
			NodeDeployScope:     "node_code_only",
			Description:         "sqlrunner is a MySQL client for an inabox proxy endpoint.",
		}
	case engineRuntime:
		return engineExecutionDescriptor{
			Engine:              engine,
			ExecutionPath:       "sqlrunner_fixture_runtime",
			PlannerProcess:      "sqlrunner",
			QueryTransport:      "in_process",
			UsesMySQLProxy:      false,
			UsesNodeRPC:         false,
			PlannerDeployTarget: "bench_runner_sqlrunner",
			NodeDeployScope:     "not_exercised",
			Description:         "SQL runtime runs in-process against the sqlrunner fixture runtime.",
		}
	case engineRuntimeInspect:
		return engineExecutionDescriptor{
			Engine:              engine,
			ExecutionPath:       "sqlrunner_fixture_runtime_inspect",
			PlannerProcess:      "sqlrunner",
			QueryTransport:      "in_process",
			UsesMySQLProxy:      false,
			UsesNodeRPC:         false,
			PlannerDeployTarget: "bench_runner_sqlrunner",
			NodeDeployScope:     "not_exercised",
			Description:         "SQL runtime inspection runs in-process without executing against cluster nodes.",
		}
	case engineMySQLReference:
		return engineExecutionDescriptor{
			Engine:              engine,
			ExecutionPath:       "mysql_reference_database",
			PlannerProcess:      "mysql_reference",
			QueryTransport:      "mysql_wire",
			UsesMySQLProxy:      false,
			UsesNodeRPC:         false,
			PlannerDeployTarget: "mysql_reference",
			NodeDeployScope:     "not_exercised",
			Description:         "sqlrunner is a MySQL client for the reference database.",
		}
	default:
		return engineExecutionDescriptor{
			Engine:              engine,
			ExecutionPath:       "unknown",
			PlannerProcess:      "unknown",
			QueryTransport:      "unknown",
			PlannerDeployTarget: "unknown",
			NodeDeployScope:     "unknown",
			Description:         "engine execution path is not recognized",
		}
	}
}

func (d engineExecutionDescriptor) BenchmarkMetadata() map[string]string {
	metadata := map[string]string{
		"execution_path":        d.ExecutionPath,
		"planner_process":       d.PlannerProcess,
		"query_transport":       d.QueryTransport,
		"uses_mysql_proxy":      formatBoolMetadata(d.UsesMySQLProxy),
		"uses_node_rpc":         formatBoolMetadata(d.UsesNodeRPC),
		"planner_deploy_target": d.PlannerDeployTarget,
		"node_deploy_scope":     d.NodeDeployScope,
	}
	return metadata
}

func formatBoolMetadata(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
