package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDistributedProxyConfigDefaults(t *testing.T) {
	config := distributedProxyConfig{}.withDefaults()
	if config.BindAddress != "127.0.0.1" {
		t.Fatalf("bind address = %q, want default loopback", config.BindAddress)
	}
	if config.MySQLPort != 4000 {
		t.Fatalf("mysql port = %d, want 4000", config.MySQLPort)
	}
	if config.ConsulAddress != "127.0.0.1:8500" {
		t.Fatalf("consul address = %q, want local agent", config.ConsulAddress)
	}
	if config.NodePort != defaultDistributedNodePort {
		t.Fatalf("node port = %d, want %d", config.NodePort, defaultDistributedNodePort)
	}
	if config.SchemaDir != "configuration" {
		t.Fatalf("schema dir = %q, want configuration", config.SchemaDir)
	}
	if config.Database != "quanta" {
		t.Fatalf("database = %q, want quanta", config.Database)
	}
	if config.mysqlAuthConfig().SummaryMode(config.Database) != "" {
		t.Fatalf("auth mode = %q, want unset", config.mysqlAuthConfig().SummaryMode(config.Database))
	}
}

func TestDistributedProxyAuthRequiresExplicitMode(t *testing.T) {
	if _, err := (distributedProxyConfig{}).mysqlAuthenticator(); err == nil || !strings.Contains(err.Error(), "mysql auth mode is required") {
		t.Fatalf("mysqlAuthenticator error = %v, want required auth guidance", err)
	}
}

func TestDistributedProxyRejectsPermissiveAuthOnNetworkBind(t *testing.T) {
	config := distributedProxyConfig{BindAddress: "0.0.0.0", AuthMode: "permissive"}
	if _, err := config.mysqlAuthenticator(); err == nil || !strings.Contains(err.Error(), "requires a loopback bind") {
		t.Fatalf("mysqlAuthenticator error = %v, want loopback guidance", err)
	}
}

func TestDistributedProxySummaryLines(t *testing.T) {
	process := distributedProxyProcess{
		Config: distributedProxyConfig{
			BindAddress:   "0.0.0.0",
			MySQLPort:     4000,
			ConsulAddress: "127.0.0.1:8500",
			NodePort:      4400,
			SchemaDir:     "tpc-h-benchmark/config",
			Database:      "quanta",
			AuthMode:      "static",
			AuthUser:      "bench",
			AuthPassword:  "secret",
		},
		Tables: []string{"lineitem", "orders"},
	}
	output := strings.Join(process.SummaryLines(), "\n")
	for _, want := range []string{
		"mode=distributed-proxy",
		"consul=127.0.0.1:8500",
		"node_port=4400",
		"mysql=0.0.0.0:4000",
		"schema_dir=tpc-h-benchmark/config",
		"auth=static",
		"tables=2 [lineitem,orders]",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("summary missing %q:\n%s", want, output)
		}
	}
}

func TestDistributedProxySummaryLinesReportStaticAuthUser(t *testing.T) {
	process := distributedProxyProcess{
		Config: distributedProxyConfig{
			BindAddress:   "127.0.0.1",
			MySQLPort:     4000,
			ConsulAddress: "127.0.0.1:8500",
			NodePort:      4400,
			SchemaDir:     "configuration",
			Database:      "quanta",
			AuthMode:      "static",
			AuthUser:      "bench",
			AuthPassword:  "secret",
		},
	}
	output := strings.Join(process.SummaryLines(), "\n")
	for _, want := range []string{"auth=static", "auth_user=bench"} {
		if !strings.Contains(output, want) {
			t.Fatalf("summary missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "secret") {
		t.Fatalf("summary leaked password material:\n%s", output)
	}
}

func TestDistributedProxySummaryLinesReportStaticAuthAccountFile(t *testing.T) {
	process := distributedProxyProcess{
		Config: distributedProxyConfig{
			BindAddress:     "127.0.0.1",
			MySQLPort:       4000,
			ConsulAddress:   "127.0.0.1:8500",
			NodePort:        4400,
			SchemaDir:       "configuration",
			Database:        "quanta",
			AuthMode:        "static",
			AuthUser:        "bench",
			AuthAccountFile: "/etc/quantastream/accounts.yaml",
		},
	}
	output := strings.Join(process.SummaryLines(), "\n")
	for _, want := range []string{"auth=static", "auth_account_file=/etc/quantastream/accounts.yaml"} {
		if !strings.Contains(output, want) {
			t.Fatalf("summary missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "auth_user=") {
		t.Fatalf("summary should not print one auth user when account file is configured:\n%s", output)
	}
}

func TestDistributedProxySummaryLinesReportAccessPolicyFile(t *testing.T) {
	process := distributedProxyProcess{
		Config: distributedProxyConfig{
			BindAddress:      "127.0.0.1",
			MySQLPort:        4000,
			ConsulAddress:    "127.0.0.1:8500",
			NodePort:         4400,
			SchemaDir:        "configuration",
			Database:         "quanta",
			AccessPolicyFile: "/etc/quantastream/access-policy.yaml",
		},
	}
	output := strings.Join(process.SummaryLines(), "\n")
	for _, want := range []string{"authorization=static_policy", "access_policy_file=/etc/quantastream/access-policy.yaml"} {
		if !strings.Contains(output, want) {
			t.Fatalf("summary missing %q:\n%s", want, output)
		}
	}
}

func TestDistributedProxyConfigCanLoadAccessPolicyFile(t *testing.T) {
	config := distributedProxyConfig{AccessPolicyFile: writeDistributedProxyTestAccessPolicyFile(t)}
	authorizer, err := config.accessAuthorizer()
	if err != nil {
		t.Fatalf("accessAuthorizer failed: %v", err)
	}
	if authorizer == nil {
		t.Fatalf("authorizer = nil, want loaded access policy")
	}
}

func writeDistributedProxyTestAccessPolicyFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "access-policy.yaml")
	if err := os.WriteFile(path, []byte(`
grants:
  - principal_kind: role
    principal: reader
    privilege: select
    schema: quanta
    table: orders
`), 0o600); err != nil {
		t.Fatalf("write access policy file: %v", err)
	}
	return path
}
