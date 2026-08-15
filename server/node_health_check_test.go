package server

import "testing"

func TestNodeConsulHealthCheckUsesDefaults(t *testing.T) {
	t.Setenv(nodeConsulHealthCheckProfileEnv, "")
	t.Setenv(nodeConsulHealthCheckIntervalEnv, "")
	t.Setenv(nodeConsulHealthCheckTimeoutEnv, "")
	t.Setenv(nodeConsulHealthCheckFailuresBeforeCriticalEnv, "")
	t.Setenv(nodeConsulHealthCheckDeregisterAfterEnv, "")

	check := nodeConsulHealthCheck("127.0.0.1", 4400, "Status")

	if check.GRPC != "127.0.0.1:4400/Status" {
		t.Fatalf("grpc = %q, want node health endpoint", check.GRPC)
	}
	if check.Interval != checkInterval.String() {
		t.Fatalf("interval = %q, want %q", check.Interval, checkInterval.String())
	}
	if check.Timeout != checkTimeout.String() {
		t.Fatalf("timeout = %q, want %q", check.Timeout, checkTimeout.String())
	}
	if check.FailuresBeforeCritical != checkFailuresBeforeCritical {
		t.Fatalf("failures before critical = %d, want %d", check.FailuresBeforeCritical, checkFailuresBeforeCritical)
	}
	if check.DeregisterCriticalServiceAfter != checkDeregisterCriticalServiceAfter.String() {
		t.Fatalf("deregister after = %q, want %q", check.DeregisterCriticalServiceAfter, checkDeregisterCriticalServiceAfter.String())
	}
}

func TestNodeConsulHealthCheckUsesBulkLoadProfile(t *testing.T) {
	t.Setenv(nodeConsulHealthCheckProfileEnv, "bulk-load")
	t.Setenv(nodeConsulHealthCheckIntervalEnv, "")
	t.Setenv(nodeConsulHealthCheckTimeoutEnv, "")
	t.Setenv(nodeConsulHealthCheckFailuresBeforeCriticalEnv, "")
	t.Setenv(nodeConsulHealthCheckDeregisterAfterEnv, "")

	check := nodeConsulHealthCheck("172.31.4.204", 4400, "Status")

	if check.Interval != bulkLoadCheckInterval.String() {
		t.Fatalf("interval = %q, want %q", check.Interval, bulkLoadCheckInterval.String())
	}
	if check.Timeout != bulkLoadCheckTimeout.String() {
		t.Fatalf("timeout = %q, want %q", check.Timeout, bulkLoadCheckTimeout.String())
	}
	if check.FailuresBeforeCritical != bulkLoadCheckFailuresBeforeCritical {
		t.Fatalf("failures before critical = %d, want %d", check.FailuresBeforeCritical, bulkLoadCheckFailuresBeforeCritical)
	}
	if check.DeregisterCriticalServiceAfter != bulkLoadCheckDeregisterAfter.String() {
		t.Fatalf("deregister after = %q, want %q", check.DeregisterCriticalServiceAfter, bulkLoadCheckDeregisterAfter.String())
	}
}

func TestNodeConsulHealthCheckUsesExplicitEnvOverrides(t *testing.T) {
	t.Setenv(nodeConsulHealthCheckProfileEnv, "production")
	t.Setenv(nodeConsulHealthCheckIntervalEnv, "10s")
	t.Setenv(nodeConsulHealthCheckTimeoutEnv, "12s")
	t.Setenv(nodeConsulHealthCheckFailuresBeforeCriticalEnv, "6")
	t.Setenv(nodeConsulHealthCheckDeregisterAfterEnv, "10m")

	check := nodeConsulHealthCheck("172.31.4.204", 4400, "Status")

	if check.GRPC != "172.31.4.204:4400/Status" {
		t.Fatalf("grpc = %q, want node health endpoint", check.GRPC)
	}
	if check.Interval != "10s" {
		t.Fatalf("interval = %q, want 10s", check.Interval)
	}
	if check.Timeout != "12s" {
		t.Fatalf("timeout = %q, want 12s", check.Timeout)
	}
	if check.FailuresBeforeCritical != 6 {
		t.Fatalf("failures before critical = %d, want 6", check.FailuresBeforeCritical)
	}
	if check.DeregisterCriticalServiceAfter != "10m0s" {
		t.Fatalf("deregister after = %q, want 10m0s", check.DeregisterCriticalServiceAfter)
	}
}
