package qsinabox

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsmysql"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/shared"
)

func TestStandardConfigAppliesStableDefaults(t *testing.T) {
	config := (StandardConfig{}).WithDefaults()
	if config.BindAddress != "127.0.0.1" {
		t.Fatalf("BindAddress = %q, want loopback default", config.BindAddress)
	}
	if config.MySQLPort != 4000 {
		t.Fatalf("MySQLPort = %d, want 4000", config.MySQLPort)
	}
	if config.NativeGRPCEnabled() {
		t.Fatalf("NativeGRPCEnabled() = true, want disabled by default")
	}
	if config.ConfigDir == "" || config.DataDir == "" || config.Database != "quanta" || config.WriteAheadLogPath != "" {
		t.Fatalf("config defaults = %+v", config)
	}
	if config.Address() != "127.0.0.1:4000" {
		t.Fatalf("Address() = %q", config.Address())
	}
}

func TestStandardConfigNativeGRPCAddressDefaultsToMySQLBind(t *testing.T) {
	config := StandardConfig{BindAddress: "0.0.0.0", NativeGRPCPort: 4100}.WithDefaults()
	if !config.NativeGRPCEnabled() {
		t.Fatalf("NativeGRPCEnabled() = false, want enabled")
	}
	if config.NativeGRPCAddress() != "0.0.0.0:4100" {
		t.Fatalf("NativeGRPCAddress() = %q, want 0.0.0.0:4100", config.NativeGRPCAddress())
	}
}

func TestStandardPlanReportsMissingLocalBackend(t *testing.T) {
	plan := NewStandardPlan(StandardConfig{}, shared.LocalNodeServices{})
	if plan.Ready {
		t.Fatalf("plan reported ready without local services")
	}
	if len(plan.Blockers) == 0 {
		t.Fatalf("plan blockers were empty")
	}
	if len(plan.StreamingRisk) == 0 {
		t.Fatalf("plan did not carry streaming risk assessment")
	}
	lines := strings.Join(plan.SummaryLines(), "\n")
	if !strings.Contains(lines, "mode=inabox-standard") {
		t.Fatalf("summary lines missing mode: %s", lines)
	}
	if !strings.Contains(lines, "streaming_risk=") {
		t.Fatalf("summary lines missing streaming risks: %s", lines)
	}
	if !strings.Contains(lines, "native_grpc=disabled") {
		t.Fatalf("summary lines missing native gRPC state: %s", lines)
	}
	if !strings.Contains(lines, "wal=disabled") {
		t.Fatalf("summary lines missing WAL state: %s", lines)
	}
	if !strings.Contains(lines, "auth=") {
		t.Fatalf("summary lines missing auth state: %s", lines)
	}
}

func TestStandardPlanReportsStaticAuthUser(t *testing.T) {
	plan := NewStandardPlan(StandardConfig{AuthMode: "static", AuthUser: "bench", AuthPassword: "secret"}, shared.LocalNodeServices{})
	lines := strings.Join(plan.SummaryLines(), "\n")
	for _, want := range []string{"auth=static", "auth_user=bench"} {
		if !strings.Contains(lines, want) {
			t.Fatalf("summary lines missing %q: %s", want, lines)
		}
	}
	if strings.Contains(lines, "secret") {
		t.Fatalf("summary lines leaked password material: %s", lines)
	}
}

func TestStandardPlanReportsStaticAuthAccountFile(t *testing.T) {
	plan := NewStandardPlan(StandardConfig{AuthMode: "static", AuthUser: "bench", AuthAccountFile: "/etc/quantastream/accounts.yaml"}, shared.LocalNodeServices{})
	lines := strings.Join(plan.SummaryLines(), "\n")
	for _, want := range []string{"auth=static", "auth_account_file=/etc/quantastream/accounts.yaml"} {
		if !strings.Contains(lines, want) {
			t.Fatalf("summary lines missing %q: %s", want, lines)
		}
	}
	if strings.Contains(lines, "auth_user=") {
		t.Fatalf("summary lines should not print one auth user when account file is configured: %s", lines)
	}
}

func TestStandardPlanReportsAccessPolicyFile(t *testing.T) {
	plan := NewStandardPlan(StandardConfig{AccessPolicyFile: "/etc/quantastream/access-policy.yaml"}, shared.LocalNodeServices{})
	lines := strings.Join(plan.SummaryLines(), "\n")
	for _, want := range []string{"authorization=static_policy", "access_policy_file=/etc/quantastream/access-policy.yaml"} {
		if !strings.Contains(lines, want) {
			t.Fatalf("summary lines missing %q: %s", want, lines)
		}
	}
}

func TestStandardPlanReportsWriteAheadLogPath(t *testing.T) {
	plan := NewStandardPlan(StandardConfig{WriteAheadLogPath: "/tmp/qs.wal"}, shared.LocalNodeServices{})
	lines := strings.Join(plan.SummaryLines(), "\n")
	if !strings.Contains(lines, "wal=/tmp/qs.wal") {
		t.Fatalf("summary lines missing WAL path: %s", lines)
	}
}

func TestObservedStandardPlanReportsWriteAheadLogRecoveryState(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "standard.wal")
	plan := NewObservedStandardPlan(StandardConfig{WriteAheadLogPath: walPath}, shared.LocalNodeServices{})
	lines := strings.Join(plan.SummaryLines(), "\n")
	if !plan.WALRecoveryObserved {
		t.Fatalf("WALRecoveryObserved = false, want observed recovery state")
	}
	for _, want := range []string{
		"wal_checkpoint_exists=false",
		"wal_checkpoint_lsn=0",
		"wal_last_lsn=0",
		"wal_record_count=0",
		"wal_checkpointed_records=0",
		"wal_replay_records=0",
		"wal_pending_records=0",
		"wal_replay_commit_boundaries=0",
	} {
		if !strings.Contains(lines, want) {
			t.Fatalf("summary lines missing %q: %s", want, lines)
		}
	}
}

func TestObservedStandardPlanWarnsWhenBSIPrimaryKeyManifestMissing(t *testing.T) {
	plan := NewObservedStandardPlan(StandardConfig{DataDir: t.TempDir()}, shared.LocalNodeServices{})
	lines := strings.Join(plan.SummaryLines(), "\n")

	if !strings.Contains(lines, "bsi_pk_authority_manifest=missing") {
		t.Fatalf("summary lines missing BSI PK manifest status: %s", lines)
	}
	if !strings.Contains(lines, "warning=BSI primary-key authority manifest is missing") {
		t.Fatalf("summary lines missing BSI PK manifest warning: %s", lines)
	}
}

func TestStandardFrontDoorConfigUsesExplicitLocalPermissiveAuth(t *testing.T) {
	config := StandardConfig{BindAddress: "127.0.0.1", MySQLPort: 4400, AuthMode: "permissive"}.NativeProxyFrontDoorConfig().WithDefaults()
	if config.BindAddress != "127.0.0.1" || config.Port != 4400 {
		t.Fatalf("front door bind = %s:%d, want 127.0.0.1:4400", config.BindAddress, config.Port)
	}
	if !config.PacketIOReady {
		t.Fatalf("front door packet IO should be ready for the existing MySQL adapter")
	}
	if !config.MySQLAdapter.PacketLoop {
		t.Fatalf("front door should use current MySQL byte-model readiness")
	}
	if _, ok := config.Authenticator.(qsmysql.PermissiveAuthenticator); !ok {
		t.Fatalf("authenticator = %T, want explicit permissive auth", config.Authenticator)
	}
}

func TestStandardFrontDoorConfigRejectsOmittedAuthMode(t *testing.T) {
	config := StandardConfig{}.NativeProxyFrontDoorConfig().WithDefaults()
	if _, ok := config.Authenticator.(qsmysql.RejectingAuthenticator); !ok {
		t.Fatalf("authenticator = %T, want rejecting authenticator", config.Authenticator)
	}
}

func TestStandardFrontDoorConfigRejectsNetworkPermissiveAuth(t *testing.T) {
	config := StandardConfig{BindAddress: "0.0.0.0", AuthMode: "permissive"}.NativeProxyFrontDoorConfig().WithDefaults()
	if _, ok := config.Authenticator.(qsmysql.RejectingAuthenticator); !ok {
		t.Fatalf("authenticator = %T, want rejecting authenticator", config.Authenticator)
	}
}

func TestStandardFrontDoorConfigCanUseStaticAuth(t *testing.T) {
	config := StandardConfig{AuthMode: "static", AuthUser: "bench", AuthPassword: "secret"}.NativeProxyFrontDoorConfig().WithDefaults()

	if _, ok := config.Authenticator.(qsmysql.StaticAuthenticator); !ok {
		t.Fatalf("authenticator = %T, want static authenticator", config.Authenticator)
	}
}

func TestStandardFrontDoorConfigCanUseAccessPolicyFile(t *testing.T) {
	config := StandardConfig{AccessPolicyFile: writeStandardTestAccessPolicyFile(t)}.NativeProxyFrontDoorConfig().WithDefaults()

	if config.Server.Authorizer == nil {
		t.Fatalf("Authorizer = nil, want access policy file authorizer")
	}
}

func TestStandardFrontDoorConfigCanEnableRuntimeProbeLogging(t *testing.T) {
	config := StandardConfig{RuntimeProbeLogging: true}.NativeProxyFrontDoorConfig().WithDefaults()

	if config.Server.ProbeLogger == nil {
		t.Fatalf("ProbeLogger = nil, want runtime probe logger when enabled")
	}
}

func TestStandardFrontDoorConfigCanEnableMySQLCommandTraceLogging(t *testing.T) {
	config := StandardConfig{MySQLCommandTraceLogging: true}.NativeProxyFrontDoorConfig().WithDefaults()

	if config.MySQLCommandLogger == nil {
		t.Fatalf("MySQLCommandLogger = nil, want command trace logger when enabled")
	}
}

func TestStandardDirectRuntimeWiresRelationshipReaderSessions(t *testing.T) {
	mount := (StandardLocalBackend{}).NewDirectRuntime(StandardConfig{DataDir: t.TempDir()}, nil, 1)
	defer mount.Close()

	reader, ok := mount.Runtime.RelationshipReader.(*qsruntime.LegacyDirectRelationshipVectorReader)
	if !ok || reader == nil {
		t.Fatalf("relationship reader = %T, want *LegacyDirectRelationshipVectorReader", mount.Runtime.RelationshipReader)
	}
	backend, ok := reader.Backend.(qsruntime.LegacyDirectBitIndexRelationshipVectorBackend)
	if !ok {
		t.Fatalf("relationship backend = %T, want LegacyDirectBitIndexRelationshipVectorBackend", reader.Backend)
	}
	if backend.Sessions == nil {
		t.Fatalf("relationship backend sessions = nil, want standard session provider for direct candidate expansion")
	}
}

func writeStandardTestAccessPolicyFile(t *testing.T) string {
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
