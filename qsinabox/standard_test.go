package qsinabox

import (
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
	if !strings.Contains(lines, "auth=permissive") {
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

func TestStandardFrontDoorConfigUsesMySQLWireDefaults(t *testing.T) {
	config := StandardConfig{BindAddress: "0.0.0.0", MySQLPort: 4400}.NativeProxyFrontDoorConfig().WithDefaults()
	if config.BindAddress != "0.0.0.0" || config.Port != 4400 {
		t.Fatalf("front door bind = %s:%d, want 0.0.0.0:4400", config.BindAddress, config.Port)
	}
	if !config.PacketIOReady {
		t.Fatalf("front door packet IO should be ready for the existing MySQL adapter")
	}
	if !config.MySQLAdapter.PacketLoop {
		t.Fatalf("front door should use current MySQL byte-model readiness")
	}
	if _, ok := config.Authenticator.(qsmysql.PermissiveAuthenticator); !ok {
		t.Fatalf("authenticator = %T, want permissive default", config.Authenticator)
	}
}

func TestStandardFrontDoorConfigCanUseStaticAuth(t *testing.T) {
	config := StandardConfig{AuthMode: "static", AuthUser: "bench", AuthPassword: "secret"}.NativeProxyFrontDoorConfig().WithDefaults()

	if _, ok := config.Authenticator.(qsmysql.StaticAuthenticator); !ok {
		t.Fatalf("authenticator = %T, want static authenticator", config.Authenticator)
	}
}

func TestStandardFrontDoorConfigCanEnableRuntimeProbeLogging(t *testing.T) {
	config := StandardConfig{RuntimeProbeLogging: true}.NativeProxyFrontDoorConfig().WithDefaults()

	if config.Server.ProbeLogger == nil {
		t.Fatalf("ProbeLogger = nil, want runtime probe logger when enabled")
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
