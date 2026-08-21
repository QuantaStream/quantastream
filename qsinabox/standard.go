package qsinabox

import (
	"fmt"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/shared"
	log "github.com/sirupsen/logrus"
)

// StandardMode is the single-process QIAB deployment mode name.
const StandardMode = "inabox-standard"

// StandardConfig captures the first process-level configuration surface for
// the single-process QIAB executable.
type StandardConfig struct {
	BindAddress         string
	MySQLPort           int
	NativeGRPCBind      string
	NativeGRPCPort      int
	ConfigDir           string
	DataDir             string
	WriteAheadLogPath   string
	Database            string
	AuthMode            string
	AuthUser            string
	AuthPassword        string
	RuntimeProbeLogging bool
}

// WithDefaults fills stable local defaults for inabox-standard.
func (c StandardConfig) WithDefaults() StandardConfig {
	if c.BindAddress == "" {
		c.BindAddress = "127.0.0.1"
	}
	if c.MySQLPort == 0 {
		c.MySQLPort = 4000
	}
	if c.NativeGRPCBind == "" {
		c.NativeGRPCBind = c.BindAddress
	}
	if c.ConfigDir == "" {
		c.ConfigDir = "configuration"
	}
	if c.DataDir == "" {
		c.DataDir = "data"
	}
	if c.Database == "" {
		c.Database = "quanta"
	}
	return c
}

// Address returns the MySQL bind address for the standard process.
func (c StandardConfig) Address() string {
	c = c.WithDefaults()
	return fmt.Sprintf("%s:%d", c.BindAddress, c.MySQLPort)
}

// NativeGRPCEnabled reports whether the standard process should publish the
// native node gRPC service surface.
func (c StandardConfig) NativeGRPCEnabled() bool {
	return c.NativeGRPCPort > 0
}

// NativeGRPCAddress returns the native node gRPC bind address.
func (c StandardConfig) NativeGRPCAddress() string {
	c = c.WithDefaults()
	return fmt.Sprintf("%s:%d", c.NativeGRPCBind, c.NativeGRPCPort)
}

// MySQLAuthConfig returns the normalized MySQL front-door auth configuration.
func (c StandardConfig) MySQLAuthConfig() qsmysql.AuthConfig {
	c = c.WithDefaults()
	return qsmysql.AuthConfig{
		Mode:     c.AuthMode,
		Username: c.AuthUser,
		Password: c.AuthPassword,
		Database: c.Database,
	}.WithDefaults(c.Database)
}

// MySQLAuthenticator builds the configured MySQL front-door authenticator.
func (c StandardConfig) MySQLAuthenticator() (qsmysql.Authenticator, error) {
	c = c.WithDefaults()
	return c.MySQLAuthConfig().Authenticator(c.Database)
}

// NativeProxyFrontDoorConfig returns the MySQL front-door defaults for this mode.
func (c StandardConfig) NativeProxyFrontDoorConfig() qsruntime.NativeProxyFrontDoorConfig {
	c = c.WithDefaults()
	serverConfig := qsruntime.NativeProxyServerConfig{
		ContextWrapper: qsruntime.WithQueryScratchpad,
	}
	if c.RuntimeProbeLogging {
		serverConfig.ProbeLogger = qsruntime.RuntimeProbeLoggerFunc(func(probe qsruntime.ExecutionProbe) {
			log.Infof("RUNTIME probe section=%s name=%s value=%s detail=%s", probe.Section, probe.Name, probe.Value, probe.Detail)
		})
	}
	authenticator, err := c.MySQLAuthenticator()
	if err != nil {
		authenticator = qsmysql.RejectingAuthenticator{Message: err.Error()}
	}
	return qsruntime.NativeProxyFrontDoorConfig{
		Server:        serverConfig,
		BindAddress:   c.BindAddress,
		Port:          c.MySQLPort,
		PacketIOReady: true,
		MySQLAdapter:  qsmysql.ByteModelReadiness(),
		Authenticator: authenticator,
		Protocol: qsbridge.NewProtocolProfile(
			qsbridge.ProtocolMySQL,
			"mysql-wire",
			qsbridge.ProtocolCapabilityStatementResults,
			qsbridge.ProtocolCapabilitySessionActions,
			qsbridge.ProtocolCapabilityExplain,
		),
	}
}

// StandardPlan is a non-executing vertical skeleton of standard-mode process
// composition. It keeps startup explicit while the local backend is mounted.
type StandardPlan struct {
	Mode                string
	Config              StandardConfig
	LocalNode           shared.LocalNodeReadiness
	StreamingRisk       []shared.LocalNodeStreamingRisk
	PKAuthority         core.BSIPrimaryKeyAuthorityManifestObservation
	WALRecovery         core.LocalWALRecoveryPlan
	WALRecoveryObserved bool
	Ready               bool
	Blockers            []string
	Warnings            []string
}

// NewStandardPlan summarizes the current standard-mode boundary readiness.
func NewStandardPlan(config StandardConfig, services shared.LocalNodeServices) StandardPlan {
	config = config.WithDefaults()
	localReadiness := services.Readiness()
	plan := StandardPlan{
		Mode:          StandardMode,
		Config:        config,
		LocalNode:     localReadiness,
		StreamingRisk: append([]shared.LocalNodeStreamingRisk(nil), localReadiness.StreamingRisks...),
		Blockers:      append([]string(nil), localReadiness.Blockers...),
		Warnings:      append([]string(nil), localReadiness.Warnings...),
	}
	if !localReadiness.Ready {
		plan.Blockers = append(plan.Blockers, "inabox-standard local node backend is not ready")
	}
	plan.Ready = len(plan.Blockers) == 0
	return plan
}

// NewObservedStandardPlan summarizes standard-mode readiness with startup
// metadata observations that require reading the local data directory.
func NewObservedStandardPlan(config StandardConfig, services shared.LocalNodeServices) StandardPlan {
	plan := NewStandardPlan(config, services)
	policy := observeStandardBSIPrimaryKeyAuthorityPolicy(config)
	plan.PKAuthority = policy.Observation
	if policy.Warning != "" {
		plan.Warnings = append(plan.Warnings, policy.Warning)
	}
	if config.WithDefaults().WriteAheadLogPath != "" {
		recoveryPlan, err := core.PlanLocalWALRecovery(config.WithDefaults().WriteAheadLogPath)
		if err != nil {
			plan.Blockers = append(plan.Blockers, "inabox-standard WAL recovery plan failed: "+err.Error())
			plan.Ready = false
		} else {
			plan.WALRecovery = recoveryPlan
			plan.WALRecoveryObserved = true
		}
	}
	return plan
}

// SummaryLines returns stable human-readable startup status lines for the CLI.
func (p StandardPlan) SummaryLines() []string {
	config := p.Config.WithDefaults()
	lines := []string{
		fmt.Sprintf("mode=%s", p.Mode),
		fmt.Sprintf("mysql=%s", config.Address()),
		"native_grpc=disabled",
		fmt.Sprintf("config_dir=%s", config.ConfigDir),
		fmt.Sprintf("data_dir=%s", config.DataDir),
		"wal=disabled",
		fmt.Sprintf("database=%s", config.Database),
		fmt.Sprintf("auth=%s", config.MySQLAuthConfig().SummaryMode(config.Database)),
		fmt.Sprintf("local_node_ready=%t", p.LocalNode.Ready),
	}
	if user := config.MySQLAuthConfig().SummaryUser(config.Database); user != "" {
		lines = append(lines, fmt.Sprintf("auth_user=%s", user))
	}
	if config.NativeGRPCEnabled() {
		lines[2] = fmt.Sprintf("native_grpc=%s", config.NativeGRPCAddress())
	}
	if config.WriteAheadLogPath != "" {
		lines[5] = fmt.Sprintf("wal=%s", config.WriteAheadLogPath)
	}
	if p.WALRecoveryObserved {
		lines = append(lines,
			fmt.Sprintf("wal_checkpoint_exists=%t", p.WALRecovery.CheckpointExists),
			fmt.Sprintf("wal_checkpoint_lsn=%d", p.WALRecovery.CheckpointLSN),
			fmt.Sprintf("wal_last_lsn=%d", p.WALRecovery.LastLSN),
			fmt.Sprintf("wal_record_count=%d", p.WALRecovery.RecordCount),
			fmt.Sprintf("wal_checkpointed_records=%d", p.WALRecovery.CheckpointedRecordCount),
			fmt.Sprintf("wal_replay_records=%d", p.WALRecovery.ReplayRecordCount()),
			fmt.Sprintf("wal_pending_records=%d", p.WALRecovery.PendingRecordCount()),
			fmt.Sprintf("wal_replay_commit_boundaries=%d", p.WALRecovery.ReplayCommitBoundaryCount),
		)
	}
	if p.PKAuthority.Status != "" {
		lines = append(lines,
			fmt.Sprintf("bsi_pk_authority_manifest=%s", p.PKAuthority.Status),
			fmt.Sprintf("bsi_pk_authority_manifest_entries=%d", p.PKAuthority.Entries),
		)
		if p.PKAuthority.ValidationLevel != "" {
			lines = append(lines, "bsi_pk_authority_manifest_validation="+p.PKAuthority.ValidationLevel)
		}
		if p.PKAuthority.ArtifactTrust != "" {
			lines = append(lines, "bsi_pk_authority_manifest_artifact_trust="+p.PKAuthority.ArtifactTrust)
		}
		if p.PKAuthority.ArtifactPresence != "" {
			lines = append(lines, "bsi_pk_authority_manifest_artifact_presence="+p.PKAuthority.ArtifactPresence)
		}
		if p.PKAuthority.ArtifactDescriptors != 0 {
			lines = append(lines, fmt.Sprintf("bsi_pk_authority_manifest_artifacts=%d", p.PKAuthority.ArtifactDescriptors))
		}
		if p.PKAuthority.ArtifactPresent != 0 {
			lines = append(lines, fmt.Sprintf("bsi_pk_authority_manifest_artifacts_present=%d", p.PKAuthority.ArtifactPresent))
		}
		if p.PKAuthority.ArtifactMissing != 0 {
			lines = append(lines, fmt.Sprintf("bsi_pk_authority_manifest_artifacts_missing=%d", p.PKAuthority.ArtifactMissing))
		}
		if p.PKAuthority.ArtifactFileCount != 0 {
			lines = append(lines, fmt.Sprintf("bsi_pk_authority_manifest_artifact_file_count=%d", p.PKAuthority.ArtifactFileCount))
		}
		if p.PKAuthority.ArtifactDetail != "" {
			lines = append(lines, "bsi_pk_authority_manifest_artifact_detail="+p.PKAuthority.ArtifactDetail)
		}
		if p.PKAuthority.EntryKeyCount != 0 {
			lines = append(lines, fmt.Sprintf("bsi_pk_authority_manifest_entry_key_count=%d", p.PKAuthority.EntryKeyCount))
		}
		if p.PKAuthority.ArtifactKeyCount != 0 {
			lines = append(lines, fmt.Sprintf("bsi_pk_authority_manifest_artifact_key_count=%d", p.PKAuthority.ArtifactKeyCount))
		}
		if p.PKAuthority.KeyCountMismatches != 0 {
			lines = append(lines, fmt.Sprintf("bsi_pk_authority_manifest_key_count_mismatches=%d", p.PKAuthority.KeyCountMismatches))
		}
		if p.PKAuthority.CleanEntries != 0 {
			lines = append(lines, fmt.Sprintf("bsi_pk_authority_manifest_clean_entries=%d", p.PKAuthority.CleanEntries))
		}
		if p.PKAuthority.DirtyEntries != 0 {
			lines = append(lines, fmt.Sprintf("bsi_pk_authority_manifest_dirty_entries=%d", p.PKAuthority.DirtyEntries))
		}
		if p.PKAuthority.ManifestEntry != "" {
			lines = append(lines, "bsi_pk_authority_manifest_entry="+p.PKAuthority.ManifestEntry)
		}
		if p.PKAuthority.Detail != "" {
			lines = append(lines, "bsi_pk_authority_manifest_detail="+p.PKAuthority.Detail)
		}
	}
	for _, blocker := range p.Blockers {
		lines = append(lines, "blocker="+blocker)
	}
	for _, warning := range p.Warnings {
		lines = append(lines, "warning="+warning)
	}
	for _, risk := range p.StreamingRisk {
		lines = append(lines, fmt.Sprintf("streaming_risk=%s.%s: %s", risk.Service, risk.Method, risk.Gate))
	}
	return lines
}
