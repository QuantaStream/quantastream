package admin

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/hashicorp/consul/api"
)

// BackupCmd groups provider-neutral storage backup operations.
type BackupCmd struct {
	Create   BackupCreateCmd   `cmd:"" help:"Create a local filesystem storage backup."`
	Inspect  BackupInspectCmd  `cmd:"" help:"Print a local filesystem storage backup manifest summary without checksum validation."`
	Validate BackupValidateCmd `cmd:"" help:"Validate a local filesystem storage backup."`
	Restore  BackupRestoreCmd  `cmd:"" help:"Restore a local filesystem storage backup into an empty data directory."`
	Smoke    BackupSmokeCmd    `cmd:"" help:"Restore a backup into a temporary directory and validate the restored image."`
	Quiesce  BackupQuiesceCmd  `cmd:"" help:"Inspect or release local backup quiescence leases."`
}

type BackupCreateCmd struct {
	DataDir        string        `help:"QuantaStream data directory to snapshot." default:"data"`
	Target         string        `help:"Backup target directory. Supports file:///path or a local path." required:""`
	Quiesce        bool          `help:"Create a local storage write barrier during the snapshot." default:"false"`
	WALPath        string        `help:"Optional local WAL path to record in the backup checkpoint."`
	EngineFlush    string        `help:"Optional running engine flush before the local snapshot: none, standard-native, or distributed. The distributed option is a commit primitive only, not a coordinated distributed backup." default:"none" enum:"none,standard-native,distributed"`
	NativeGRPCAddr string        `help:"Native gRPC endpoint used with --engine-flush=standard-native." default:"127.0.0.1:4100"`
	FlushTimeout   time.Duration `help:"Timeout for running engine flush before snapshot." default:"5m"`
}

type BackupValidateCmd struct {
	Source string `help:"Backup source directory. Supports file:///path or a local path." required:""`
}

type BackupInspectCmd struct {
	Source string `help:"Backup source directory. Supports file:///path or a local path." required:""`
}

type BackupRestoreCmd struct {
	Source  string `help:"Backup source directory. Supports file:///path or a local path." required:""`
	DataDir string `help:"Empty QuantaStream data directory to restore into." default:"data"`
}

type BackupSmokeCmd struct {
	Source         string `help:"Backup source directory. Supports file:///path or a local path." required:""`
	TempDir        string `help:"Optional parent directory for the temporary restore smoke directory."`
	KeepRestoreDir bool   `help:"Keep the temporary restore directory for manual inspection." default:"false"`
}

type BackupQuiesceCmd struct {
	Status  BackupQuiesceStatusCmd  `cmd:"" help:"Show the active local backup quiescence lease, if any."`
	Release BackupQuiesceReleaseCmd `cmd:"" help:"Release an active local backup quiescence lease."`
}

type BackupQuiesceStatusCmd struct {
	DataDir string `help:"QuantaStream data directory to inspect." default:"data"`
}

type BackupQuiesceReleaseCmd struct {
	DataDir string `help:"QuantaStream data directory to release." default:"data"`
	LeaseID string `help:"Expected active lease id. Omit only with --force."`
	Force   bool   `help:"Release the active lease without matching an expected lease id." default:"false"`
}

func (c *BackupCreateCmd) Run(ctx *Context) error {
	flushBeforeSnapshot, err := c.flushBeforeSnapshot(ctx)
	if err != nil {
		return err
	}
	manifest, err := core.CreateLocalStorageBackup(context.Background(), core.CreateLocalStorageBackupRequest{
		DataDir:             c.DataDir,
		Target:              c.Target,
		Quiesce:             c.Quiesce,
		WALPath:             c.WALPath,
		Owner:               "qstream-admin backup create",
		FlushBeforeSnapshot: flushBeforeSnapshot,
	})
	if err != nil {
		return err
	}
	target, err := core.ResolveLocalFileTarget(c.Target)
	if err != nil {
		return err
	}
	fmt.Printf("backup_created=%s\n", target)
	printBackupManifestSummary(manifest)
	printBackupEngineFlushSummary(c.engineFlushMode(), c.NativeGRPCAddr, c.FlushTimeout)
	if c.Quiesce {
		printQuiescentBackupLiveSourceNotes(c.engineFlushMode())
	}
	return nil
}

func (c *BackupCreateCmd) flushBeforeSnapshot(ctx *Context) (func(context.Context, string) error, error) {
	mode := c.engineFlushMode()
	if mode == "none" {
		return nil, nil
	}
	if !c.Quiesce {
		return nil, fmt.Errorf("--engine-flush requires --quiesce so the engine commit happens under a local storage write barrier")
	}
	timeout := effectiveBackupFlushTimeout(c.FlushTimeout)
	switch mode {
	case "standard-native":
		addr := strings.TrimSpace(c.NativeGRPCAddr)
		if addr == "" {
			return nil, fmt.Errorf("--native-grpc-addr is required with --engine-flush=standard-native")
		}
		return func(parent context.Context, dataDir string) error {
			if err := commitEngineBeforeSnapshot(parent, timeout, shared.LoaderConnectionConfig{
				Mode:    shared.LoaderConnectionStandardNative,
				Owner:   "qstream-admin-backup",
				Address: addr,
			}); err != nil {
				return err
			}
			return core.FlushLocalStorageForBackup(parent, dataDir)
		}, nil
	case "distributed":
		return func(parent context.Context, dataDir string) error {
			consulAddr := "127.0.0.1:8500"
			if ctx != nil && strings.TrimSpace(ctx.ConsulAddr) != "" {
				consulAddr = strings.TrimSpace(ctx.ConsulAddr)
			}
			consul, err := api.NewClient(&api.Config{Address: consulAddr})
			if err != nil {
				return fmt.Errorf("create Consul client for backup engine flush: %w", err)
			}
			if err := commitEngineBeforeSnapshot(parent, timeout, shared.LoaderConnectionConfig{
				Mode:   shared.LoaderConnectionDistributed,
				Owner:  "qstream-admin-backup",
				Consul: consul,
			}); err != nil {
				return err
			}
			return core.FlushLocalStorageForBackup(parent, dataDir)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported --engine-flush %q", c.EngineFlush)
	}
}

func (c *BackupCreateCmd) engineFlushMode() string {
	mode := strings.ToLower(strings.TrimSpace(c.EngineFlush))
	if mode == "" {
		return "none"
	}
	return mode
}

func effectiveBackupFlushTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 5 * time.Minute
	}
	return timeout
}

func printBackupEngineFlushSummary(mode, nativeGRPCAddr string, timeout time.Duration) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" || mode == "none" {
		return
	}
	fmt.Printf("backup_engine_flush=%s\n", mode)
	if mode == "standard-native" {
		fmt.Printf("backup_engine_flush_native_grpc_addr=%s\n", strings.TrimSpace(nativeGRPCAddr))
	}
	if mode == "distributed" {
		fmt.Printf("backup_engine_flush_distributed_scope=commit_only\n")
		fmt.Printf("backup_engine_flush_distributed_backup_supported=false\n")
		fmt.Printf("backup_engine_flush_distributed_backup_issue=https://github.com/QuantaStream/quantastream/issues/10\n")
	}
	fmt.Printf("backup_engine_flush_timeout=%s\n", effectiveBackupFlushTimeout(timeout))
}

func commitEngineBeforeSnapshot(parent context.Context, timeout time.Duration, config shared.LoaderConnectionConfig) error {
	flushCtx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	conn, err := shared.NewLoaderConnection(flushCtx, config)
	if err != nil {
		return fmt.Errorf("connect running engine for backup flush: %w", err)
	}
	defer conn.Disconnect()
	if err := shared.NewBitmapIndex(conn).CommitWithContext(flushCtx); err != nil {
		return fmt.Errorf("commit running engine for backup flush: %w", err)
	}
	return nil
}

func (c *BackupInspectCmd) Run(ctx *Context) error {
	manifest, source, err := core.LoadLocalStorageBackupManifest(c.Source)
	if err != nil {
		return err
	}
	fmt.Printf("backup_inspected=%s\n", source)
	printBackupManifestSummary(manifest)
	return nil
}

func (c *BackupValidateCmd) Run(ctx *Context) error {
	manifest, err := core.ValidateLocalStorageBackup(c.Source)
	if err != nil {
		return err
	}
	source, err := core.ResolveLocalFileTarget(c.Source)
	if err != nil {
		return err
	}
	fmt.Printf("backup_valid=%s\n", source)
	printBackupManifestSummary(manifest)
	return nil
}

func (c *BackupRestoreCmd) Run(ctx *Context) error {
	manifest, err := core.RestoreLocalStorageBackup(context.Background(), core.RestoreLocalStorageBackupRequest{
		Source:  c.Source,
		DataDir: c.DataDir,
	})
	if err != nil {
		return err
	}
	target, err := core.ResolveLocalFileTarget(c.DataDir)
	if err != nil {
		return err
	}
	fmt.Printf("backup_restored=%s\n", target)
	printBackupManifestSummary(manifest)
	return nil
}

func (c *BackupSmokeCmd) Run(ctx *Context) error {
	manifest, err := core.ValidateLocalStorageBackup(c.Source)
	if err != nil {
		return err
	}
	source, err := core.ResolveLocalFileTarget(c.Source)
	if err != nil {
		return err
	}
	tempParent := strings.TrimSpace(c.TempDir)
	if tempParent != "" {
		tempParent, err = core.ResolveLocalFileTarget(tempParent)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(tempParent, 0755); err != nil {
			return fmt.Errorf("create backup smoke temp parent: %w", err)
		}
	}
	restoreDir, err := os.MkdirTemp(tempParent, "quantastream-backup-smoke-*")
	if err != nil {
		return fmt.Errorf("create backup smoke restore directory: %w", err)
	}
	if !c.KeepRestoreDir {
		defer func() { _ = os.RemoveAll(restoreDir) }()
	}
	if _, err := core.RestoreLocalStorageBackup(context.Background(), core.RestoreLocalStorageBackupRequest{
		Source:  c.Source,
		DataDir: restoreDir,
	}); err != nil {
		return err
	}
	if err := core.ValidateLocalStorageRestoredDataDir(restoreDir, manifest); err != nil {
		return err
	}
	fmt.Printf("backup_smoke_valid=%s\n", source)
	fmt.Printf("backup_smoke_restore_dir=%s\n", restoreDir)
	fmt.Printf("backup_smoke_restore_kept=%t\n", c.KeepRestoreDir)
	printBackupManifestSummary(manifest)
	if manifest.Checkpoint.WALIncluded {
		plan, err := planRestoredBackupWAL(restoreDir, manifest)
		if err != nil {
			return err
		}
		fmt.Printf("backup_smoke_wal_plan_valid=true\n")
		printWALRecoveryPlan(plan)
	}
	return nil
}

func (c *BackupQuiesceStatusCmd) Run(ctx *Context) error {
	path, err := core.LocalStorageQuiescencePath(c.DataDir)
	if err != nil {
		return err
	}
	lease, found, err := core.ObserveLocalStorageQuiescence(c.DataDir)
	if err != nil {
		return err
	}
	fmt.Printf("backup_quiescence_path=%s\n", path)
	fmt.Printf("backup_quiescence_active=%t\n", found)
	if found {
		printBackupQuiescenceLeaseSummary(lease)
	}
	return nil
}

func (c *BackupQuiesceReleaseCmd) Run(ctx *Context) error {
	if c.LeaseID == "" && !c.Force {
		return fmt.Errorf("backup quiesce release requires --lease-id or --force")
	}
	path, err := core.LocalStorageQuiescencePath(c.DataDir)
	if err != nil {
		return err
	}
	lease, found, err := core.ObserveLocalStorageQuiescence(c.DataDir)
	if err != nil {
		return err
	}
	fmt.Printf("backup_quiescence_path=%s\n", path)
	if !found {
		fmt.Printf("backup_quiescence_active=false\n")
		return nil
	}
	leaseID := c.LeaseID
	if c.Force {
		leaseID = ""
	}
	if err := core.EndLocalStorageQuiescence(c.DataDir, leaseID); err != nil {
		return err
	}
	fmt.Printf("backup_quiescence_released=true\n")
	printBackupQuiescenceLeaseSummary(lease)
	return nil
}

func printBackupManifestSummary(manifest core.LocalStorageBackupManifest) {
	fmt.Printf("backup_format=%s\n", manifest.Format)
	fmt.Printf("backup_version=%d\n", manifest.Version)
	fmt.Printf("backup_mode=%s\n", manifest.Mode)
	if manifest.Product.Name != "" {
		fmt.Printf("backup_product=%s\n", manifest.Product.Name)
	}
	if manifest.Product.ShortName != "" {
		fmt.Printf("backup_product_short=%s\n", manifest.Product.ShortName)
	}
	if manifest.Product.Version != "" {
		fmt.Printf("backup_product_version=%s\n", manifest.Product.Version)
	}
	if manifest.Product.Commit != "" {
		fmt.Printf("backup_product_commit=%s\n", manifest.Product.Commit)
	}
	if manifest.Product.BuildDate != "" {
		fmt.Printf("backup_product_build_date=%s\n", manifest.Product.BuildDate)
	}
	if manifest.Product.Summary != "" {
		fmt.Printf("backup_product_summary=%s\n", manifest.Product.Summary)
	}
	fmt.Printf("backup_files=%d\n", manifest.FileCount)
	fmt.Printf("backup_directories=%d\n", manifest.DirectoryCount)
	fmt.Printf("backup_bytes=%d\n", manifest.ByteCount)
	if manifest.SearchIndex.Observed {
		fmt.Printf("backup_search_index_present=%t\n", manifest.SearchIndex.Present)
		fmt.Printf("backup_search_index_kind=%s\n", manifest.SearchIndex.Kind)
		fmt.Printf("backup_search_index_path=%s\n", manifest.SearchIndex.Path)
		fmt.Printf("backup_search_index_files=%d\n", manifest.SearchIndex.FileCount)
		fmt.Printf("backup_search_index_directories=%d\n", manifest.SearchIndex.DirectoryCount)
		fmt.Printf("backup_search_index_bytes=%d\n", manifest.SearchIndex.ByteCount)
	}
	fmt.Printf("backup_checkpoint_kind=%s\n", manifest.Checkpoint.Kind)
	fmt.Printf("backup_wal_included=%t\n", manifest.Checkpoint.WALIncluded)
	fmt.Printf("backup_checkpoint_lsn=%d\n", manifest.Checkpoint.LSN)
	if manifest.Checkpoint.WALPath != "" {
		fmt.Printf("backup_wal_path=%s\n", manifest.Checkpoint.WALPath)
	}
	if manifest.Checkpoint.WALCheckpointPath != "" {
		fmt.Printf("backup_wal_checkpoint_path=%s\n", manifest.Checkpoint.WALCheckpointPath)
	}
	if manifest.Checkpoint.WALLastLSN != 0 {
		fmt.Printf("backup_wal_last_lsn=%d\n", manifest.Checkpoint.WALLastLSN)
	}
	if manifest.Checkpoint.WALReplayRecords != 0 {
		fmt.Printf("backup_wal_replay_records=%d\n", manifest.Checkpoint.WALReplayRecords)
	}
	if manifest.Checkpoint.WALPendingRecords != 0 {
		fmt.Printf("backup_wal_pending_records=%d\n", manifest.Checkpoint.WALPendingRecords)
	}
}

func printQuiescentBackupLiveSourceNotes(engineFlushMode string) {
	fmt.Printf("backup_live_source_requires_committed_state=true\n")
	switch strings.ToLower(strings.TrimSpace(engineFlushMode)) {
	case "none", "":
		fmt.Printf("backup_live_source_flush_hint=commit_or_drain_live_engine_before_snapshot\n")
	case "distributed":
		fmt.Printf("backup_live_source_flush_hint=distributed_commit_only_local_snapshot_not_cluster_backup\n")
	default:
		fmt.Printf("backup_live_source_flush_hint=engine_flush_after_quiesce_drain_external_clients\n")
	}
	fmt.Printf("backup_live_source_snapshot_scope=durable_filesystem_state\n")
}

func planRestoredBackupWAL(restoreDir string, manifest core.LocalStorageBackupManifest) (core.LocalWALRecoveryPlan, error) {
	if strings.TrimSpace(manifest.Checkpoint.WALPath) == "" {
		return core.LocalWALRecoveryPlan{}, fmt.Errorf("backup manifest marks WAL included but has no WAL path")
	}
	walPath, err := restoredBackupPathForOriginal(restoreDir, manifest, manifest.Checkpoint.WALPath)
	if err != nil {
		return core.LocalWALRecoveryPlan{}, err
	}
	opts := core.LocalWALOptions{}
	if strings.TrimSpace(manifest.Checkpoint.WALCheckpointPath) != "" {
		checkpointPath, err := restoredBackupPathForOriginal(restoreDir, manifest, manifest.Checkpoint.WALCheckpointPath)
		if err != nil {
			return core.LocalWALRecoveryPlan{}, err
		}
		opts.CheckpointPath = checkpointPath
	}
	return core.PlanLocalWALRecoveryWithOptions(walPath, opts)
}

func restoredBackupPathForOriginal(restoreDir string, manifest core.LocalStorageBackupManifest, originalPath string) (string, error) {
	sourceDir := strings.TrimSpace(manifest.SourceDataDir)
	if sourceDir == "" {
		return "", fmt.Errorf("backup manifest has no source data directory for restored WAL mapping")
	}
	relPath, err := filepath.Rel(filepath.Clean(sourceDir), filepath.Clean(originalPath))
	if err != nil {
		return "", fmt.Errorf("map backup WAL path %q into restored data directory: %w", originalPath, err)
	}
	if relPath == "." || relPath == ".." || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) || filepath.IsAbs(relPath) {
		return "", fmt.Errorf("backup WAL path %q is outside source data directory %q", originalPath, sourceDir)
	}
	return filepath.Join(restoreDir, relPath), nil
}

func printBackupQuiescenceLeaseSummary(lease core.LocalStorageQuiescenceLease) {
	fmt.Printf("backup_quiescence_id=%s\n", lease.ID)
	fmt.Printf("backup_quiescence_owner=%s\n", lease.Owner)
	fmt.Printf("backup_quiescence_reason=%s\n", lease.Reason)
	fmt.Printf("backup_quiescence_data_dir=%s\n", lease.DataDir)
	fmt.Printf("backup_quiescence_created_at=%s\n", lease.CreatedAt.Format("2006-01-02T15:04:05Z07:00"))
	if lease.WALPath != "" {
		fmt.Printf("backup_quiescence_wal_path=%s\n", lease.WALPath)
	}
	if lease.WALCheckpointPath != "" {
		fmt.Printf("backup_quiescence_wal_checkpoint_path=%s\n", lease.WALCheckpointPath)
	}
	if lease.CheckpointLSN != 0 {
		fmt.Printf("backup_quiescence_checkpoint_lsn=%d\n", lease.CheckpointLSN)
	}
	if lease.LastLSN != 0 {
		fmt.Printf("backup_quiescence_last_lsn=%d\n", lease.LastLSN)
	}
	if lease.ReplayRecords != 0 {
		fmt.Printf("backup_quiescence_replay_records=%d\n", lease.ReplayRecords)
	}
	if lease.PendingRecords != 0 {
		fmt.Printf("backup_quiescence_pending_records=%d\n", lease.PendingRecords)
	}
}
