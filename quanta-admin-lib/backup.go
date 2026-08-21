package admin

import (
	"context"
	"fmt"

	"github.com/QuantaStream/quantastream/core"
)

// BackupCmd groups provider-neutral storage backup operations.
type BackupCmd struct {
	Create   BackupCreateCmd   `cmd:"" help:"Create a local filesystem storage backup."`
	Validate BackupValidateCmd `cmd:"" help:"Validate a local filesystem storage backup."`
	Restore  BackupRestoreCmd  `cmd:"" help:"Restore a local filesystem storage backup into an empty data directory."`
	Quiesce  BackupQuiesceCmd  `cmd:"" help:"Inspect or release local backup quiescence leases."`
}

type BackupCreateCmd struct {
	DataDir string `help:"QuantaStream data directory to snapshot." default:"data"`
	Target  string `help:"Backup target directory. Supports file:///path or a local path." required:""`
	Quiesce bool   `help:"Create a local storage write barrier during the snapshot." default:"false"`
	WALPath string `help:"Optional local WAL path to record in the backup checkpoint."`
}

type BackupValidateCmd struct {
	Source string `help:"Backup source directory. Supports file:///path or a local path." required:""`
}

type BackupRestoreCmd struct {
	Source  string `help:"Backup source directory. Supports file:///path or a local path." required:""`
	DataDir string `help:"Empty QuantaStream data directory to restore into." default:"data"`
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
	manifest, err := core.CreateLocalStorageBackup(context.Background(), core.CreateLocalStorageBackupRequest{
		DataDir: c.DataDir,
		Target:  c.Target,
		Quiesce: c.Quiesce,
		WALPath: c.WALPath,
		Owner:   "quanta-admin backup create",
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
