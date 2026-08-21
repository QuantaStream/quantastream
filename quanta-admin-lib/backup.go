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
}

type BackupCreateCmd struct {
	DataDir string `help:"QuantaStream data directory to snapshot." default:"data"`
	Target  string `help:"Backup target directory. Supports file:///path or a local path." required:""`
}

type BackupValidateCmd struct {
	Source string `help:"Backup source directory. Supports file:///path or a local path." required:""`
}

type BackupRestoreCmd struct {
	Source  string `help:"Backup source directory. Supports file:///path or a local path." required:""`
	DataDir string `help:"Empty QuantaStream data directory to restore into." default:"data"`
}

func (c *BackupCreateCmd) Run(ctx *Context) error {
	manifest, err := core.CreateLocalStorageBackup(context.Background(), core.CreateLocalStorageBackupRequest{
		DataDir: c.DataDir,
		Target:  c.Target,
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

func printBackupManifestSummary(manifest core.LocalStorageBackupManifest) {
	fmt.Printf("backup_format=%s\n", manifest.Format)
	fmt.Printf("backup_version=%d\n", manifest.Version)
	fmt.Printf("backup_mode=%s\n", manifest.Mode)
	fmt.Printf("backup_files=%d\n", manifest.FileCount)
	fmt.Printf("backup_directories=%d\n", manifest.DirectoryCount)
	fmt.Printf("backup_bytes=%d\n", manifest.ByteCount)
	fmt.Printf("backup_checkpoint_kind=%s\n", manifest.Checkpoint.Kind)
	fmt.Printf("backup_wal_included=%t\n", manifest.Checkpoint.WALIncluded)
}
