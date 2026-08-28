package admin

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
)

func TestBackupCommandsCreateValidateAndRestoreLocalSnapshot(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeAdminBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")
	backupDir := filepath.Join(root, "backup")
	restoreDir := filepath.Join(root, "restore")
	ctx := &Context{}

	createOutput, err := captureAdminBackupStdout(t, func() error {
		return (&BackupCreateCmd{DataDir: dataDir, Target: backupDir}).Run(ctx)
	})
	if err != nil {
		t.Fatalf("BackupCreateCmd.Run returned error: %v", err)
	}
	assertAdminBackupOutputContains(t, createOutput,
		"backup_created=",
		"backup_product=QuantaStream",
		"backup_product_short=QStream",
		"backup_product_version=",
		"backup_product_summary=QuantaStream",
	)
	validateOutput, err := captureAdminBackupStdout(t, func() error {
		return (&BackupValidateCmd{Source: backupDir}).Run(ctx)
	})
	if err != nil {
		t.Fatalf("BackupValidateCmd.Run returned error: %v", err)
	}
	assertAdminBackupOutputContains(t, validateOutput,
		"backup_valid=",
		"backup_product=QuantaStream",
		"backup_product_summary=QuantaStream",
	)
	restoreOutput, err := captureAdminBackupStdout(t, func() error {
		return (&BackupRestoreCmd{Source: backupDir, DataDir: restoreDir}).Run(ctx)
	})
	if err != nil {
		t.Fatalf("BackupRestoreCmd.Run returned error: %v", err)
	}
	assertAdminBackupOutputContains(t, restoreOutput,
		"backup_restored=",
		"backup_product=QuantaStream",
		"backup_product_summary=QuantaStream",
	)
	data, err := os.ReadFile(filepath.Join(restoreDir, "config/customer/schema.yaml"))
	if err != nil {
		t.Fatalf("read restored schema: %v", err)
	}
	if string(data) != "name: customer\n" {
		t.Fatalf("restored schema = %q", string(data))
	}
}

func TestBackupCreateCmdSupportsQuiescentSnapshot(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeAdminBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")
	backupDir := filepath.Join(root, "backup")

	output, err := captureAdminBackupStdout(t, func() error {
		return (&BackupCreateCmd{DataDir: dataDir, Target: backupDir, Quiesce: true}).Run(&Context{})
	})
	if err != nil {
		t.Fatalf("BackupCreateCmd.Run returned error: %v", err)
	}
	assertAdminBackupOutputContains(t, output,
		"backup_mode=quiescent-local-snapshot",
		"backup_live_source_requires_committed_state=true",
		"backup_live_source_flush_hint=commit_or_drain_live_engine_before_snapshot",
		"backup_live_source_snapshot_scope=durable_filesystem_state",
	)
	manifest, err := core.ValidateLocalStorageBackup(backupDir)
	if err != nil {
		t.Fatalf("ValidateLocalStorageBackup returned error: %v", err)
	}
	if manifest.Mode != core.LocalStorageBackupModeQuiescent {
		t.Fatalf("manifest mode = %s, want quiescent", manifest.Mode)
	}
}

func TestBackupCreateCmdEngineFlushRequiresQuiesce(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeAdminBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")
	backupDir := filepath.Join(root, "backup")

	_, err := captureAdminBackupStdout(t, func() error {
		return (&BackupCreateCmd{
			DataDir:     dataDir,
			Target:      backupDir,
			EngineFlush: "standard-native",
		}).Run(&Context{})
	})
	if err == nil || !strings.Contains(err.Error(), "--engine-flush requires --quiesce") {
		t.Fatalf("BackupCreateCmd.Run error = %v, want quiesce requirement", err)
	}
	if _, found, leaseErr := core.ObserveLocalStorageQuiescence(dataDir); leaseErr != nil || found {
		t.Fatalf("ObserveLocalStorageQuiescence found=%t err=%v, want absent", found, leaseErr)
	}
	if _, statErr := os.Stat(filepath.Join(backupDir, core.LocalStorageBackupManifestFileName)); !os.IsNotExist(statErr) {
		t.Fatalf("backup manifest stat err=%v, want not exist", statErr)
	}
}

func TestBackupEngineFlushSummaryClarifiesDistributedScope(t *testing.T) {
	output, err := captureAdminBackupStdout(t, func() error {
		printBackupEngineFlushSummary("distributed", "", time.Minute)
		printQuiescentBackupLiveSourceNotes("distributed")
		return nil
	})
	if err != nil {
		t.Fatalf("captureAdminBackupStdout returned error: %v", err)
	}
	assertAdminBackupOutputContains(t, output,
		"backup_engine_flush=distributed",
		"backup_engine_flush_distributed_scope=commit_only",
		"backup_engine_flush_distributed_backup_supported=false",
		"backup_engine_flush_distributed_backup_issue=https://github.com/QuantaStream/quantastream/issues/10",
		"backup_engine_flush_timeout=1m0s",
		"backup_live_source_flush_hint=distributed_commit_only_local_snapshot_not_cluster_backup",
		"backup_live_source_snapshot_scope=durable_filesystem_state",
	)
}

func TestBackupInspectCmdReadsManifestWithoutChecksumValidation(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeAdminBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")
	backupDir := filepath.Join(root, "backup")

	if err := (&BackupCreateCmd{DataDir: dataDir, Target: backupDir}).Run(&Context{}); err != nil {
		t.Fatalf("BackupCreateCmd.Run returned error: %v", err)
	}
	writeAdminBackupTestFile(t, filepath.Join(backupDir, core.LocalStorageBackupSnapshotDir), "config/customer/schema.yaml", "name: corrupt!\n")

	output, err := captureAdminBackupStdout(t, func() error {
		return (&BackupInspectCmd{Source: backupDir}).Run(&Context{})
	})
	if err != nil {
		t.Fatalf("BackupInspectCmd.Run returned error: %v", err)
	}
	assertAdminBackupOutputContains(t, output,
		"backup_inspected=",
		"backup_format=quantastream-local-storage-backup",
		"backup_product=QuantaStream",
		"backup_files=1",
	)
	if _, err := captureAdminBackupStdout(t, func() error {
		return (&BackupValidateCmd{Source: backupDir}).Run(&Context{})
	}); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("BackupValidateCmd.Run error = %v, want checksum mismatch", err)
	}
}

func TestBackupSmokeCmdRestoresValidatesAndRemovesTemporaryImage(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeAdminBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")
	backupDir := filepath.Join(root, "backup")
	tempParent := filepath.Join(root, "smoke-temp")

	if err := (&BackupCreateCmd{DataDir: dataDir, Target: backupDir}).Run(&Context{}); err != nil {
		t.Fatalf("BackupCreateCmd.Run returned error: %v", err)
	}
	output, err := captureAdminBackupStdout(t, func() error {
		return (&BackupSmokeCmd{Source: backupDir, TempDir: tempParent}).Run(&Context{})
	})
	if err != nil {
		t.Fatalf("BackupSmokeCmd.Run returned error: %v", err)
	}
	assertAdminBackupOutputContains(t, output,
		"backup_smoke_valid=",
		"backup_smoke_restore_dir=",
		"backup_smoke_restore_kept=false",
		"backup_product=QuantaStream",
		"backup_files=1",
	)
	restoreDir := adminBackupOutputValue(t, output, "backup_smoke_restore_dir=")
	if _, err := os.Stat(restoreDir); !os.IsNotExist(err) {
		t.Fatalf("smoke restore dir stat err=%v, want removed", err)
	}
}

func TestBackupSmokeCmdPlansRestoredWAL(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeAdminBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")
	walPath := filepath.Join(dataDir, "wal", "storage.wal")
	wal, err := core.OpenLocalWALWithOptions(walPath, core.LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions returned error: %v", err)
	}
	if _, _, err := wal.CommitBoundary(context.Background(), core.LocalWALRecord{
		OperationID: "commit-before-smoke",
		Kind:        core.LocalWALRecordKindCommit,
	}, func() error { return nil }); err != nil {
		t.Fatalf("CommitBoundary returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL returned error: %v", err)
	}
	backupDir := filepath.Join(root, "backup")
	if err := (&BackupCreateCmd{DataDir: dataDir, Target: backupDir, Quiesce: true, WALPath: walPath}).Run(&Context{}); err != nil {
		t.Fatalf("BackupCreateCmd.Run returned error: %v", err)
	}

	output, err := captureAdminBackupStdout(t, func() error {
		return (&BackupSmokeCmd{Source: backupDir, TempDir: filepath.Join(root, "smoke-temp")}).Run(&Context{})
	})
	if err != nil {
		t.Fatalf("BackupSmokeCmd.Run returned error: %v", err)
	}
	assertAdminBackupOutputContains(t, output,
		"backup_smoke_wal_plan_valid=true",
		"backup_wal_included=true",
		"wal_records=1",
		"wal_checkpoint_lsn=1",
		"wal_replay_records=0",
		"wal_pending_records=0",
		"wal_needs_replay=false",
	)
}

func TestBackupQuiesceCommandsStatusAndReleaseLease(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}
	lease, err := core.BeginLocalStorageQuiescence(context.Background(), core.BeginLocalStorageQuiescenceRequest{
		DataDir: dataDir,
		Owner:   "unit-test",
		Reason:  "stale lease smoke",
	})
	if err != nil {
		t.Fatalf("BeginLocalStorageQuiescence returned error: %v", err)
	}

	if err := (&BackupQuiesceStatusCmd{DataDir: dataDir}).Run(&Context{}); err != nil {
		t.Fatalf("BackupQuiesceStatusCmd.Run returned error: %v", err)
	}
	if err := (&BackupQuiesceReleaseCmd{DataDir: dataDir, LeaseID: lease.ID}).Run(&Context{}); err != nil {
		t.Fatalf("BackupQuiesceReleaseCmd.Run returned error: %v", err)
	}
	if _, found, err := core.ObserveLocalStorageQuiescence(dataDir); err != nil || found {
		t.Fatalf("ObserveLocalStorageQuiescence after release found=%t err=%v, want absent", found, err)
	}
}

func TestBackupQuiesceReleaseRequiresLeaseIDOrForce(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}
	if _, err := core.BeginLocalStorageQuiescence(context.Background(), core.BeginLocalStorageQuiescenceRequest{DataDir: dataDir}); err != nil {
		t.Fatalf("BeginLocalStorageQuiescence returned error: %v", err)
	}
	err := (&BackupQuiesceReleaseCmd{DataDir: dataDir}).Run(&Context{})
	if err == nil || !strings.Contains(err.Error(), "--lease-id or --force") {
		t.Fatalf("BackupQuiesceReleaseCmd.Run error = %v, want lease-id/force requirement", err)
	}
	if _, found, err := core.ObserveLocalStorageQuiescence(dataDir); err != nil || !found {
		t.Fatalf("ObserveLocalStorageQuiescence after rejected release found=%t err=%v, want active", found, err)
	}
}

func captureAdminBackupStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	orig := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}
	os.Stdout = writer
	defer func() { os.Stdout = orig }()
	runErr := fn()
	closeErr := writer.Close()
	os.Stdout = orig
	data, readErr := io.ReadAll(reader)
	if err := reader.Close(); err != nil && readErr == nil {
		readErr = err
	}
	if readErr != nil {
		t.Fatalf("read captured stdout: %v", readErr)
	}
	if closeErr != nil && runErr == nil {
		runErr = closeErr
	}
	return string(data), runErr
}

func assertAdminBackupOutputContains(t *testing.T, output string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(output, want) {
			t.Fatalf("backup command output missing %q:\n%s", want, output)
		}
	}
}

func adminBackupOutputValue(t *testing.T, output, prefix string) string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	t.Fatalf("backup command output missing value prefix %q:\n%s", prefix, output)
	return ""
}

func writeAdminBackupTestFile(t *testing.T, root, rel, body string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir %s: %v", rel, err)
	}
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}
