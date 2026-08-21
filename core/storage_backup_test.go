package core

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCreateValidateAndRestoreLocalStorageBackup(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")
	writeBackupTestFile(t, dataDir, "bitmap/customer/c_custkey.IntBSI/0", "bitmap-slice")
	writeBackupTestFile(t, dataDir, "index/customer/c_name.StringEnum/main.pix", "kv-index")
	if err := os.MkdirAll(filepath.Join(dataDir, "empty-dir"), 0755); err != nil {
		t.Fatalf("mkdir empty-dir: %v", err)
	}

	backupDir := filepath.Join(root, "backup")
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	manifest, err := CreateLocalStorageBackup(context.Background(), CreateLocalStorageBackupRequest{
		DataDir: dataDir,
		Target:  "file://" + backupDir,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("CreateLocalStorageBackup returned error: %v", err)
	}
	if manifest.Version != LocalStorageBackupVersion || manifest.Format != LocalStorageBackupFormat {
		t.Fatalf("manifest identity = %d/%s", manifest.Version, manifest.Format)
	}
	if manifest.Mode != LocalStorageBackupModeOffline {
		t.Fatalf("manifest mode = %s", manifest.Mode)
	}
	if manifest.Product.Name != "QuantaStream" || manifest.Product.ShortName != "QStream" {
		t.Fatalf("manifest product = %+v, want QuantaStream/QStream", manifest.Product)
	}
	if manifest.Product.Version == "" || manifest.Product.Summary == "" {
		t.Fatalf("manifest product missing version summary: %+v", manifest.Product)
	}
	if manifest.FileCount != 3 || manifest.DirectoryCount == 0 {
		t.Fatalf("manifest counts files=%d dirs=%d", manifest.FileCount, manifest.DirectoryCount)
	}
	if manifest.ByteCount == 0 {
		t.Fatalf("manifest byte count should be non-zero")
	}
	if _, err := os.Stat(filepath.Join(backupDir, LocalStorageBackupManifestFileName)); err != nil {
		t.Fatalf("backup manifest not written: %v", err)
	}

	validated, err := ValidateLocalStorageBackup("file://" + backupDir)
	if err != nil {
		t.Fatalf("ValidateLocalStorageBackup returned error: %v", err)
	}
	if validated.FileCount != manifest.FileCount {
		t.Fatalf("validated file count = %d, want %d", validated.FileCount, manifest.FileCount)
	}
	if validated.Product.Summary != manifest.Product.Summary {
		t.Fatalf("validated product summary = %q, want %q", validated.Product.Summary, manifest.Product.Summary)
	}

	restoreDir := filepath.Join(root, "restore")
	restored, err := RestoreLocalStorageBackup(context.Background(), RestoreLocalStorageBackupRequest{
		Source:  "file://" + backupDir,
		DataDir: restoreDir,
	})
	if err != nil {
		t.Fatalf("RestoreLocalStorageBackup returned error: %v", err)
	}
	if restored.FileCount != manifest.FileCount {
		t.Fatalf("restored file count = %d, want %d", restored.FileCount, manifest.FileCount)
	}
	assertBackupTestFile(t, restoreDir, "config/customer/schema.yaml", "name: customer\n")
	assertBackupTestFile(t, restoreDir, "bitmap/customer/c_custkey.IntBSI/0", "bitmap-slice")
	assertBackupTestFile(t, restoreDir, "index/customer/c_name.StringEnum/main.pix", "kv-index")
	if info, err := os.Stat(filepath.Join(restoreDir, "empty-dir")); err != nil || !info.IsDir() {
		t.Fatalf("empty dir not restored info=%v err=%v", info, err)
	}
}

func TestValidateLocalStorageBackupRejectsCorruptFile(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")
	backupDir := filepath.Join(root, "backup")
	if _, err := CreateLocalStorageBackup(context.Background(), CreateLocalStorageBackupRequest{
		DataDir: dataDir,
		Target:  backupDir,
	}); err != nil {
		t.Fatalf("CreateLocalStorageBackup returned error: %v", err)
	}
	writeBackupTestFile(t, filepath.Join(backupDir, LocalStorageBackupSnapshotDir), "config/customer/schema.yaml", "name: CUSTOMER\n")

	_, err := ValidateLocalStorageBackup(backupDir)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("ValidateLocalStorageBackup error = %v, want checksum mismatch", err)
	}
}

func TestValidateLocalStorageBackupRejectsUnmanifestedSnapshotEntry(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")
	backupDir := filepath.Join(root, "backup")
	if _, err := CreateLocalStorageBackup(context.Background(), CreateLocalStorageBackupRequest{
		DataDir: dataDir,
		Target:  backupDir,
	}); err != nil {
		t.Fatalf("CreateLocalStorageBackup returned error: %v", err)
	}
	writeBackupTestFile(t, filepath.Join(backupDir, LocalStorageBackupSnapshotDir), "config/customer/untracked.yaml", "name: hidden\n")

	_, err := ValidateLocalStorageBackup(backupDir)
	if err == nil || !strings.Contains(err.Error(), "unmanifested entry") {
		t.Fatalf("ValidateLocalStorageBackup error = %v, want unmanifested entry", err)
	}
}

func TestCreateQuiescentLocalStorageBackupRecordsWALCheckpointAndRemovesLease(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")

	walPath := filepath.Join(dataDir, "wal", "standard.wal")
	wal, err := OpenLocalWALWithOptions(walPath, LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions returned error: %v", err)
	}
	record, checkpoint, err := wal.CommitBoundary(context.Background(), LocalWALRecord{
		OperationID: "commit-before-backup",
		Kind:        LocalWALRecordKindCommit,
	}, func() error { return nil })
	if err != nil {
		t.Fatalf("CommitBoundary returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL returned error: %v", err)
	}

	backupDir := filepath.Join(root, "backup")
	manifest, err := CreateLocalStorageBackup(context.Background(), CreateLocalStorageBackupRequest{
		DataDir: dataDir,
		Target:  backupDir,
		Quiesce: true,
		WALPath: walPath,
		Owner:   "unit-test",
		Reason:  "snapshot",
	})
	if err != nil {
		t.Fatalf("CreateLocalStorageBackup returned error: %v", err)
	}
	if manifest.Mode != LocalStorageBackupModeQuiescent {
		t.Fatalf("manifest mode = %s, want %s", manifest.Mode, LocalStorageBackupModeQuiescent)
	}
	if manifest.Checkpoint.Kind != "quiescent" || !manifest.Checkpoint.WALIncluded {
		t.Fatalf("checkpoint kind/included = %s/%t, want quiescent/true", manifest.Checkpoint.Kind, manifest.Checkpoint.WALIncluded)
	}
	if manifest.Checkpoint.LSN != checkpoint.LastCommittedLSN || manifest.Checkpoint.WALLastLSN != record.LSN {
		t.Fatalf("checkpoint LSN/last = %d/%d, want %d/%d", manifest.Checkpoint.LSN, manifest.Checkpoint.WALLastLSN, checkpoint.LastCommittedLSN, record.LSN)
	}
	if manifest.Checkpoint.WALPath != walPath || manifest.Checkpoint.WALCheckpointPath == "" {
		t.Fatalf("checkpoint WAL path/checkpoint = %q/%q", manifest.Checkpoint.WALPath, manifest.Checkpoint.WALCheckpointPath)
	}
	if _, found, err := ObserveLocalStorageQuiescence(dataDir); err != nil || found {
		t.Fatalf("quiescence lease after backup found=%t err=%v, want removed", found, err)
	}
	if _, err := os.Stat(filepath.Join(backupDir, LocalStorageBackupSnapshotDir, LocalStorageQuiescenceDir, LocalStorageQuiescenceFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snapshot carried quiescence lease err=%v, want not exist", err)
	}
	if _, err := ValidateLocalStorageBackup(backupDir); err != nil {
		t.Fatalf("ValidateLocalStorageBackup returned error: %v", err)
	}
}

func TestCreateQuiescentLocalStorageBackupRejectsExternalWALPath(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")

	walPath := filepath.Join(root, "external", "standard.wal")
	wal, err := OpenLocalWALWithOptions(walPath, LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions returned error: %v", err)
	}
	if _, _, err := wal.CommitBoundary(context.Background(), LocalWALRecord{
		OperationID: "commit-outside-data-dir",
		Kind:        LocalWALRecordKindCommit,
	}, func() error { return nil }); err != nil {
		t.Fatalf("CommitBoundary returned error: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL returned error: %v", err)
	}

	backupDir := filepath.Join(root, "backup")
	_, err = CreateLocalStorageBackup(context.Background(), CreateLocalStorageBackupRequest{
		DataDir: dataDir,
		Target:  backupDir,
		Quiesce: true,
		WALPath: walPath,
	})
	if err == nil || !strings.Contains(err.Error(), "outside data directory") {
		t.Fatalf("CreateLocalStorageBackup error = %v, want outside data directory", err)
	}
	if _, found, err := ObserveLocalStorageQuiescence(dataDir); err != nil || found {
		t.Fatalf("quiescence lease after rejected backup found=%t err=%v, want absent", found, err)
	}
	if _, err := os.Stat(backupDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup dir after rejected backup err=%v, want not exist", err)
	}
}

func TestLocalStorageQuiescenceRejectsSecondLeaseAndMutations(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}
	lease, err := BeginLocalStorageQuiescence(context.Background(), BeginLocalStorageQuiescenceRequest{
		DataDir: dataDir,
		Owner:   "unit-test",
		Reason:  "first lease",
	})
	if err != nil {
		t.Fatalf("BeginLocalStorageQuiescence returned error: %v", err)
	}
	if _, err := BeginLocalStorageQuiescence(context.Background(), BeginLocalStorageQuiescenceRequest{DataDir: dataDir}); err == nil || !strings.Contains(err.Error(), "already quiesced") {
		t.Fatalf("second BeginLocalStorageQuiescence error = %v, want already quiesced", err)
	}
	if err := RejectLocalStorageMutationIfQuiesced(dataDir, "insert"); err == nil || !strings.Contains(err.Error(), "blocked while local storage is quiesced") {
		t.Fatalf("RejectLocalStorageMutationIfQuiesced error = %v, want blocked", err)
	}
	if err := EndLocalStorageQuiescence(dataDir, lease.ID); err != nil {
		t.Fatalf("EndLocalStorageQuiescence returned error: %v", err)
	}
	if err := RejectLocalStorageMutationIfQuiesced(dataDir, "insert"); err != nil {
		t.Fatalf("RejectLocalStorageMutationIfQuiesced after end = %v, want nil", err)
	}
}

func TestRestoreLocalStorageBackupRejectsNonEmptyTarget(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")
	backupDir := filepath.Join(root, "backup")
	if _, err := CreateLocalStorageBackup(context.Background(), CreateLocalStorageBackupRequest{
		DataDir: dataDir,
		Target:  backupDir,
	}); err != nil {
		t.Fatalf("CreateLocalStorageBackup returned error: %v", err)
	}
	restoreDir := filepath.Join(root, "restore")
	writeBackupTestFile(t, restoreDir, "existing", "do-not-overwrite")

	_, err := RestoreLocalStorageBackup(context.Background(), RestoreLocalStorageBackupRequest{
		Source:  backupDir,
		DataDir: restoreDir,
	})
	if err == nil || !strings.Contains(err.Error(), "must be empty") {
		t.Fatalf("RestoreLocalStorageBackup error = %v, want non-empty target rejection", err)
	}
	assertBackupTestFile(t, restoreDir, "existing", "do-not-overwrite")
}

func TestCreateLocalStorageBackupRejectsTargetInsideSource(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")

	_, err := CreateLocalStorageBackup(context.Background(), CreateLocalStorageBackupRequest{
		DataDir: dataDir,
		Target:  filepath.Join(dataDir, "backup"),
	})
	if err == nil || !strings.Contains(err.Error(), "must not be inside source") {
		t.Fatalf("CreateLocalStorageBackup error = %v, want nested target rejection", err)
	}
}

func TestRestoreLocalStorageBackupRejectsEscapingManifestPath(t *testing.T) {
	root := t.TempDir()
	backupDir := filepath.Join(root, "backup")
	if err := os.MkdirAll(filepath.Join(backupDir, LocalStorageBackupSnapshotDir), 0755); err != nil {
		t.Fatalf("mkdir backup: %v", err)
	}
	manifest := `{
  "version": 1,
  "format": "quantastream-local-storage-backup",
  "mode": "offline-local-snapshot",
  "snapshot_dir": "snapshot",
  "entries": [{"path": "../escape", "type": "file", "mode": 420, "size": 1, "sha256": "x"}]
}`
	if err := os.WriteFile(filepath.Join(backupDir, LocalStorageBackupManifestFileName), []byte(manifest), 0644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	_, err := RestoreLocalStorageBackup(context.Background(), RestoreLocalStorageBackupRequest{
		Source:  backupDir,
		DataDir: filepath.Join(root, "restore"),
	})
	if err == nil || !strings.Contains(err.Error(), "escapes snapshot") {
		t.Fatalf("RestoreLocalStorageBackup error = %v, want escaping path rejection", err)
	}
}

func writeBackupTestFile(t *testing.T, root, rel, body string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir %s: %v", rel, err)
	}
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

func assertBackupTestFile(t *testing.T, root, rel, want string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	if string(data) != want {
		t.Fatalf("%s = %q, want %q", rel, string(data), want)
	}
}
