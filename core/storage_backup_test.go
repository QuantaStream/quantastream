package core

import (
	"context"
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
