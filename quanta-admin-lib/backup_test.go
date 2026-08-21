package admin

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBackupCommandsCreateValidateAndRestoreLocalSnapshot(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	writeAdminBackupTestFile(t, dataDir, "config/customer/schema.yaml", "name: customer\n")
	backupDir := filepath.Join(root, "backup")
	restoreDir := filepath.Join(root, "restore")
	ctx := &Context{}

	if err := (&BackupCreateCmd{DataDir: dataDir, Target: backupDir}).Run(ctx); err != nil {
		t.Fatalf("BackupCreateCmd.Run returned error: %v", err)
	}
	if err := (&BackupValidateCmd{Source: backupDir}).Run(ctx); err != nil {
		t.Fatalf("BackupValidateCmd.Run returned error: %v", err)
	}
	if err := (&BackupRestoreCmd{Source: backupDir, DataDir: restoreDir}).Run(ctx); err != nil {
		t.Fatalf("BackupRestoreCmd.Run returned error: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(restoreDir, "config/customer/schema.yaml"))
	if err != nil {
		t.Fatalf("read restored schema: %v", err)
	}
	if string(data) != "name: customer\n" {
		t.Fatalf("restored schema = %q", string(data))
	}
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
