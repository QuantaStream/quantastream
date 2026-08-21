package admin

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

	if err := (&BackupCreateCmd{DataDir: dataDir, Target: backupDir, Quiesce: true}).Run(&Context{}); err != nil {
		t.Fatalf("BackupCreateCmd.Run returned error: %v", err)
	}
	manifest, err := core.ValidateLocalStorageBackup(backupDir)
	if err != nil {
		t.Fatalf("ValidateLocalStorageBackup returned error: %v", err)
	}
	if manifest.Mode != core.LocalStorageBackupModeQuiescent {
		t.Fatalf("manifest mode = %s, want quiescent", manifest.Mode)
	}
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
