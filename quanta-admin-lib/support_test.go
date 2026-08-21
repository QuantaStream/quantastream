package admin

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/core"
)

func TestSupportBundleCmdCreatesDiagnosticArchive(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	configDir := filepath.Join(dataDir, "config")
	writeAdminBackupTestFile(t, configDir, "customer/schema.yaml", "name: customer\n")
	writeAdminBackupTestFile(t, configDir, "views/q3_order_line_base.yaml", "name: q3_order_line_base\n")
	writeAdminBackupTestFile(t, configDir, "CATALOG_OBJECTS", "customer table\nq3_order_line_base view\n")
	writeAdminBackupTestFile(t, configDir, "auth.yaml", "secret: do-not-include\n")
	logPath := filepath.Join(root, "qstream.log")
	if err := os.WriteFile(logPath, []byte("0123456789abcdef"), 0644); err != nil {
		t.Fatalf("write log: %v", err)
	}
	backupDir := filepath.Join(root, "backup")
	if _, err := core.CreateLocalStorageBackup(t.Context(), core.CreateLocalStorageBackupRequest{
		DataDir: dataDir,
		Target:  backupDir,
	}); err != nil {
		t.Fatalf("CreateLocalStorageBackup returned error: %v", err)
	}
	output := filepath.Join(root, "support.tar.gz")
	bundleOutput, err := captureAdminBackupStdout(t, func() error {
		return (&SupportBundleCmd{
			Output:            output,
			DataDir:           dataDir,
			BackupSource:      []string{backupDir},
			LogPath:           []string{logPath},
			MaxLogBytes:       6,
			SkipClusterStatus: true,
		}).Run(&Context{ConsulAddr: "127.0.0.1:8500", Port: 4000})
	})
	if err != nil {
		t.Fatalf("SupportBundleCmd.Run returned error: %v", err)
	}
	assertAdminBackupOutputContains(t, bundleOutput,
		"support_bundle_created="+output,
		"support_bundle_entries=",
	)
	entries := readSupportBundleTestArchive(t, output)
	for _, name := range []string{
		"README.txt",
		"metadata/version.txt",
		"metadata/runtime.txt",
		"config/summary.txt",
		"wal/skipped.txt",
		"backups/backup-001-manifest.json",
		"logs/qstream.log",
		"cluster/status-skipped.txt",
	} {
		if _, ok := entries[name]; !ok {
			t.Fatalf("support bundle missing %s; entries=%v", name, sortedSupportBundleTestNames(entries))
		}
	}
	configSummary := string(entries["config/summary.txt"])
	assertAdminBackupOutputContains(t, configSummary,
		"tables_schema_count=1",
		"views_schema_count=1",
		"auth_file=auth.yaml",
		"included=false",
	)
	if strings.Contains(configSummary, "do-not-include") {
		t.Fatalf("config summary leaked auth file contents:\n%s", configSummary)
	}
	logTail := string(entries["logs/qstream.log"])
	assertAdminBackupOutputContains(t, logTail,
		"[truncated to last 6 bytes",
		"abcdef",
	)
	manifest := string(entries["backups/backup-001-manifest.json"])
	assertAdminBackupOutputContains(t, manifest,
		`"format": "quantastream-local-storage-backup"`,
		`"name": "QuantaStream"`,
	)
}

func readSupportBundleTestArchive(t *testing.T, path string) map[string][]byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open support bundle: %v", err)
	}
	defer file.Close()
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("open gzip support bundle: %v", err)
	}
	defer gzipReader.Close()
	tarReader := tar.NewReader(gzipReader)
	entries := map[string][]byte{}
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read support bundle tar: %v", err)
		}
		body, err := io.ReadAll(tarReader)
		if err != nil {
			t.Fatalf("read support bundle entry %s: %v", header.Name, err)
		}
		entries[header.Name] = body
	}
	return entries
}

func sortedSupportBundleTestNames(entries map[string][]byte) []string {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
