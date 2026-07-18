package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
)

func TestBitmapShardManifestBuilderGroupsStandardAndBSIFiles(t *testing.T) {
	dataDir := t.TempDir()
	builder := newBitmapShardManifestBuilder(dataDir)
	shardTime := time.Date(1994, 1, 2, 0, 0, 0, 0, time.UTC)

	standardPath := writeManifestTestFile(t, dataDir, "bitmap/customer/is_active/0/1994-01-02T00")
	standardInfo := statManifestTestFile(t, standardPath)
	builder.addStandardFile(standardPath, standardInfo, "customer", "is_active", 0, shardTime)

	bsiEBMPath := writeManifestTestFile(t, dataDir, "bitmap/lineitem/l_quantity/bsi/1994-01-02T00/EBM")
	bsiSlicePath := writeManifestTestFile(t, dataDir, "bitmap/lineitem/l_quantity/bsi/1994-01-02T00/1")
	builder.addBSIFile(bsiSlicePath, statManifestTestFile(t, bsiSlicePath), "lineitem", "l_quantity", shardTime, 1)
	builder.addBSIFile(bsiEBMPath, statManifestTestFile(t, bsiEBMPath), "lineitem", "l_quantity", shardTime, 0)

	manifest := builder.manifest(time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), "test")
	if manifest.Version != bitmapShardManifestVersion {
		t.Fatalf("manifest version = %d, want %d", manifest.Version, bitmapShardManifestVersion)
	}
	if manifest.Stats.TotalEntries != 2 || manifest.Stats.StandardEntries != 1 || manifest.Stats.BSIEntries != 1 {
		t.Fatalf("unexpected manifest entry stats: %+v", manifest.Stats)
	}
	if manifest.Stats.TotalFiles != 3 || manifest.Stats.StandardFiles != 1 || manifest.Stats.BSIFiles != 2 {
		t.Fatalf("unexpected manifest file stats: %+v", manifest.Stats)
	}

	standard := manifest.Entries[0]
	if standard.Kind != bitmapShardKindStandard || standard.RowIDOrBits != 0 {
		t.Fatalf("expected row-zero standard entry first, got %+v", standard)
	}
	if standard.Files[0].RelativePath != "bitmap/customer/is_active/0/1994-01-02T00" {
		t.Fatalf("unexpected standard relative path: %s", standard.Files[0].RelativePath)
	}

	bsi := manifest.Entries[1]
	if bsi.Kind != bitmapShardKindBSI || bsi.RowIDOrBits != -1 {
		t.Fatalf("expected BSI entry, got %+v", bsi)
	}
	if len(bsi.Files) != 2 {
		t.Fatalf("expected 2 BSI files, got %d", len(bsi.Files))
	}
	if bsi.Files[0].Role != "existence" || bsi.Files[0].BitSlice != 0 {
		t.Fatalf("expected EBM/existence file first, got %+v", bsi.Files[0])
	}
	if bsi.Files[1].Role != "bit_slice" || bsi.Files[1].BitSlice != 1 {
		t.Fatalf("expected bit-slice file second, got %+v", bsi.Files[1])
	}
}

func TestBitmapShardManifestSaveAndLoad(t *testing.T) {
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
	}
	manifest := BitmapShardManifest{
		GeneratedAt: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC),
		Source:      "test",
		Entries: []BitmapShardManifestEntry{
			{
				Table:       "customer",
				Field:       "is_active",
				Kind:        bitmapShardKindStandard,
				RowIDOrBits: 0,
				Shard:       "1994-01-02T00",
				ShardTime:   time.Date(1994, 1, 2, 0, 0, 0, 0, time.UTC),
				Files: []BitmapShardManifestFile{
					{RelativePath: "bitmap/customer/is_active/0/1994-01-02T00", Role: "bitmap"},
				},
			},
		},
		Stats: BitmapShardManifestStats{
			TotalEntries:    1,
			StandardEntries: 1,
			TotalFiles:      1,
			StandardFiles:   1,
		},
	}

	if err := index.saveBitmapShardManifest(manifest); err != nil {
		t.Fatalf("saveBitmapShardManifest returned error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(index.dataDir, BitmapShardManifestFileName)); err != nil {
		t.Fatalf("expected manifest file to exist: %v", err)
	}

	loaded, err := index.loadBitmapShardManifest()
	if err != nil {
		t.Fatalf("loadBitmapShardManifest returned error: %v", err)
	}
	if loaded.Version != bitmapShardManifestVersion {
		t.Fatalf("loaded version = %d, want %d", loaded.Version, bitmapShardManifestVersion)
	}
	if loaded.Source != "test" || len(loaded.Entries) != 1 {
		t.Fatalf("unexpected loaded manifest: %+v", loaded)
	}
	if loaded.Entries[0].RowIDOrBits != 0 {
		t.Fatalf("row-zero entry was not preserved: %+v", loaded.Entries[0])
	}
}

func writeManifestTestFile(t *testing.T, dataDir, rel string) string {
	t.Helper()
	path := filepath.Join(dataDir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("create test file directory: %v", err)
	}
	if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	return path
}

func statManifestTestFile(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat test file: %v", err)
	}
	return info
}
