package server

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/golang/protobuf/ptypes/empty"
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
	if len(bsi.Files) != 0 {
		t.Fatalf("expected compact BSI manifest to omit per-file entries, got %d", len(bsi.Files))
	}
	if bsi.BaseRelativePath != "bitmap/lineitem/l_quantity/bsi/1994-01-02T00" {
		t.Fatalf("unexpected BSI base relative path: %s", bsi.BaseRelativePath)
	}
	if bsi.FileCount != 2 || bsi.MaxBitSlice != 1 || !bsi.HasExistence {
		t.Fatalf("unexpected compact BSI metadata: %+v", bsi)
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

func TestBitmapShardManifestInvalidationIsIdempotent(t *testing.T) {
	index := newManifestPersistenceTestIndex(t)
	if err := index.saveBitmapShardManifest(BitmapShardManifest{Source: "test"}); err != nil {
		t.Fatalf("saveBitmapShardManifest returned error: %v", err)
	}

	if err := index.invalidateBitmapShardManifest("test"); err != nil {
		t.Fatalf("invalidateBitmapShardManifest returned error: %v", err)
	}
	if err := index.invalidateBitmapShardManifest("test"); err != nil {
		t.Fatalf("second invalidateBitmapShardManifest returned error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(index.dataDir, BitmapShardManifestFileName)); !os.IsNotExist(err) {
		t.Fatalf("expected manifest to be removed, stat err=%v", err)
	}
}

func TestBitmapPersistInvalidatesShardManifestWhenStandardBitmapWrites(t *testing.T) {
	index := newManifestPersistenceTestIndex(t)
	now := time.Unix(100, 0)
	index.bitmapCache = map[string]map[string]map[uint64]map[int64]*StandardBitmap{
		"customers": {
			"isActive": {
				0: {
					now.UnixNano(): {
						Bits:        roaring64.BitmapOf(1, 2),
						ModTime:     now,
						PersistTime: now.Add(-time.Second),
					},
				},
			},
		},
	}
	if err := index.saveBitmapShardManifest(BitmapShardManifest{Source: "test"}); err != nil {
		t.Fatalf("saveBitmapShardManifest returned error: %v", err)
	}

	_, writes, err := index.checkPersistBitmapCache(false)
	if err != nil {
		t.Fatalf("checkPersistBitmapCache returned error: %v", err)
	}
	if writes != 1 {
		t.Fatalf("writes = %d, want 1", writes)
	}
	if _, err := os.Stat(filepath.Join(index.dataDir, BitmapShardManifestFileName)); !os.IsNotExist(err) {
		t.Fatalf("expected manifest to be invalidated after standard persist, stat err=%v", err)
	}
}

func TestBitmapPersistLeavesShardManifestWhenStandardBitmapDoesNotWrite(t *testing.T) {
	index := newManifestPersistenceTestIndex(t)
	now := time.Unix(100, 0)
	index.bitmapCache = map[string]map[string]map[uint64]map[int64]*StandardBitmap{
		"customers": {
			"isActive": {
				0: {
					now.UnixNano(): {
						Bits:        roaring64.BitmapOf(1, 2),
						ModTime:     now,
						PersistTime: now,
					},
				},
			},
		},
	}
	if err := index.saveBitmapShardManifest(BitmapShardManifest{Source: "test"}); err != nil {
		t.Fatalf("saveBitmapShardManifest returned error: %v", err)
	}

	_, writes, err := index.checkPersistBitmapCache(false)
	if err != nil {
		t.Fatalf("checkPersistBitmapCache returned error: %v", err)
	}
	if writes != 0 {
		t.Fatalf("writes = %d, want 0", writes)
	}
	if _, err := os.Stat(filepath.Join(index.dataDir, BitmapShardManifestFileName)); err != nil {
		t.Fatalf("expected manifest to remain after clean skipped persist, stat err=%v", err)
	}
}

func TestBSIPersistInvalidatesShardManifestWhenBSIWrites(t *testing.T) {
	index := newManifestPersistenceTestIndex(t)
	now := time.Unix(100, 0)
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	index.bsiCache = map[string]map[string]map[int64]*BSIBitmap{
		"orders": {
			"o_orderkey": {
				now.UnixNano(): {
					BSI:         values,
					ModTime:     now,
					PersistTime: now.Add(-time.Second),
				},
			},
		},
	}
	if err := index.saveBitmapShardManifest(BitmapShardManifest{Source: "test"}); err != nil {
		t.Fatalf("saveBitmapShardManifest returned error: %v", err)
	}

	_, writes, err := index.checkPersistBSICache(false)
	if err != nil {
		t.Fatalf("checkPersistBSICache returned error: %v", err)
	}
	if writes != 1 {
		t.Fatalf("writes = %d, want 1", writes)
	}
	if _, err := os.Stat(filepath.Join(index.dataDir, BitmapShardManifestFileName)); !os.IsNotExist(err) {
		t.Fatalf("expected manifest to be invalidated after BSI persist, stat err=%v", err)
	}
}

func TestSaveBitmapShardManifestFromCacheRefreshesAfterDirtyWrites(t *testing.T) {
	index := newManifestPersistenceTestIndex(t)
	now := time.Unix(100, 0)
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	index.bitmapCache = map[string]map[string]map[uint64]map[int64]*StandardBitmap{
		"customers": {
			"isActive": {
				0: {
					now.UnixNano(): {
						Bits:        roaring64.BitmapOf(1, 2),
						ModTime:     now,
						PersistTime: now.Add(-time.Second),
					},
				},
			},
		},
	}
	index.bsiCache = map[string]map[string]map[int64]*BSIBitmap{
		"orders": {
			"o_orderkey": {
				now.UnixNano(): {
					BSI:         values,
					ModTime:     now,
					PersistTime: now.Add(-time.Second),
				},
			},
		},
	}

	_, bitmapWrites, err := index.checkPersistBitmapCache(false)
	if err != nil {
		t.Fatalf("checkPersistBitmapCache returned error: %v", err)
	}
	_, bsiWrites, err := index.checkPersistBSICache(false)
	if err != nil {
		t.Fatalf("checkPersistBSICache returned error: %v", err)
	}
	if bitmapWrites != 1 || bsiWrites != 1 {
		t.Fatalf("writes = bitmap:%d bsi:%d, want 1 and 1", bitmapWrites, bsiWrites)
	}
	if _, err := os.Stat(filepath.Join(index.dataDir, BitmapShardManifestFileName)); !os.IsNotExist(err) {
		t.Fatalf("expected low-level persistence to invalidate manifest before refresh, stat err=%v", err)
	}
	if err := index.saveBitmapShardManifestFromCache("test_refresh"); err != nil {
		t.Fatalf("saveBitmapShardManifestFromCache returned error: %v", err)
	}
	manifest, err := index.loadBitmapShardManifest()
	if err != nil {
		t.Fatalf("loadBitmapShardManifest returned error: %v", err)
	}
	if manifest.Source != "test_refresh" {
		t.Fatalf("manifest source = %q, want test_refresh", manifest.Source)
	}
	if manifest.Stats.StandardEntries != 1 || manifest.Stats.BSIEntries != 1 {
		t.Fatalf("unexpected manifest stats after persist refresh: %+v", manifest.Stats)
	}
	observation := index.observeBitmapShardManifest(manifest)
	if observation.Status != "ok" {
		t.Fatalf("refreshed manifest observation status = %s detail=%s, want ok", observation.Status, observation.Detail)
	}
}

func TestCommitSavepointRefreshesManifestForColdStandardBundleLoad(t *testing.T) {
	index := newManifestLoadTestIndex(t)
	shardTime := time.Unix(0, 0)
	now := time.Unix(100, 0)
	index.bitmapCache = map[string]map[string]map[uint64]map[int64]*StandardBitmap{
		"customer": {
			"c_mktsegment": {
				10: {
					shardTime.UnixNano(): {
						Bits:        roaring64.BitmapOf(1, 2, 3),
						ModTime:     now,
						PersistTime: now,
					},
				},
				20: {
					shardTime.UnixNano(): {
						Bits:        roaring64.BitmapOf(7, 8),
						ModTime:     now,
						PersistTime: now,
					},
				},
			},
		},
	}

	if _, err := index.Commit(context.Background(), &empty.Empty{}); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	manifest, observation := index.loadAndObserveBitmapShardManifest(nil)
	if observation.Status != "ok" {
		t.Fatalf("commit manifest status = %s detail=%s, want ok", observation.Status, observation.Detail)
	}
	if manifest.Source != "commit" {
		t.Fatalf("manifest source = %q, want commit", manifest.Source)
	}
	if manifest.Stats.StandardEntries != 1 || manifest.Stats.StandardFiles != 1 {
		t.Fatalf("manifest standard stats = %+v, want one bundled standard shard", manifest.Stats)
	}

	cold := newManifestLoadTestIndex(t)
	cold.Node.dataDir = index.dataDir
	if err := cold.readBitmapFiles(cold.fragQueue); err != nil {
		t.Fatalf("cold readBitmapFiles returned error: %v", err)
	}
	loadedLeft := cold.bitmapCache["customer"]["c_mktsegment"][10][shardTime.UnixNano()]
	if loadedLeft == nil {
		t.Fatal("expected cold restart to materialize row 10 from committed standard bundle")
	}
	if got := loadedLeft.Bits.GetCardinality(); got != 3 {
		t.Fatalf("cold row 10 cardinality = %d, want 3", got)
	}
	loadedRight := cold.bitmapCache["customer"]["c_mktsegment"][20][shardTime.UnixNano()]
	if loadedRight == nil {
		t.Fatal("expected cold restart to materialize row 20 from committed standard bundle")
	}
	if got := loadedRight.Bits.GetCardinality(); got != 2 {
		t.Fatalf("cold row 20 cardinality = %d, want 2", got)
	}
}

func TestCommitReusesCleanManifestSavepoint(t *testing.T) {
	index := newManifestLoadTestIndex(t)
	shardTime := time.Unix(0, 0)
	now := time.Unix(100, 0)
	index.bitmapCache = map[string]map[string]map[uint64]map[int64]*StandardBitmap{
		"customer": {
			"c_mktsegment": {
				10: {
					shardTime.UnixNano(): {
						Bits:        roaring64.BitmapOf(1, 2, 3),
						ModTime:     now,
						PersistTime: now,
					},
				},
			},
		},
	}

	if _, err := index.Commit(context.Background(), &empty.Empty{}); err != nil {
		t.Fatalf("first Commit returned error: %v", err)
	}
	before, err := index.loadBitmapShardManifest()
	if err != nil {
		t.Fatalf("loadBitmapShardManifest before second commit returned error: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := index.Commit(context.Background(), &empty.Empty{}); err != nil {
		t.Fatalf("second Commit returned error: %v", err)
	}
	after, err := index.loadBitmapShardManifest()
	if err != nil {
		t.Fatalf("loadBitmapShardManifest after second commit returned error: %v", err)
	}
	if !after.GeneratedAt.Equal(before.GeneratedAt) {
		t.Fatalf("clean second commit rewrote manifest generated_at: before=%s after=%s", before.GeneratedAt, after.GeneratedAt)
	}
	if after.Source != before.Source {
		t.Fatalf("clean second commit changed manifest source: before=%q after=%q", before.Source, after.Source)
	}
}

func TestObserveBitmapShardManifestReportsMissing(t *testing.T) {
	index := newManifestPersistenceTestIndex(t)
	scan := BitmapShardManifest{
		Stats: BitmapShardManifestStats{TotalEntries: 1, TotalFiles: 1},
	}

	observation := index.observeBitmapShardManifest(scan)
	if observation.Status != "missing" {
		t.Fatalf("status = %s, want missing", observation.Status)
	}
	if observation.ScanEntries != 1 || observation.ScanFiles != 1 {
		t.Fatalf("unexpected scan stats in observation: %+v", observation)
	}
}

func TestObserveBitmapShardManifestReportsOK(t *testing.T) {
	index, manifest := newObservedManifestTestIndex(t)
	if err := index.saveBitmapShardManifest(manifest); err != nil {
		t.Fatalf("saveBitmapShardManifest returned error: %v", err)
	}

	observation := index.observeBitmapShardManifest(manifest)
	if observation.Status != "ok" {
		t.Fatalf("status = %s detail=%s, want ok", observation.Status, observation.Detail)
	}
	if observation.ManifestEntries != 1 || observation.ManifestFiles != 1 {
		t.Fatalf("unexpected manifest stats in observation: %+v", observation)
	}
}

func TestObserveBitmapShardManifestReportsMismatch(t *testing.T) {
	index, manifest := newObservedManifestTestIndex(t)
	if err := index.saveBitmapShardManifest(manifest); err != nil {
		t.Fatalf("saveBitmapShardManifest returned error: %v", err)
	}
	scan := manifest
	scan.Stats.TotalFiles = 2

	observation := index.observeBitmapShardManifest(scan)
	if observation.Status != "mismatch" {
		t.Fatalf("status = %s detail=%s, want mismatch", observation.Status, observation.Detail)
	}
}

func TestObserveBitmapShardManifestReportsStaleWhenFileMissing(t *testing.T) {
	index, manifest := newObservedManifestTestIndex(t)
	if err := index.saveBitmapShardManifest(manifest); err != nil {
		t.Fatalf("saveBitmapShardManifest returned error: %v", err)
	}
	if err := os.Remove(filepath.Join(index.dataDir, filepath.FromSlash(manifest.Entries[0].Files[0].RelativePath))); err != nil {
		t.Fatalf("remove manifest file target: %v", err)
	}

	observation := index.observeBitmapShardManifest(manifest)
	if observation.Status != "stale" {
		t.Fatalf("status = %s detail=%s, want stale", observation.Status, observation.Detail)
	}
	if observation.MissingFileCount != 1 {
		t.Fatalf("missing files = %d, want 1", observation.MissingFileCount)
	}
}

func TestObserveBitmapShardManifestReportsInvalidStats(t *testing.T) {
	index, manifest := newObservedManifestTestIndex(t)
	manifest.Stats.TotalEntries = 2
	if err := index.saveBitmapShardManifest(manifest); err != nil {
		t.Fatalf("saveBitmapShardManifest returned error: %v", err)
	}

	observation := index.observeBitmapShardManifest(manifest)
	if observation.Status != "invalid" {
		t.Fatalf("status = %s detail=%s, want invalid", observation.Status, observation.Detail)
	}
}

func TestUseBitmapShardManifestEnabled(t *testing.T) {
	t.Setenv("QUANTASTREAM_USE_SHARD_MANIFEST", "")
	if !useBitmapShardManifestEnabled() {
		t.Fatal("expected empty setting to enable manifest startup by default")
	}
	t.Setenv("QUANTASTREAM_USE_SHARD_MANIFEST", "true")
	if !useBitmapShardManifestEnabled() {
		t.Fatal("expected true to enable manifest startup")
	}
	t.Setenv("QUANTASTREAM_USE_SHARD_MANIFEST", "0")
	if useBitmapShardManifestEnabled() {
		t.Fatal("expected 0 to disable manifest startup")
	}
}

func TestReadBitmapFilesFromManifestLoadsStandardBitmap(t *testing.T) {
	index := newManifestLoadTestIndex(t)
	shardTime := time.Date(1994, 1, 2, 0, 0, 0, 0, time.UTC)
	bits := roaring64.BitmapOf(1, 2, 3)
	data, err := bits.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal bitmap: %v", err)
	}
	target := writeManifestTestFileBytes(t, index.dataDir, "bitmap/customer/c_mktsegment/0/1994-01-02T00", data)
	info := statManifestTestFile(t, target)
	builder := newBitmapShardManifestBuilder(index.dataDir)
	builder.addStandardFile(target, info, "customer", "c_mktsegment", 0, shardTime)
	manifest := builder.manifest(time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), "test")
	observation := BitmapShardManifestObservation{
		Status:          "ok",
		ManifestEntries: manifest.Stats.TotalEntries,
		ManifestFiles:   manifest.Stats.TotalFiles,
	}

	if err := index.readBitmapFilesFromManifest(manifest, observation, index.fragQueue, time.Now()); err != nil {
		t.Fatalf("readBitmapFilesFromManifest returned error: %v", err)
	}
	loaded := index.bitmapCache["customer"]["c_mktsegment"][0][shardTime.UnixNano()]
	if loaded == nil {
		t.Fatal("expected standard bitmap to load from manifest")
	}
	if got := loaded.Bits.GetCardinality(); got != 3 {
		t.Fatalf("loaded cardinality = %d, want 3", got)
	}
}

func TestReadBitmapFilesFromManifestLoadsBundledStandardBitmap(t *testing.T) {
	index := newManifestLoadTestIndex(t)
	shardTime := time.Date(1994, 1, 2, 0, 0, 0, 0, time.UTC)
	left := roaring64.BitmapOf(1, 2, 3)
	right := roaring64.BitmapOf(7, 8)
	leftData, err := left.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal left bitmap: %v", err)
	}
	rightData, err := right.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal right bitmap: %v", err)
	}
	bundle, err := encodeStandardBitmapBundle([]standardBitmapBundleEntry{
		{RowID: 10, Data: leftData},
		{RowID: 20, Data: rightData},
	})
	if err != nil {
		t.Fatalf("encodeStandardBitmapBundle returned error: %v", err)
	}
	target := writeManifestTestFileBytes(t, index.dataDir, filepath.ToSlash(filepath.Join("bitmap", "customer", "c_mktsegment", standardBundleLeafDir, "1994-01-02T00", standardBundleFileName)), bundle)
	builder := newBitmapShardManifestBuilder(index.dataDir)
	builder.addStandardBundleFile(target, statManifestTestFile(t, target), "customer", "c_mktsegment", shardTime)
	manifest := builder.manifest(time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), "test")
	observation := BitmapShardManifestObservation{
		Status:          "ok",
		ManifestEntries: manifest.Stats.TotalEntries,
		ManifestFiles:   manifest.Stats.TotalFiles,
	}

	if err := index.readBitmapFilesFromManifest(manifest, observation, index.fragQueue, time.Now()); err != nil {
		t.Fatalf("readBitmapFilesFromManifest returned error: %v", err)
	}
	loadedLeft := index.bitmapCache["customer"]["c_mktsegment"][10][shardTime.UnixNano()]
	if loadedLeft == nil {
		t.Fatal("expected bundled standard bitmap row 10 to load from manifest")
	}
	if got := loadedLeft.Bits.GetCardinality(); got != 3 {
		t.Fatalf("loaded row 10 cardinality = %d, want 3", got)
	}
	loadedRight := index.bitmapCache["customer"]["c_mktsegment"][20][shardTime.UnixNano()]
	if loadedRight == nil {
		t.Fatal("expected bundled standard bitmap row 20 to load from manifest")
	}
	if got := loadedRight.Bits.GetCardinality(); got != 2 {
		t.Fatalf("loaded row 20 cardinality = %d, want 2", got)
	}
}

func TestReadBitmapFilesFromManifestLoadsBSI(t *testing.T) {
	index := newManifestLoadTestIndex(t)
	shardTime := time.Date(1994, 1, 2, 0, 0, 0, 0, time.UTC)
	bsi := roaring64.NewDefaultBSI()
	bsi.SetValue(1, 10)
	bsi.SetValue(2, 20)
	data, err := bsi.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal BSI: %v", err)
	}
	builder := newBitmapShardManifestBuilder(index.dataDir)
	for i, slice := range data {
		name := "EBM"
		if i > 0 {
			name = strconv.Itoa(i)
		}
		target := writeManifestTestFileBytes(t, index.dataDir, filepath.ToSlash(filepath.Join("bitmap", "lineitem", "l_quantity", "bsi", "1994-01-02T00", name)), slice)
		builder.addBSIFile(target, statManifestTestFile(t, target), "lineitem", "l_quantity", shardTime, i)
	}
	manifest := builder.manifest(time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), "test")
	observation := BitmapShardManifestObservation{
		Status:          "ok",
		ManifestEntries: manifest.Stats.TotalEntries,
		ManifestFiles:   manifest.Stats.TotalFiles,
	}

	if err := index.readBitmapFilesFromManifest(manifest, observation, index.fragQueue, time.Now()); err != nil {
		t.Fatalf("readBitmapFilesFromManifest returned error: %v", err)
	}
	loaded := index.bsiCache["lineitem"]["l_quantity"][shardTime.UnixNano()]
	if loaded == nil {
		t.Fatal("expected BSI to load from manifest")
	}
	if got := loaded.GetExistenceBitmap().GetCardinality(); got != 2 {
		t.Fatalf("loaded BSI existence cardinality = %d, want 2", got)
	}
}

func TestReadBitmapFilesFromManifestLoadsBundledBSI(t *testing.T) {
	index := newManifestLoadTestIndex(t)
	shardTime := time.Date(1994, 1, 2, 0, 0, 0, 0, time.UTC)
	bsi := roaring64.NewDefaultBSI()
	bsi.SetValue(1, 10)
	bsi.SetValue(2, 20)
	data, err := bsi.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal BSI: %v", err)
	}
	bundle, err := encodeBSIBundle(data)
	if err != nil {
		t.Fatalf("encodeBSIBundle returned error: %v", err)
	}
	target := writeManifestTestFileBytes(t, index.dataDir, filepath.ToSlash(filepath.Join("bitmap", "lineitem", "l_quantity", "bsi", "1994-01-02T00", bsiBundleFileName)), bundle)
	builder := newBitmapShardManifestBuilder(index.dataDir)
	builder.addBSIBundleFile(target, statManifestTestFile(t, target), "lineitem", "l_quantity", shardTime)
	manifest := builder.manifest(time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), "test")
	observation := BitmapShardManifestObservation{
		Status:          "ok",
		ManifestEntries: manifest.Stats.TotalEntries,
		ManifestFiles:   manifest.Stats.TotalFiles,
	}

	if err := index.readBitmapFilesFromManifest(manifest, observation, index.fragQueue, time.Now()); err != nil {
		t.Fatalf("readBitmapFilesFromManifest returned error: %v", err)
	}
	loaded := index.bsiCache["lineitem"]["l_quantity"][shardTime.UnixNano()]
	if loaded == nil {
		t.Fatal("expected bundled BSI to load from manifest")
	}
	if got := loaded.GetExistenceBitmap().GetCardinality(); got != 2 {
		t.Fatalf("loaded bundled BSI existence cardinality = %d, want 2", got)
	}
}

func newObservedManifestTestIndex(t *testing.T) (*BitmapIndex, BitmapShardManifest) {
	t.Helper()
	index := newManifestPersistenceTestIndex(t)
	shardTime := time.Date(1994, 1, 2, 0, 0, 0, 0, time.UTC)
	target := writeManifestTestFile(t, index.dataDir, "bitmap/customer/is_active/0/1994-01-02T00")
	info := statManifestTestFile(t, target)
	builder := newBitmapShardManifestBuilder(index.dataDir)
	builder.addStandardFile(target, info, "customer", "is_active", 0, shardTime)
	return index, builder.manifest(time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), "test")
}

func newManifestLoadTestIndex(t *testing.T) *BitmapIndex {
	t.Helper()
	table, err := shared.LoadSchema("../tpc-h-benchmark/config", "customer", nil)
	if err != nil {
		t.Fatalf("load customer schema: %v", err)
	}
	lineitem, err := shared.LoadSchema("../tpc-h-benchmark/config", "lineitem", nil)
	if err != nil {
		t.Fatalf("load lineitem schema: %v", err)
	}
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("manifest-load-test"),
			dataDir: t.TempDir(),
		},
		bitmapCache: make(map[string]map[string]map[uint64]map[int64]*StandardBitmap),
		bsiCache:    make(map[string]map[string]map[int64]*BSIBitmap),
		seedCache:   make(map[string]*SeedBitmap),
		tableCache: map[string]*shared.BasicTable{
			"customer": table,
			"lineitem": lineitem,
		},
		fragQueue: make(chan *BitmapFragment, 16),
		workers:   []*WorkerThread{NewWorkerThread(0)},
	}
	go index.batchProcessLoop(index.workers[0])
	return index
}

func newManifestPersistenceTestIndex(t *testing.T) *BitmapIndex {
	t.Helper()
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
	}
	index.ServicePort = 1
	return index
}

func writeManifestTestFile(t *testing.T, dataDir, rel string) string {
	t.Helper()
	return writeManifestTestFileBytes(t, dataDir, rel, []byte("test"))
}

func writeManifestTestFileBytes(t *testing.T, dataDir, rel string, data []byte) string {
	t.Helper()
	path := filepath.Join(dataDir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("create test file directory: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
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
