package server

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/golang/protobuf/ptypes/empty"
)

func TestIsBSIBitmapPathUsesPathShape(t *testing.T) {
	if !isBSIBitmapPath([]string{"customers_qa", "cust_id", "bsi", "default", "1"}) {
		t.Fatal("expected bsi path to be classified as BSI")
	}

	if isBSIBitmapPath([]string{"customers_qa", "cust_id", "0", "1970-01-01T00"}) {
		t.Fatal("expected row-id bitmap path under a BSI field to be classified as standard")
	}
}

func TestPartitionPathsUseUTCShardTime(t *testing.T) {
	localTime := time.Date(1969, 12, 31, 18, 0, 0, 0, time.FixedZone("CST", -6*60*60))
	partition := &Partition{
		Index:       "orders",
		Field:       "o_orderkey",
		Time:        localTime,
		TQType:      "YMD",
		RowIDOrBits: 1,
	}

	path, _ := partition.generatePath(false, "/tmp/quanta", "strings")
	if !strings.Contains(path, "1970-01-01T00") {
		t.Fatalf("expected index path to use UTC shard time, got %s", path)
	}
	if strings.Contains(path, "1969-12-31T18") {
		t.Fatalf("expected index path not to use local shard time, got %s", path)
	}
}

func TestBitmapPartitionRowZeroIsStandardPath(t *testing.T) {
	partition := &Partition{
		Index:       "customers",
		Field:       "isActive",
		Time:        time.Unix(0, 0),
		RowIDOrBits: 0,
	}

	index := &BitmapIndex{Node: &Node{dataDir: "/tmp/quanta"}}
	path := index.generateBitmapFilePath(partition, false)
	if !strings.Contains(path, "customers/isActive/0") {
		t.Fatalf("expected row 0 standard bitmap path, got %s", path)
	}
	if strings.Contains(path, "customers/isActive/bsi") {
		t.Fatalf("expected row 0 not to be treated as BSI path, got %s", path)
	}
}

func TestPartitionHashKeyIncludesZeroRowID(t *testing.T) {
	partition := &Partition{
		Index:       "customers",
		Field:       "isActive",
		Time:        time.Unix(0, 0),
		RowIDOrBits: 0,
	}

	if got, want := partitionHashKey(partition), "customers/isActive/0/1970-01-01T00"; got != want {
		t.Fatalf("expected row 0 in partition hash key, got %s want %s", got, want)
	}
}

func TestPartitionHashKeyOmitsNegativeBSIRowID(t *testing.T) {
	partition := &Partition{
		Index:       "orders",
		Field:       "o_orderkey",
		Time:        time.Unix(0, 0),
		RowIDOrBits: -1,
	}

	if got, want := partitionHashKey(partition), "orders/o_orderkey/1970-01-01T00"; got != want {
		t.Fatalf("expected BSI partition hash key, got %s want %s", got, want)
	}
}

func TestForcePersistWritesCleanStandardBitmap(t *testing.T) {
	now := time.Unix(100, 0)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
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
		},
	}

	index.checkPersistBitmapCache(true)

	path := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + standardBundleLeafDir + sep + "default" + sep + standardBundleFileName
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected forced standard bitmap persist to write %s: %v", path, err)
	}
}

func TestForcePersistWritesCleanBSI(t *testing.T) {
	now := time.Unix(100, 0)
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	values.SetValue(2, 20)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"orders": {
				"o_orderkey": {
					now.UnixNano(): {
						BSI:         values,
						ModTime:     now,
						PersistTime: now,
					},
				},
			},
		},
	}

	index.checkPersistBSICache(true)

	_, path := index.bsiPackBundleFilePath("orders", time.Unix(0, 0), "")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected forced BSI persist to write %s: %v", path, err)
	}
}

func TestBSIBundleRoundTrip(t *testing.T) {
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	values.SetValue(2, 20)
	chunks, err := values.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary returned error: %v", err)
	}
	bundle, err := encodeBSIBundle(chunks)
	if err != nil {
		t.Fatalf("encodeBSIBundle returned error: %v", err)
	}
	decoded, err := decodeBSIBundle(bundle)
	if err != nil {
		t.Fatalf("decodeBSIBundle returned error: %v", err)
	}
	loaded := roaring64.NewDefaultBSI()
	if err := loaded.UnmarshalBinary(decoded); err != nil {
		t.Fatalf("UnmarshalBinary returned error: %v", err)
	}
	if got, exists := loaded.GetValue(1); !exists || got != 10 {
		t.Fatalf("loaded value for column 1 = %v exists=%v, want 10 true", got, exists)
	}
	if got, exists := loaded.GetValue(2); !exists || got != 20 {
		t.Fatalf("loaded value for column 2 = %v exists=%v, want 20 true", got, exists)
	}
}

func TestBSIPackBundleRoundTrip(t *testing.T) {
	orderKey := roaring64.NewDefaultBSI()
	orderKey.SetValue(1, 100)
	orderKey.SetValue(2, 200)
	quantity := roaring64.NewDefaultBSI()
	quantity.SetValue(1, 3)
	quantity.SetValue(2, 7)

	orderChunks, err := orderKey.MarshalBinary()
	if err != nil {
		t.Fatalf("orderKey MarshalBinary returned error: %v", err)
	}
	quantityChunks, err := quantity.MarshalBinary()
	if err != nil {
		t.Fatalf("quantity MarshalBinary returned error: %v", err)
	}
	pack, err := encodeBSIPackBundle([]bsiPackBundleEntry{
		{Field: "l_quantity", Data: quantityChunks},
		{Field: "l_orderkey", Data: orderChunks},
	})
	if err != nil {
		t.Fatalf("encodeBSIPackBundle returned error: %v", err)
	}
	decoded, err := decodeBSIPackBundle(pack)
	if err != nil {
		t.Fatalf("decodeBSIPackBundle returned error: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded pack entry count = %d, want 2", len(decoded))
	}
	orderEntry, ok := findBSIPackBundleEntry(decoded, "l_orderkey")
	if !ok {
		t.Fatal("expected l_orderkey entry")
	}
	loadedOrder := roaring64.NewDefaultBSI()
	if err := loadedOrder.UnmarshalBinary(orderEntry.Data); err != nil {
		t.Fatalf("loadedOrder UnmarshalBinary returned error: %v", err)
	}
	if got, exists := loadedOrder.GetValue(2); !exists || got != 200 {
		t.Fatalf("loaded l_orderkey value for column 2 = %v exists=%v, want 200 true", got, exists)
	}
	quantityEntry, ok := findBSIPackBundleEntry(decoded, "l_quantity")
	if !ok {
		t.Fatal("expected l_quantity entry")
	}
	loadedQuantity := roaring64.NewDefaultBSI()
	if err := loadedQuantity.UnmarshalBinary(quantityEntry.Data); err != nil {
		t.Fatalf("loadedQuantity UnmarshalBinary returned error: %v", err)
	}
	if got, exists := loadedQuantity.GetValue(1); !exists || got != 3 {
		t.Fatalf("loaded l_quantity value for column 1 = %v exists=%v, want 3 true", got, exists)
	}
}

func TestStandardBitmapBundleRoundTrip(t *testing.T) {
	left := roaring64.BitmapOf(1, 2, 3)
	right := roaring64.BitmapOf(7, 8)
	leftData, err := left.MarshalBinary()
	if err != nil {
		t.Fatalf("left MarshalBinary returned error: %v", err)
	}
	rightData, err := right.MarshalBinary()
	if err != nil {
		t.Fatalf("right MarshalBinary returned error: %v", err)
	}
	bundle, err := encodeStandardBitmapBundle([]standardBitmapBundleEntry{
		{RowID: 10, Data: leftData},
		{RowID: 20, Data: rightData},
	})
	if err != nil {
		t.Fatalf("encodeStandardBitmapBundle returned error: %v", err)
	}
	decoded, err := decodeStandardBitmapBundle(bundle)
	if err != nil {
		t.Fatalf("decodeStandardBitmapBundle returned error: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded entry count = %d, want 2", len(decoded))
	}
	loaded := roaring64.NewBitmap()
	if err := loaded.UnmarshalBinary(decoded[1].Data); err != nil {
		t.Fatalf("loaded UnmarshalBinary returned error: %v", err)
	}
	if decoded[1].RowID != 20 || loaded.GetCardinality() != 2 {
		t.Fatalf("decoded second entry row=%d cardinality=%d, want row=20 cardinality=2", decoded[1].RowID, loaded.GetCardinality())
	}
}

func TestSaveCompleteBSIRemovesLegacySliceFiles(t *testing.T) {
	now := time.Unix(100, 0)
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
	}
	dir := index.dataDir + sep + "bitmap" + sep + "orders" + sep + "o_orderkey" + sep + "bsi" + sep + "default"
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.WriteFile(dir+sep+"EBM", []byte("old"), 0644); err != nil {
		t.Fatalf("write old EBM returned error: %v", err)
	}
	if err := os.WriteFile(dir+sep+"1", []byte("old"), 0644); err != nil {
		t.Fatalf("write old slice returned error: %v", err)
	}

	if err := index.saveCompleteBSI(&BSIBitmap{BSI: values}, "orders", "o_orderkey", int(values.BitCount()), now); err != nil {
		t.Fatalf("saveCompleteBSI returned error: %v", err)
	}
	if _, err := os.Stat(dir + sep + bsiBundleFileName); err != nil {
		t.Fatalf("expected BSI bundle to be persisted: %v", err)
	}
	if _, err := os.Stat(dir + sep + "EBM"); !os.IsNotExist(err) {
		t.Fatalf("expected old EBM slice to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(dir + sep + "1"); !os.IsNotExist(err) {
		t.Fatalf("expected old bit slice to be removed, stat err=%v", err)
	}
}

func TestSaveCompleteBSIWithTimingsReportsBundleWork(t *testing.T) {
	now := time.Unix(100, 0)
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	values.SetValue(2, 20)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
	}

	timings, err := index.saveCompleteBSIWithTimings(&BSIBitmap{BSI: values}, "orders", "o_orderkey", now)
	if err != nil {
		t.Fatalf("saveCompleteBSIWithTimings returned error: %v", err)
	}
	if timings.chunkCount == 0 {
		t.Fatal("expected BSI marshal to report at least one chunk")
	}
	if timings.chunkBytes == 0 {
		t.Fatal("expected BSI marshal to report chunk bytes")
	}
	if timings.bundleBytes <= timings.chunkBytes {
		t.Fatalf("expected bundle bytes to include bundle framing, chunk_bytes=%d bundle_bytes=%d",
			timings.chunkBytes, timings.bundleBytes)
	}
	dir := index.dataDir + sep + "bitmap" + sep + "orders" + sep + "o_orderkey" + sep + "bsi" + sep + "default"
	if _, err := os.Stat(dir + sep + bsiBundleFileName); err != nil {
		t.Fatalf("expected BSI bundle to be persisted: %v", err)
	}
}

func TestSaveCompleteStandardBundleRemovesLegacyRowFiles(t *testing.T) {
	now := time.Unix(100, 0)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
	}
	legacyDir := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + "0"
	if err := os.MkdirAll(legacyDir, 0755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	legacyPath := legacyDir + sep + "1970-01-01T00"
	if err := os.WriteFile(legacyPath, []byte("old"), 0644); err != nil {
		t.Fatalf("write old standard bitmap returned error: %v", err)
	}

	written, err := index.saveCompleteStandardBundle(map[uint64]*StandardBitmap{
		0: {
			Bits:    roaring64.BitmapOf(1, 2),
			ModTime: now,
		},
	}, "customers", "isActive", now, "")
	if err != nil {
		t.Fatalf("saveCompleteStandardBundle returned error: %v", err)
	}
	if written != 1 {
		t.Fatalf("written entries = %d, want 1", written)
	}
	bundlePath := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + standardBundleLeafDir + sep + "default" + sep + standardBundleFileName
	if _, err := os.Stat(bundlePath); err != nil {
		t.Fatalf("expected standard bundle to be persisted: %v", err)
	}
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("expected old standard bitmap file to be removed, stat err=%v", err)
	}
}

func TestCommitPersistsDirtyStandardBitmapWithoutRewritingCleanBundle(t *testing.T) {
	cleanTime := time.Unix(100, 0)
	dirtyTime := cleanTime.Add(time.Hour)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
			"customers": {
				"isActive": {
					0: {
						cleanTime.UnixNano(): {
							Bits:        roaring64.BitmapOf(1, 2),
							ModTime:     cleanTime,
							PersistTime: cleanTime,
						},
					},
				},
				"dirty_isActive": {
					1: {
						dirtyTime.UnixNano(): {
							Bits:        roaring64.BitmapOf(3, 4),
							ModTime:     dirtyTime,
							PersistTime: cleanTime,
						},
					},
				},
			},
		},
		fragQueue: make(chan *BitmapFragment, 16),
		workers:   []*WorkerThread{NewWorkerThread(0)},
	}
	go index.batchProcessLoop(index.workers[0])

	cleanBitmap := index.bitmapCache["customers"]["isActive"][0][cleanTime.UnixNano()]
	if _, err := index.saveCompleteStandardBundle(map[uint64]*StandardBitmap{0: cleanBitmap},
		"customers", "isActive", cleanTime, ""); err != nil {
		t.Fatalf("saveCompleteStandardBundle returned error: %v", err)
	}
	cleanPath := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + standardBundleLeafDir + sep + "default" + sep + standardBundleFileName
	beforeClean, err := os.Stat(cleanPath)
	if err != nil {
		t.Fatalf("expected clean standard bitmap precondition to be persisted, stat err=%v", err)
	}
	time.Sleep(10 * time.Millisecond)

	if _, err := index.Commit(context.Background(), &empty.Empty{}); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}

	afterClean, err := os.Stat(cleanPath)
	if err != nil {
		t.Fatalf("expected clean standard bitmap bundle to remain available, stat err=%v", err)
	}
	if !afterClean.ModTime().Equal(beforeClean.ModTime()) {
		t.Fatalf("dirty commit rewrote clean standard bitmap bundle: before=%s after=%s",
			beforeClean.ModTime(), afterClean.ModTime())
	}
	dirtyPath := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "dirty_isActive" + sep + standardBundleLeafDir + sep + "default" + sep + standardBundleFileName
	if _, err := os.Stat(dirtyPath); err != nil {
		t.Fatalf("expected dirty standard bitmap to be persisted by commit: %v", err)
	}
}

func TestDirtyStandardBitmapPersistsCompleteShardBundle(t *testing.T) {
	now := time.Unix(100, 0)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
			"customers": {
				"isActive": {
					0: {
						now.UnixNano(): {
							Bits:        roaring64.BitmapOf(1, 2),
							ModTime:     now,
							PersistTime: now,
						},
					},
					1: {
						now.UnixNano(): {
							Bits:        roaring64.BitmapOf(3, 4, 5),
							ModTime:     now.Add(time.Second),
							PersistTime: now,
						},
					},
				},
			},
		},
	}
	index.ServicePort = 1

	_, writes, err := index.checkPersistBitmapCache(false)
	if err != nil {
		t.Fatalf("checkPersistBitmapCache returned error: %v", err)
	}
	if writes != 1 {
		t.Fatalf("writes = %d, want 1 bundle write", writes)
	}
	bundlePath := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + standardBundleLeafDir + sep + "default" + sep + standardBundleFileName
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatalf("read standard bundle returned error: %v", err)
	}
	entries, err := decodeStandardBitmapBundle(data)
	if err != nil {
		t.Fatalf("decodeStandardBitmapBundle returned error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("bundle entry count = %d, want 2", len(entries))
	}
	loaded := roaring64.NewBitmap()
	if err := loaded.UnmarshalBinary(entries[0].Data); err != nil {
		t.Fatalf("row 0 UnmarshalBinary returned error: %v", err)
	}
	if entries[0].RowID != 0 || loaded.GetCardinality() != 2 {
		t.Fatalf("first bundle entry row=%d cardinality=%d, want row=0 cardinality=2", entries[0].RowID, loaded.GetCardinality())
	}
}

func TestCommitPersistsDirtyBSIWithoutRewritingCleanBundle(t *testing.T) {
	cleanTime := time.Unix(100, 0)
	dirtyTime := cleanTime.Add(time.Hour)
	cleanValues := roaring64.NewDefaultBSI()
	cleanValues.SetValue(1, 10)
	dirtyValues := roaring64.NewDefaultBSI()
	dirtyValues.SetValue(2, 20)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"orders": {
				"clean_orderkey": {
					cleanTime.UnixNano(): {
						BSI:         cleanValues,
						ModTime:     cleanTime,
						PersistTime: cleanTime,
					},
				},
				"dirty_orderkey": {
					dirtyTime.UnixNano(): {
						BSI:         dirtyValues,
						ModTime:     dirtyTime,
						PersistTime: cleanTime,
					},
				},
			},
		},
		fragQueue: make(chan *BitmapFragment, 16),
		workers:   []*WorkerThread{NewWorkerThread(0)},
	}
	go index.batchProcessLoop(index.workers[0])

	cleanBSI := index.bsiCache["orders"]["clean_orderkey"][cleanTime.UnixNano()]
	if err := index.saveCompleteBSI(cleanBSI, "orders", "clean_orderkey", int(cleanBSI.BitCount()), cleanTime); err != nil {
		t.Fatalf("saveCompleteBSI returned error: %v", err)
	}
	cleanPath := index.dataDir + sep + "bitmap" + sep + "orders" + sep + "clean_orderkey" + sep + "bsi" + sep + "default" + sep + bsiBundleFileName
	beforeClean, err := os.Stat(cleanPath)
	if err != nil {
		t.Fatalf("expected clean BSI precondition to be persisted, stat err=%v", err)
	}
	time.Sleep(10 * time.Millisecond)

	if _, err := index.Commit(context.Background(), &empty.Empty{}); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}

	afterClean, err := os.Stat(cleanPath)
	if err != nil {
		t.Fatalf("expected clean BSI bundle to remain available, stat err=%v", err)
	}
	if !afterClean.ModTime().Equal(beforeClean.ModTime()) {
		t.Fatalf("dirty commit rewrote clean BSI bundle: before=%s after=%s",
			beforeClean.ModTime(), afterClean.ModTime())
	}
	_, dirtyPath := index.bsiPackBundleFilePath("orders", time.Unix(0, 0), "")
	if _, err := os.Stat(dirtyPath); err != nil {
		t.Fatalf("expected dirty BSI to be persisted by commit: %v", err)
	}
}

func TestShutdownPersistSkipsCleanStandardBitmap(t *testing.T) {
	now := time.Unix(100, 0)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
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
		},
	}
	index.ServicePort = 1

	index.checkPersistBitmapCache(false)

	path := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + standardBundleLeafDir + sep + "default" + sep + standardBundleFileName
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected clean standard bitmap not to be persisted, stat err=%v", err)
	}
}

func TestShutdownPersistSkipsCleanBSI(t *testing.T) {
	now := time.Unix(100, 0)
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	values.SetValue(2, 20)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"orders": {
				"o_orderkey": {
					now.UnixNano(): {
						BSI:         values,
						ModTime:     now,
						PersistTime: now,
					},
				},
			},
		},
	}
	index.ServicePort = 1

	index.checkPersistBSICache(false)

	path := index.dataDir + sep + "bitmap" + sep + "orders" + sep + "o_orderkey" + sep + "bsi" + sep + "default" + sep + bsiBundleFileName
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected clean BSI not to be persisted, stat err=%v", err)
	}
}

func TestShutdownPersistWritesDirtyStandardBitmap(t *testing.T) {
	persisted := time.Unix(100, 0)
	modified := persisted.Add(time.Second)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
			"customers": {
				"isActive": {
					0: {
						persisted.UnixNano(): {
							Bits:        roaring64.BitmapOf(1, 2),
							ModTime:     modified,
							PersistTime: persisted,
						},
					},
				},
			},
		},
	}
	index.ServicePort = 1

	index.checkPersistBitmapCache(false)

	path := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + standardBundleLeafDir + sep + "default" + sep + standardBundleFileName
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected dirty standard bitmap to be persisted, stat err=%v", err)
	}
}

func TestShutdownPersistWritesDirtyBSI(t *testing.T) {
	persisted := time.Unix(100, 0)
	modified := persisted.Add(time.Second)
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	values.SetValue(2, 20)
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"orders": {
				"o_orderkey": {
					persisted.UnixNano(): {
						BSI:         values,
						ModTime:     modified,
						PersistTime: persisted,
					},
				},
			},
		},
	}
	index.ServicePort = 1

	index.checkPersistBSICache(false)

	_, path := index.bsiPackBundleFilePath("orders", time.Unix(0, 0), "")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected dirty BSI to be persisted, stat err=%v", err)
	}
}

func TestCommitPersistForcesCleanCachesToSavepoint(t *testing.T) {
	now := time.Unix(100, 0)
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	values.SetValue(2, 20)

	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("test-node"),
			dataDir: t.TempDir(),
		},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
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
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"orders": {
				"o_orderkey": {
					now.UnixNano(): {
						BSI:         values,
						ModTime:     now,
						PersistTime: now,
					},
				},
			},
		},
	}
	index.ServicePort = 1

	bitmapCount, bitmapWrites, bsiCount, bsiWrites, err := index.persistCaches(true)
	if err != nil {
		t.Fatalf("persistCaches returned error: %v", err)
	}
	if bitmapCount != 1 || bitmapWrites != 1 || bsiCount != 1 || bsiWrites != 1 {
		t.Fatalf("expected forced commit persist to write clean caches, got bitmapCount=%d bitmapWrites=%d bsiCount=%d bsiWrites=%d",
			bitmapCount, bitmapWrites, bsiCount, bsiWrites)
	}

	bitmapPath := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + standardBundleLeafDir + sep + "default" + sep + standardBundleFileName
	if _, err := os.Stat(bitmapPath); err != nil {
		t.Fatalf("expected clean standard bitmap to be persisted by forced commit, stat err=%v", err)
	}

	_, bsiPath := index.bsiPackBundleFilePath("orders", time.Unix(0, 0), "")
	if _, err := os.Stat(bsiPath); err != nil {
		t.Fatalf("expected clean BSI to be persisted by forced commit, stat err=%v", err)
	}
}

func TestLocalClusterZeroPortPersistsDirtyBSI(t *testing.T) {
	now := time.Unix(100, 0)
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	values.SetValue(2, 20)
	conn := shared.NewDefaultConnection("local-cluster")
	conn.IsLocalCluster = true

	index := &BitmapIndex{
		Node: &Node{
			Conn:    conn,
			dataDir: t.TempDir(),
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"orders": {
				"o_orderkey": {
					now.UnixNano(): {
						BSI:         values,
						ModTime:     now,
						PersistTime: now.Add(-time.Second),
					},
				},
			},
		},
	}

	bsiCount, bsiWrites, err := index.checkPersistBSICache(false)
	if err != nil {
		t.Fatalf("checkPersistBSICache returned error: %v", err)
	}
	if bsiCount != 1 || bsiWrites != 1 {
		t.Fatalf("expected local zero-port BSI persistence, got count=%d writes=%d", bsiCount, bsiWrites)
	}

	_, path := index.bsiPackBundleFilePath("orders", time.Unix(0, 0), "")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected local zero-port BSI persist to write %s: %v", path, err)
	}
}

func TestCalculateMemoryUsageCountsStandardBitmapAndBSI(t *testing.T) {
	now := time.Unix(100, 0)
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	values.SetValue(2, 20)
	index := &BitmapIndex{
		Node: &Node{
			Conn: shared.NewDefaultConnection("test-node"),
		},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
			"customers": {
				"isActive": {
					0: {
						now.UnixNano(): {
							Bits: roaring64.BitmapOf(1, 2),
						},
					},
				},
			},
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"orders": {
				"o_orderkey": {
					now.UnixNano(): {
						BSI: values,
					},
				},
			},
		},
	}

	index.calculateMemoryUsage()

	if index.memoryUsed == 0 {
		t.Fatal("expected memory usage estimate to include cached bitmap and BSI shards")
	}
	if index.shardCount != 2 {
		t.Fatalf("expected shard count to include cached bitmap and BSI shards, got %d", index.shardCount)
	}
}

func TestCalculateShardCountCountsStandardBitmapAndBSI(t *testing.T) {
	now := time.Unix(100, 0)
	values := roaring64.NewDefaultBSI()
	values.SetValue(1, 10)
	index := &BitmapIndex{
		Node: &Node{
			Conn: shared.NewDefaultConnection("test-node"),
		},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
			"customers": {
				"isActive": {
					0: {
						now.UnixNano(): {
							Bits: roaring64.BitmapOf(1, 2),
						},
					},
				},
			},
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"orders": {
				"o_orderkey": {
					now.UnixNano(): {
						BSI: values,
					},
				},
			},
		},
	}

	index.calculateShardCount()

	if index.shardCount != 2 {
		t.Fatalf("expected shard count to include cached bitmap and BSI shards, got %d", index.shardCount)
	}
}

func TestCalculateMemoryUsageSkipsBusyCache(t *testing.T) {
	now := time.Unix(100, 0)
	index := &BitmapIndex{
		Node: &Node{
			Conn: shared.NewDefaultConnection("test-node"),
		},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
			"customers": {
				"isActive": {
					0: {
						now.UnixNano(): {
							Bits: roaring64.BitmapOf(1, 2),
						},
					},
				},
			},
		},
	}
	index.memoryUsed = 123
	index.shardCount = 7

	index.bitmapCacheLock.Lock()
	index.calculateMemoryUsage()
	index.bitmapCacheLock.Unlock()

	if index.memoryUsed != 123 {
		t.Fatalf("expected busy cache to retain previous estimate, got %d", index.memoryUsed)
	}
	if index.shardCount != 7 {
		t.Fatalf("expected busy cache to retain previous shard count, got %d", index.shardCount)
	}
}
