package server

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
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

	path := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + "0" + sep + "1970-01-01T00"
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

	path := index.dataDir + sep + "bitmap" + sep + "orders" + sep + "o_orderkey" + sep + "bsi" + sep + "default" + sep + "EBM"
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected forced BSI persist to write %s: %v", path, err)
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

	path := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + "0" + sep + "1970-01-01T00"
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

	path := index.dataDir + sep + "bitmap" + sep + "orders" + sep + "o_orderkey" + sep + "bsi" + sep + "default" + sep + "EBM"
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

	path := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + "0" + sep + "1970-01-01T00"
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

	path := index.dataDir + sep + "bitmap" + sep + "orders" + sep + "o_orderkey" + sep + "bsi" + sep + "default" + sep + "EBM"
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected dirty BSI to be persisted, stat err=%v", err)
	}
}

func TestCommitPersistSkipsCleanCaches(t *testing.T) {
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

	bitmapCount, bitmapWrites, bsiCount, bsiWrites, err := index.persistDirtyCaches()
	if err != nil {
		t.Fatalf("persistDirtyCaches returned error: %v", err)
	}
	if bitmapCount != 1 || bitmapWrites != 0 || bsiCount != 1 || bsiWrites != 0 {
		t.Fatalf("expected clean commit persist to count caches but skip writes, got bitmapCount=%d bitmapWrites=%d bsiCount=%d bsiWrites=%d",
			bitmapCount, bitmapWrites, bsiCount, bsiWrites)
	}

	bitmapPath := index.dataDir + sep + "bitmap" + sep + "customers" + sep + "isActive" + sep + "0" + sep + "1970-01-01T00"
	if _, err := os.Stat(bitmapPath); !os.IsNotExist(err) {
		t.Fatalf("expected clean standard bitmap not to be persisted by commit, stat err=%v", err)
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

	index.bitmapCacheLock.Lock()
	index.calculateMemoryUsage()
	index.bitmapCacheLock.Unlock()

	if index.memoryUsed != 123 {
		t.Fatalf("expected busy cache to retain previous estimate, got %d", index.memoryUsed)
	}
}
