package server

import (
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestTimeRangeExistenceCachesSeedAndUpdatesFromBSI(t *testing.T) {
	index := newSeedCacheTestIndex(t)
	field := "l_shipdate"
	day1 := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2023, 6, 2, 0, 0, 0, 0, time.UTC)

	index.bsiCache["lineitem"][field][day1.UnixNano()] = seedCacheTestBSI(map[uint64]int64{1: 20230601})
	index.bsiCache["lineitem"][field][day2.UnixNano()] = seedCacheTestBSI(map[uint64]int64{2: 20230602})

	seed, err := index.timeRangeExistence("lineitem", field, day1, day2)
	if err != nil {
		t.Fatalf("timeRangeExistence returned error: %v", err)
	}
	if got, want := seed.GetCardinality(), uint64(2); got != want {
		t.Fatalf("seed cardinality = %d, want %d", got, want)
	}
	if len(index.seedCache) != 1 {
		t.Fatalf("seed cache entries = %d, want 1", len(index.seedCache))
	}

	update := seedCacheTestBSI(map[uint64]int64{3: 20230602})
	fragment := newBitmapFragment("lineitem", field, -1, day2, mustMarshalSeedCacheBSI(t, update.BSI), true, false, true)
	index.updateBSICache(fragment)

	cached, ok := index.cachedSeedBitmap("lineitem", field, day1, day2)
	if !ok {
		t.Fatal("expected cached seed after update")
	}
	if got, want := cached.GetCardinality(), uint64(3); got != want {
		t.Fatalf("cached seed cardinality = %d, want %d", got, want)
	}
	if !cached.Contains(3) {
		t.Fatalf("cached seed does not include rownum 3 after update: %#v", cached.ToArray())
	}
}

func newSeedCacheTestIndex(t *testing.T) *BitmapIndex {
	t.Helper()
	root := t.TempDir()
	configDir := filepath.Join(root, "config", "lineitem")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("create config dir: %v", err)
	}
	schema := []byte(`tableName: lineitem
primaryKey: l_shipdate
timeQuantumType: YMD
timeQuantumField: l_shipdate
attributes:
- fieldName: l_shipdate
  sourceName: /data/l_shipdate
  mappingStrategy: SysMillisBSI
  type: DateTime
`)
	if err := os.WriteFile(filepath.Join(configDir, "schema.yaml"), schema, 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	table, err := shared.LoadSchema(filepath.Join(root, "config"), "lineitem", nil)
	if err != nil {
		t.Fatalf("load schema: %v", err)
	}
	return &BitmapIndex{
		Node:      &Node{},
		bsiCache:  map[string]map[string]map[int64]*BSIBitmap{"lineitem": {"l_shipdate": {}}},
		seedCache: make(map[string]*SeedBitmap),
		tableCache: map[string]*shared.BasicTable{
			"lineitem": table,
		},
	}
}

func seedCacheTestBSI(values map[uint64]int64) *BSIBitmap {
	bsi := roaring64.NewDefaultBSI()
	for rownum, value := range values {
		bsi.SetBigValue(rownum, big.NewInt(value))
	}
	return &BSIBitmap{BSI: bsi}
}

func mustMarshalSeedCacheBSI(t *testing.T, bsi *roaring64.BSI) [][]byte {
	t.Helper()
	data, err := bsi.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal BSI: %v", err)
	}
	return data
}
