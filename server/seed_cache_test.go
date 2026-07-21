package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/hashicorp/consul/api"
	"github.com/stvp/rendezvous"
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

	cached, count, ok := index.cachedSeedBitmap("lineitem", field, day1, day2)
	if !ok {
		t.Fatal("expected cached seed after update")
	}
	if got, want := cached.GetCardinality(), uint64(3); got != want {
		t.Fatalf("cached seed cardinality = %d, want %d", got, want)
	}
	if got, want := count, uint64(3); got != want {
		t.Fatalf("cached seed row count = %d, want %d", got, want)
	}
	if !cached.Contains(3) {
		t.Fatalf("cached seed does not include rownum 3 after update: %#v", cached.ToArray())
	}
}

func TestLiveBSIUpdateUsesApplyTimeForDirtyTracking(t *testing.T) {
	index := newSeedCacheTestIndex(t)
	field := "l_shipdate"
	day := time.Date(2023, 6, 2, 0, 0, 0, 0, time.UTC)
	persistedAt := time.Now().Add(-time.Second)
	existing := seedCacheTestBSI(map[uint64]int64{1: 20230602})
	existing.ModTime = persistedAt
	existing.PersistTime = persistedAt
	index.bsiCache["lineitem"][field][day.UnixNano()] = existing

	update := seedCacheTestBSI(map[uint64]int64{2: 20230602})
	fragment := newBitmapFragment("lineitem", field, -1, day, mustMarshalSeedCacheBSI(t, update.BSI), true, false, false)
	fragment.ModTime = persistedAt.Add(-time.Hour)

	index.updateBSICache(fragment)

	updated := index.bsiCache["lineitem"][field][day.UnixNano()]
	if got, want := updated.GetExistenceBitmap().GetCardinality(), uint64(2); got != want {
		t.Fatalf("updated BSI cardinality = %d, want %d", got, want)
	}
	if !updated.ModTime.After(updated.PersistTime) {
		t.Fatalf("live update should mark shard dirty: mod=%s persist=%s fragment=%s",
			updated.ModTime, updated.PersistTime, fragment.ModTime)
	}
}

func TestLiveBitmapUpdateUsesApplyTimeForDirtyTracking(t *testing.T) {
	index := newSeedCacheTestIndex(t)
	field := "l_returnflag"
	rowID := uint64(1)
	day := time.Date(2023, 6, 2, 0, 0, 0, 0, time.UTC)
	persistedAt := time.Now().Add(-time.Second)
	existing := &StandardBitmap{
		Bits:        roaring64.BitmapOf(1),
		ModTime:     persistedAt,
		PersistTime: persistedAt,
		AccessTime:  persistedAt,
	}
	index.bitmapCache = map[string]map[string]map[uint64]map[int64]*StandardBitmap{
		"lineitem": {
			field: {
				rowID: {
					day.UnixNano(): existing,
				},
			},
		},
	}

	bits := roaring64.BitmapOf(2)
	data, err := bits.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal bitmap: %v", err)
	}
	fragment := newBitmapFragment("lineitem", field, int64(rowID), day, [][]byte{data}, false, false, false)
	fragment.ModTime = persistedAt.Add(-time.Hour)

	index.updateBitmapCache(fragment)

	updated := index.bitmapCache["lineitem"][field][rowID][day.UnixNano()]
	if got, want := updated.Bits.GetCardinality(), uint64(2); got != want {
		t.Fatalf("updated bitmap cardinality = %d, want %d", got, want)
	}
	if !updated.ModTime.After(updated.PersistTime) {
		t.Fatalf("live update should mark standard bitmap dirty: mod=%s persist=%s fragment=%s",
			updated.ModTime, updated.PersistTime, fragment.ModTime)
	}
}

func TestTimeRangeExistenceReadsLocalBSIWithoutOwnershipFiltering(t *testing.T) {
	index := newSeedCacheTestIndex(t)
	index.Node.consul = &api.Client{}
	index.Node.hashKey = "not-the-rendezvous-owner"
	index.Node.Conn.HashTable = rendezvous.New([]string{"different-owner"})
	field := "l_shipdate"
	day := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
	index.bsiCache["lineitem"][field][day.UnixNano()] = seedCacheTestBSI(map[uint64]int64{
		10: 20230601,
		11: 20230601,
	})

	seed, err := index.timeRangeExistence("lineitem", field, day, day)
	if err != nil {
		t.Fatalf("timeRangeExistence returned error: %v", err)
	}
	if got, want := seed.GetCardinality(), uint64(2); got != want {
		t.Fatalf("seed cardinality = %d, want %d", got, want)
	}
	if !seed.Contains(10) || !seed.Contains(11) {
		t.Fatalf("seed does not contain local BSI rownums: %#v", seed.ToArray())
	}
}

func TestQueryExistenceSeedReturnsUnionWithoutGlobalExistenceCap(t *testing.T) {
	index := newSeedCacheTestIndex(t)
	field := "l_shipdate"
	day := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
	index.bsiCache["lineitem"][field][day.UnixNano()] = seedCacheTestBSI(map[uint64]int64{
		21: 20230601,
		22: 20230601,
	})
	if !index.isBSI("lineitem", field) {
		attr, err := index.getFieldConfig("lineitem", field)
		t.Fatalf("fixture field is not classified as BSI; attr=%#v err=%v", attr, err)
	}
	protoQuery := &pb.BitmapQuery{
		FromTime: day.UnixNano(),
		ToTime:   day.UnixNano(),
		Query: []*pb.QueryFragment{{
			Id:        "seed",
			Index:     "lineitem",
			Field:     field,
			Operation: pb.QueryFragment_UNION,
			NullCheck: true,
			Negate:    true,
		}},
	}

	result, err := index.Query(context.Background(), protoQuery)
	if err != nil {
		t.Fatalf("Query returned error: %v", err)
	}
	union := roaring64.NewBitmap()
	if err := union.UnmarshalBinary(result.GetUnions()); err != nil {
		t.Fatalf("unmarshal union: %v", err)
	}
	if got, want := union.GetCardinality(), uint64(2); got != want {
		t.Fatalf("union cardinality = %d, want %d", got, want)
	}
	existence := roaring64.NewBitmap()
	if err := existence.UnmarshalBinary(result.GetExistences()); err != nil {
		t.Fatalf("unmarshal existence: %v", err)
	}
	if got := existence.GetCardinality(); got != 0 {
		t.Fatalf("existence cardinality = %d, want 0", got)
	}
	if got := len(result.GetIntersects()); got != 0 {
		t.Fatalf("intersect count = %d, want 0", got)
	}
}

func TestQueryExistenceSeedPreservesRowCountAcrossCollapsedShards(t *testing.T) {
	index := newSeedCacheTestIndex(t)
	field := "l_shipdate"
	day1 := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2023, 6, 2, 0, 0, 0, 0, time.UTC)
	index.bsiCache["lineitem"][field][day1.UnixNano()] = seedCacheTestBSI(map[uint64]int64{
		1: 20230601,
		2: 20230601,
	})
	index.bsiCache["lineitem"][field][day2.UnixNano()] = seedCacheTestBSI(map[uint64]int64{
		1: 20230602,
		2: 20230602,
	})
	protoQuery := &pb.BitmapQuery{
		FromTime: day1.UnixNano(),
		ToTime:   day2.UnixNano(),
		Query: []*pb.QueryFragment{{
			Id:        "seed",
			Index:     "lineitem",
			Field:     field,
			Operation: pb.QueryFragment_UNION,
			NullCheck: true,
			Negate:    true,
		}},
	}

	result, err := index.Query(context.Background(), protoQuery)
	if err != nil {
		t.Fatalf("Query returned error: %v", err)
	}
	ir := shared.NewIntermediateResult("lineitem")
	if err := ir.UnmarshalAndAdd(result); err != nil {
		t.Fatalf("unmarshal intermediate result: %v", err)
	}
	ir.Collapse()
	if got, want := ir.GetFinalUnion().GetCardinality(), uint64(2); got != want {
		t.Fatalf("collapsed union cardinality = %d, want %d", got, want)
	}
	if got, want := ir.Count(), uint64(4); got != want {
		t.Fatalf("preserved row count = %d, want %d", got, want)
	}
}

func TestProjectBSIReturnsDirectFoundSetProjection(t *testing.T) {
	index := newSeedCacheTestIndex(t)
	field := "l_extendedprice"
	day := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
	index.bsiCache["lineitem"][field][day.UnixNano()] = seedCacheTestBSI(map[uint64]int64{
		1: 10000,
		2: 20000,
	})

	bsi, stats, err := index.ProjectBSIWithStats("lineitem", field, day.UnixNano(), day.UnixNano(), roaring64.BitmapOf(2), false)
	if err != nil {
		t.Fatalf("ProjectBSI returned error: %v", err)
	}
	if bsi == nil {
		t.Fatal("ProjectBSI returned nil BSI")
	}
	if got, want := bsi.GetExistenceBitmap().GetCardinality(), uint64(1); got != want {
		t.Fatalf("projected cardinality = %d, want %d", got, want)
	}
	if _, ok := bsi.GetValue(1); ok {
		t.Fatalf("rownum 1 should not be retained by foundset")
	}
	value, ok := bsi.GetValue(2)
	if !ok || value != 20000 {
		t.Fatalf("rownum 2 value = %d ok=%t, want 20000 true", value, ok)
	}
	if stats.ShardsVisited != 1 || stats.ShardsInWindow != 1 || stats.ShardsLocal != 1 || stats.ShardsRetained != 1 {
		t.Fatalf("stats shards = visited:%d window:%d local:%d retained:%d, want all 1",
			stats.ShardsVisited, stats.ShardsInWindow, stats.ShardsLocal, stats.ShardsRetained)
	}
	if stats.RetainedRows != 1 {
		t.Fatalf("stats retained rows = %d, want 1", stats.RetainedRows)
	}
}

func TestProjectBSIsWithStatsReturnsAlignedProjectionFields(t *testing.T) {
	index := newSeedCacheTestIndex(t)
	day := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
	index.bsiCache["lineitem"]["l_shipdate"][day.UnixNano()] = seedCacheTestBSI(map[uint64]int64{
		1: 20230601,
		2: 20230602,
	})
	index.bsiCache["lineitem"]["l_extendedprice"][day.UnixNano()] = seedCacheTestBSI(map[uint64]int64{
		1: 10000,
		2: 20000,
	})

	results, stats, err := index.ProjectBSIsWithStats(
		"lineitem",
		[]string{"l_shipdate", "l_extendedprice"},
		day.UnixNano(),
		day.UnixNano(),
		roaring64.BitmapOf(2),
		false,
	)
	if err != nil {
		t.Fatalf("ProjectBSIsWithStats returned error: %v", err)
	}
	if len(results) != 2 || len(stats) != 2 {
		t.Fatalf("projected fields = %d stats = %d, want 2/2", len(results), len(stats))
	}
	assertProjectedBSIValue(t, results["l_shipdate"], 2, 20230602)
	assertProjectedBSIValue(t, results["l_extendedprice"], 2, 20000)
	if _, ok := results["l_shipdate"].GetValue(1); ok {
		t.Fatalf("rownum 1 should not be retained in l_shipdate projection")
	}
	if stats["l_shipdate"].RetainedRows != 1 || stats["l_extendedprice"].RetainedRows != 1 {
		t.Fatalf("retained rows = shipdate:%d extendedprice:%d, want 1/1",
			stats["l_shipdate"].RetainedRows,
			stats["l_extendedprice"].RetainedRows)
	}
}

func TestProjectBSIWithStatsBypassesRetainWhenFoundSetCoversShard(t *testing.T) {
	index := newSeedCacheTestIndex(t)
	day := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
	index.bsiCache["lineitem"]["l_shipdate"][day.UnixNano()] = seedCacheTestBSI(map[uint64]int64{
		1: 20230601,
		2: 20230602,
	})

	bsi, stats, err := index.ProjectBSIWithStats(
		"lineitem",
		"l_shipdate",
		day.UnixNano(),
		day.UnixNano(),
		roaring64.BitmapOf(1, 2, 99),
		false,
	)
	if err != nil {
		t.Fatalf("ProjectBSIWithStats returned error: %v", err)
	}
	assertProjectedBSIValue(t, bsi, 1, 20230601)
	assertProjectedBSIValue(t, bsi, 2, 20230602)
	if got, want := stats.RetainedRows, uint64(2); got != want {
		t.Fatalf("retained rows = %d, want %d", got, want)
	}
	if got, want := stats.RetainBypassRows, uint64(2); got != want {
		t.Fatalf("retain bypass rows = %d, want %d", got, want)
	}
}

func TestProjectBSIWithStatsRetainsSparseFoundSetWithoutIntermediateRetainSet(t *testing.T) {
	index := newSeedCacheTestIndex(t)
	day := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
	index.bsiCache["lineitem"]["l_shipdate"][day.UnixNano()] = seedCacheTestBSI(map[uint64]int64{
		1: 20230601,
		2: 20230602,
		3: 20230603,
	})

	bsi, stats, err := index.ProjectBSIWithStats(
		"lineitem",
		"l_shipdate",
		day.UnixNano(),
		day.UnixNano(),
		roaring64.BitmapOf(2, 99),
		false,
	)
	if err != nil {
		t.Fatalf("ProjectBSIWithStats returned error: %v", err)
	}
	assertProjectedBSIValue(t, bsi, 2, 20230602)
	if _, ok := bsi.GetValue(1); ok {
		t.Fatalf("rownum 1 should not be retained")
	}
	if _, ok := bsi.GetValue(3); ok {
		t.Fatalf("rownum 3 should not be retained")
	}
	if got, want := stats.RetainedRows, uint64(1); got != want {
		t.Fatalf("retained rows = %d, want %d", got, want)
	}
	if got := stats.RetainBypassRows; got != 0 {
		t.Fatalf("retain bypass rows = %d, want 0", got)
	}
}

func TestProjectBSIWithStatsRetainsBroadPartialFoundSet(t *testing.T) {
	index := newSeedCacheTestIndex(t)
	day := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
	index.bsiCache["lineitem"]["l_shipdate"][day.UnixNano()] = seedCacheTestBSI(map[uint64]int64{
		1: 20230601,
		2: 20230602,
		3: 20230603,
	})

	bsi, stats, err := index.ProjectBSIWithStats(
		"lineitem",
		"l_shipdate",
		day.UnixNano(),
		day.UnixNano(),
		roaring64.BitmapOf(1, 3, 99, 100),
		false,
	)
	if err != nil {
		t.Fatalf("ProjectBSIWithStats returned error: %v", err)
	}
	assertProjectedBSIValue(t, bsi, 1, 20230601)
	assertProjectedBSIValue(t, bsi, 3, 20230603)
	if _, ok := bsi.GetValue(2); ok {
		t.Fatalf("rownum 2 should not be retained")
	}
	if got, want := stats.RetainedRows, uint64(2); got != want {
		t.Fatalf("retained rows = %d, want %d", got, want)
	}
	if got := stats.RetainBypassRows; got != 0 {
		t.Fatalf("retain bypass rows = %d, want 0", got)
	}
}

func assertProjectedBSIValue(t *testing.T, bsi *roaring64.BSI, rownum uint64, want int64) {
	t.Helper()
	if bsi == nil {
		t.Fatalf("projected BSI = nil")
	}
	got, ok := bsi.GetValue(rownum)
	if !ok || got != want {
		t.Fatalf("rownum %d value = %d ok=%t, want %d true", rownum, got, ok, want)
	}
}

func newSeedCacheTestIndex(t *testing.T) *BitmapIndex {
	t.Helper()
	root := t.TempDir()
	configDir := filepath.Join(root, "config", "lineitem")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("create config dir: %v", err)
	}
	schema := []byte(`
tableName: lineitem
primaryKey: l_shipdate
timeQuantumType: YMD
timeQuantumField: l_shipdate
attributes:
- fieldName: l_shipdate
  sourceName: /data/l_shipdate
  mappingStrategy: SysMillisBSI
  type: DateTime
- fieldName: l_extendedprice
  sourceName: /data/l_extendedprice
  mappingStrategy: FloatScaleBSI
  type: Float
- fieldName: l_returnflag
  sourceName: /data/l_returnflag
  mappingStrategy: StringEnum
  type: String
`)
	if err := os.WriteFile(filepath.Join(configDir, "schema.yaml"), schema, 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	table, err := shared.LoadSchema(filepath.Join(root, "config"), "lineitem", nil)
	if err != nil {
		t.Fatalf("load schema: %v", err)
	}
	return &BitmapIndex{
		Node:      &Node{Conn: shared.NewDefaultConnection("seed-cache-test")},
		bsiCache:  map[string]map[string]map[int64]*BSIBitmap{"lineitem": {"l_shipdate": {}, "l_extendedprice": {}}},
		seedCache: make(map[string]*SeedBitmap),
		tableCache: map[string]*shared.BasicTable{
			"lineitem": table,
		},
	}
}

func seedCacheTestBSI(values map[uint64]int64) *BSIBitmap {
	bsi := roaring64.NewDefaultBSI()
	for rownum, value := range values {
		bsi.SetValue(rownum, value)
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
