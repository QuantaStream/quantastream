package qsruntime

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestWithQueryScratchpadInstallsOneRequestCache(t *testing.T) {
	ctx := WithQueryScratchpad(context.Background())
	scratchpad := QueryScratchpadFromContext(ctx)
	if scratchpad == nil {
		t.Fatal("scratchpad was not installed")
	}
	if scratchpad.ProjectionBSIs == nil {
		t.Fatal("projection BSI cache was not installed")
	}
	if scratchpad.DomainMappings == nil {
		t.Fatal("domain mapping cache was not installed")
	}
	if scratchpad.RelationshipVectorProjections == nil {
		t.Fatal("relationship-vector projection cache was not installed")
	}
	if scratchpad.Instrumentation == nil {
		t.Fatal("execution instrumentation was not installed")
	}
	if again := WithQueryScratchpad(ctx); again != ctx {
		t.Fatal("scratchpad wrapper should preserve an existing request cache")
	}
}

func TestExecutionInstrumentationRecordsStructuredObservations(t *testing.T) {
	recorder := NewExecutionInstrumentation()
	recorder.ObserveDuration("direct_bitmap", "phase_bitmap_query_elapsed", 12*time.Millisecond, "query")
	recorder.ObserveCount("direct_bitmap", "bitmap_count", 42, "rownums=42")
	recorder.ObserveEvent("direct_bitmap", "strategy", "seed_cache", "warm")

	snapshot := recorder.Snapshot()
	if len(snapshot.Timings) != 1 || snapshot.Timings[0].Duration != 12*time.Millisecond {
		t.Fatalf("timings = %#v, want one 12ms timing", snapshot.Timings)
	}
	if len(snapshot.Counters) != 1 || snapshot.Counters[0].Value != 42 {
		t.Fatalf("counters = %#v, want bitmap_count=42", snapshot.Counters)
	}
	if len(snapshot.Events) != 1 || snapshot.Events[0].Value != "seed_cache" {
		t.Fatalf("events = %#v, want strategy event", snapshot.Events)
	}

	snapshot.Timings[0].Duration = time.Second
	if got := recorder.Snapshot().Timings[0].Duration; got != 12*time.Millisecond {
		t.Fatalf("mutating snapshot changed recorder duration to %s", got)
	}
}

func TestQueryScratchpadCacheObservationHelpersRecordLookupAndStore(t *testing.T) {
	ctx := WithQueryScratchpad(context.Background())

	RecordQueryScratchpadCacheLookup(ctx, "projection_bsi_cache", true, "exact", "index=lineitem field=l_orderkey")
	RecordQueryScratchpadCacheLookup(ctx, "domain_mapping_cache", false, "miss", "source=l target=o")
	RecordQueryScratchpadCacheStore(ctx, "relationship_vector_projection_cache", "key=lineitem:l_orderkey")

	snapshot := ExecutionInstrumentationSnapshotFromContext(ctx)
	assertExecutionCounter(t, snapshot, "query_scratchpad", "projection_bsi_cache_hit", 1)
	assertExecutionCounter(t, snapshot, "query_scratchpad", "domain_mapping_cache_miss", 1)
	assertExecutionCounter(t, snapshot, "query_scratchpad", "relationship_vector_projection_cache_store", 1)
	if !executionEventFound(snapshot, "query_scratchpad", "projection_bsi_cache_lookup", "hit") {
		t.Fatalf("projection cache hit event not found in %#v", snapshot.Events)
	}
	if !executionEventFound(snapshot, "query_scratchpad", "domain_mapping_cache_lookup", "miss") {
		t.Fatalf("domain mapping miss event not found in %#v", snapshot.Events)
	}
	if !executionEventFound(snapshot, "query_scratchpad", "relationship_vector_projection_cache_store", "stored") {
		t.Fatalf("relationship-vector store event not found in %#v", snapshot.Events)
	}
}

func executionEventFound(snapshot ExecutionInstrumentationSnapshot, section string, name string, value string) bool {
	for _, event := range snapshot.Events {
		if event.Section == section && event.Name == name && event.Value == value {
			return true
		}
	}
	return false
}

func TestWithDirectProjectionBSICacheInstallsSharedScratchpad(t *testing.T) {
	ctx := WithDirectProjectionBSICache(context.Background())
	if QueryScratchpadFromContext(ctx) == nil {
		t.Fatal("direct compatibility wrapper should install the shared scratchpad")
	}
}

func TestProjectionBSICacheVerifiesRownumCoverage(t *testing.T) {
	request := NativeProjectionBSIReadRequest{
		Index:         "lineitem",
		PhysicalField: "l_orderkey",
		Rownums: []qsbridge.QuantaRownum{
			101,
			102,
			103,
		},
	}
	key := ProjectionBSICacheKeyFor(request, 10, 20)
	cache := NewProjectionBSICache()
	bsi := roaring64.NewDefaultBSI()
	rownumSet := legacyDirectRelationshipBitmap(request.Rownums)
	cache.Set(key, rownumSet, bsi)

	var absentCache *ProjectionBSICache
	if _, mode, ok := absentCache.Get(key, rownumSet); ok || mode != "cache_absent" {
		t.Fatalf("nil cache lookup mode = %q/%t, want cache_absent miss", mode, ok)
	}
	if got, mode, ok := cache.Get(key, rownumSet); !ok || mode != "exact" || got != bsi {
		t.Fatalf("cache lookup = %#v/%q/%t, want exact stored BSI", got, mode, ok)
	}
	reordered := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{103, 102, 101})
	if got, mode, ok := cache.Get(key, reordered); !ok || mode != "exact" || got != bsi {
		t.Fatalf("reordered cache lookup = %#v/%q/%t, want exact stored BSI", got, mode, ok)
	}
	fieldChanged := request
	fieldChanged.PhysicalField = "l_suppkey"
	if _, mode, ok := cache.Get(ProjectionBSICacheKeyFor(fieldChanged, 10, 20), rownumSet); ok || mode != "key_miss" {
		t.Fatalf("field-changed lookup mode = %q/%t, want key_miss", mode, ok)
	}
	rownumsChanged := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{101, 999, 103})
	if _, mode, ok := cache.Get(key, rownumsChanged); ok || mode != "coverage_miss" {
		t.Fatalf("rownum-changed lookup mode = %q/%t, want coverage_miss", mode, ok)
	}
	if _, mode, ok := cache.Get(ProjectionBSICacheKeyFor(request, 10, 30), rownumSet); ok || mode != "key_miss" {
		t.Fatalf("window-changed lookup mode = %q/%t, want key_miss", mode, ok)
	}
}

func TestProjectionBSICacheRetainsCoveredSubset(t *testing.T) {
	request := NativeProjectionBSIReadRequest{
		Index:         "lineitem",
		PhysicalField: "l_orderkey",
		Rownums:       []qsbridge.QuantaRownum{101, 102, 103},
	}
	key := ProjectionBSICacheKeyFor(request, 10, 20)
	cache := NewProjectionBSICache()
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(101, big.NewInt(1001))
	bsi.SetBigValue(102, big.NewInt(1002))
	bsi.SetBigValue(103, big.NewInt(1003))
	cache.Set(key, legacyDirectRelationshipBitmap(request.Rownums), bsi)

	subset := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{103, 101})
	got, mode, ok := cache.Get(key, subset)
	if !ok || mode != "retained_subset" {
		t.Fatalf("subset cache lookup mode = %q/%t, want retained_subset", mode, ok)
	}
	if got == bsi {
		t.Fatal("subset lookup should return a retained copy")
	}
	if got.GetExistenceBitmap().GetCardinality() != 2 {
		t.Fatalf("subset cardinality = %d, want 2", got.GetExistenceBitmap().GetCardinality())
	}
	if _, ok := got.GetBigValue(102); ok {
		t.Fatal("subset lookup should not retain rownum 102")
	}
}

func TestProjectionBSICacheReturnsPartialMissingRows(t *testing.T) {
	request := NativeProjectionBSIReadRequest{
		Index:         "lineitem",
		PhysicalField: "l_orderkey",
		Rownums:       []qsbridge.QuantaRownum{101, 102},
	}
	key := ProjectionBSICacheKeyFor(request, 10, 20)
	cache := NewProjectionBSICache()
	cached := roaring64.NewDefaultBSI()
	cached.SetBigValue(101, big.NewInt(1001))
	cached.SetBigValue(102, big.NewInt(1002))
	cache.Set(key, legacyDirectRelationshipBitmap(request.Rownums), cached)

	requested := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{101, 102, 103, 104})
	partial, mode, ok := cache.GetPartial(key, requested)
	if !ok || mode != "partial_hit" {
		t.Fatalf("partial lookup mode = %q/%t, want partial_hit", mode, ok)
	}
	if partial.CoveredCardinality() != 2 || partial.MissingCardinality() != 2 {
		t.Fatalf("partial coverage = %d/%d, want 2 covered and 2 missing", partial.CoveredCardinality(), partial.MissingCardinality())
	}
	if got := partial.MissingRownums(); len(got) != 2 || got[0] != 103 || got[1] != 104 {
		t.Fatalf("missing rownums = %#v, want 103 and 104", got)
	}
	if value, ok := partial.BSI.GetBigValue(101); !ok || value.Int64() != 1001 {
		t.Fatalf("cached partial value for 101 = %v/%t, want 1001", value, ok)
	}
	if _, ok := partial.BSI.GetBigValue(103); ok {
		t.Fatal("cached partial should not include missing rownum 103")
	}

	fetched := roaring64.NewDefaultBSI()
	fetched.SetBigValue(103, big.NewInt(1003))
	fetched.SetBigValue(104, big.NewInt(1004))
	merged := partial.MergeFetchedMissing(fetched)
	for rownum, want := range map[uint64]int64{101: 1001, 102: 1002, 103: 1003, 104: 1004} {
		value, ok := merged.GetBigValue(rownum)
		if !ok || value.Int64() != want {
			t.Fatalf("merged value for %d = %v/%t, want %d", rownum, value, ok, want)
		}
	}
}

func TestProjectionBSICacheMergesMultipleEntries(t *testing.T) {
	request := NativeProjectionBSIReadRequest{
		Index:         "lineitem",
		PhysicalField: "l_orderkey",
	}
	key := ProjectionBSICacheKeyFor(request, 10, 20)
	cache := NewProjectionBSICache()
	first := roaring64.NewDefaultBSI()
	first.SetBigValue(101, big.NewInt(1001))
	first.SetBigValue(102, big.NewInt(1002))
	cache.Set(key, legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{101, 102}), first)
	second := roaring64.NewDefaultBSI()
	second.SetBigValue(103, big.NewInt(1003))
	cache.Set(key, legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{103}), second)

	requested := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{101, 102, 103})
	partial, mode, ok := cache.GetPartial(key, requested)
	if !ok || mode != "merged_entries" {
		t.Fatalf("merged lookup mode = %q/%t, want merged_entries", mode, ok)
	}
	if partial.CoveredCardinality() != 3 || partial.MissingCardinality() != 0 {
		t.Fatalf("merged coverage = %d/%d, want 3 covered and 0 missing", partial.CoveredCardinality(), partial.MissingCardinality())
	}
	for rownum, want := range map[uint64]int64{101: 1001, 102: 1002, 103: 1003} {
		value, ok := partial.BSI.GetBigValue(rownum)
		if !ok || value.Int64() != want {
			t.Fatalf("merged cache value for %d = %v/%t, want %d", rownum, value, ok, want)
		}
	}
}

func TestDomainMappingCacheRetainsCoveredSubset(t *testing.T) {
	cache := NewDomainMappingCache()
	key := DomainMappingCacheKey{
		SourceDomain:  "l:lineitem",
		TargetDomain:  "o:orders",
		VectorIndex:   "lineitem",
		VectorField:   "l_orderkey",
		Direction:     "child_to_parent",
		FromTimeNanos: 10,
		ToTimeNanos:   20,
	}
	parentRows := []qsbridge.QuantaRownum{1, 2, 3}
	childRows := []qsbridge.QuantaRownum{101, 102, 103, 104}
	cache.Set(key, parentRows, childRows, map[qsbridge.QuantaRownum]qsbridge.QuantaRownum{
		101: 1,
		102: 2,
		103: 3,
	})

	exact, mode, ok := cache.Get(key, []qsbridge.QuantaRownum{3, 1, 2}, []qsbridge.QuantaRownum{104, 103, 102, 101})
	if !ok || mode != "exact" {
		t.Fatalf("exact cache lookup mode = %q/%t, want exact", mode, ok)
	}
	if exact[101] != 1 || exact[102] != 2 || exact[103] != 3 {
		t.Fatalf("exact mapping = %#v, want cached parent map", exact)
	}

	subset, mode, ok := cache.Get(key, []qsbridge.QuantaRownum{1, 3}, []qsbridge.QuantaRownum{103, 101})
	if !ok || mode != "retained_subset" {
		t.Fatalf("subset cache lookup mode = %q/%t, want retained_subset", mode, ok)
	}
	if len(subset) != 2 || subset[101] != 1 || subset[103] != 3 {
		t.Fatalf("subset mapping = %#v, want filtered child-parent map", subset)
	}

	parentFiltered, mode, ok := cache.Get(key, []qsbridge.QuantaRownum{1}, []qsbridge.QuantaRownum{103, 101})
	if !ok || mode != "retained_subset" {
		t.Fatalf("parent-filtered cache lookup mode = %q/%t, want retained_subset", mode, ok)
	}
	if len(parentFiltered) != 1 || parentFiltered[101] != 1 {
		t.Fatalf("parent-filtered mapping = %#v, want only parent 1 child", parentFiltered)
	}

	if _, _, ok := cache.Get(key, []qsbridge.QuantaRownum{1, 5}, []qsbridge.QuantaRownum{101}); ok {
		t.Fatal("cache should miss when requested parent set is not covered")
	}
	if _, _, ok := cache.Get(key, []qsbridge.QuantaRownum{1}, []qsbridge.QuantaRownum{101, 999}); ok {
		t.Fatal("cache should miss when requested child set is not covered")
	}
}

func TestRelationshipVectorProjectionCacheStoresProjectedFKBSIs(t *testing.T) {
	cache := NewRelationshipVectorProjectionCache()
	key := "lineitem\x00l_orderkey\x0010\x0020\x00all"
	bsi := roaring64.NewDefaultBSI()
	cache.Put(key, bsi)

	got, ok := cache.Get(key)
	if !ok || got != bsi {
		t.Fatalf("cache lookup = %#v/%t, want stored FK BSI", got, ok)
	}
	if _, ok := cache.Get("orders\x00o_custkey\x0010\x0020\x00all"); ok {
		t.Fatal("cache should distinguish relationship-vector projection keys")
	}
	if got, ok := NewLegacyDirectRelationshipVectorProjectionCache().Get(key); ok || got != nil {
		t.Fatalf("empty compatibility cache lookup = %#v/%t, want miss", got, ok)
	}
}
