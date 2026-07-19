package qsruntime

import (
	"context"
	"math/big"
	"testing"

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
	if again := WithQueryScratchpad(ctx); again != ctx {
		t.Fatal("scratchpad wrapper should preserve an existing request cache")
	}
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

	if got, mode, ok := cache.Get(key, rownumSet); !ok || mode != "exact" || got != bsi {
		t.Fatalf("cache lookup = %#v/%q/%t, want exact stored BSI", got, mode, ok)
	}
	reordered := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{103, 102, 101})
	if got, mode, ok := cache.Get(key, reordered); !ok || mode != "exact" || got != bsi {
		t.Fatalf("reordered cache lookup = %#v/%q/%t, want exact stored BSI", got, mode, ok)
	}
	fieldChanged := request
	fieldChanged.PhysicalField = "l_suppkey"
	if _, _, ok := cache.Get(ProjectionBSICacheKeyFor(fieldChanged, 10, 20), rownumSet); ok {
		t.Fatal("cache should distinguish projected fields")
	}
	rownumsChanged := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{101, 999, 103})
	if _, _, ok := cache.Get(key, rownumsChanged); ok {
		t.Fatal("cache should verify rownum coverage before returning an entry")
	}
	if _, _, ok := cache.Get(ProjectionBSICacheKeyFor(request, 10, 30), rownumSet); ok {
		t.Fatal("cache should distinguish projection windows")
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
