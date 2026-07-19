package qsruntime

import (
	"context"
	"math/big"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestWithDirectProjectionBSICacheInstallsOneRequestCache(t *testing.T) {
	ctx := WithDirectProjectionBSICache(context.Background())
	cache := directProjectionBSICacheFromContext(ctx)
	if cache == nil {
		t.Fatal("cache was not installed")
	}
	if again := WithDirectProjectionBSICache(ctx); again != ctx {
		t.Fatal("cache wrapper should preserve an existing request cache")
	}
}

func TestDirectProjectionBSICacheVerifiesRownumsOnDigestKey(t *testing.T) {
	request := NativeProjectionBSIReadRequest{
		Index:         "lineitem",
		PhysicalField: "l_orderkey",
		Rownums: []qsbridge.QuantaRownum{
			101,
			102,
			103,
		},
	}
	key := directProjectionBSICacheKeyFor(request, 10, 20)
	cache := NewDirectProjectionBSICache()
	bsi := roaring64.NewDefaultBSI()
	rownumSet := legacyDirectRelationshipBitmap(request.Rownums)
	cache.set(key, rownumSet, bsi)

	if got, mode, ok := cache.get(key, rownumSet); !ok || mode != "exact" || got != bsi {
		t.Fatalf("cache lookup = %#v/%q/%t, want exact stored BSI", got, mode, ok)
	}
	reordered := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{103, 102, 101})
	if got, mode, ok := cache.get(key, reordered); !ok || mode != "exact" || got != bsi {
		t.Fatalf("reordered cache lookup = %#v/%q/%t, want exact stored BSI", got, mode, ok)
	}
	fieldChanged := request
	fieldChanged.PhysicalField = "l_suppkey"
	if _, _, ok := cache.get(directProjectionBSICacheKeyFor(fieldChanged, 10, 20), rownumSet); ok {
		t.Fatal("cache should distinguish projected fields")
	}
	rownumsChanged := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{101, 999, 103})
	if _, _, ok := cache.get(key, rownumsChanged); ok {
		t.Fatal("cache should verify rownum coverage before returning an entry")
	}
	if _, _, ok := cache.get(directProjectionBSICacheKeyFor(request, 10, 30), rownumSet); ok {
		t.Fatal("cache should distinguish projection windows")
	}
}

func TestDirectProjectionBSICacheRetainsCoveredSubset(t *testing.T) {
	request := NativeProjectionBSIReadRequest{
		Index:         "lineitem",
		PhysicalField: "l_orderkey",
		Rownums:       []qsbridge.QuantaRownum{101, 102, 103},
	}
	key := directProjectionBSICacheKeyFor(request, 10, 20)
	cache := NewDirectProjectionBSICache()
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(101, big.NewInt(1001))
	bsi.SetBigValue(102, big.NewInt(1002))
	bsi.SetBigValue(103, big.NewInt(1003))
	cache.set(key, legacyDirectRelationshipBitmap(request.Rownums), bsi)

	subset := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{103, 101})
	got, mode, ok := cache.get(key, subset)
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
