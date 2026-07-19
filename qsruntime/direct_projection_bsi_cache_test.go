package qsruntime

import (
	"context"
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

	if got, ok := cache.get(key, rownumSet); !ok || got != bsi {
		t.Fatalf("cache lookup = %#v/%t, want stored BSI", got, ok)
	}
	reordered := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{103, 102, 101})
	if got, ok := cache.get(key, reordered); !ok || got != bsi {
		t.Fatalf("reordered cache lookup = %#v/%t, want stored BSI", got, ok)
	}
	fieldChanged := request
	fieldChanged.PhysicalField = "l_suppkey"
	if _, ok := cache.get(directProjectionBSICacheKeyFor(fieldChanged, 10, 20), rownumSet); ok {
		t.Fatal("cache should distinguish projected fields")
	}
	rownumsChanged := legacyDirectRelationshipBitmap([]qsbridge.QuantaRownum{101, 999, 103})
	if _, ok := cache.get(key, rownumsChanged); ok {
		t.Fatal("cache should verify exact rownum set before returning an entry")
	}
	if _, ok := cache.get(directProjectionBSICacheKeyFor(request, 10, 30), rownumSet); ok {
		t.Fatal("cache should distinguish projection windows")
	}
}
