package qsinabox

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestWithStandardProjectionBSICacheInstallsOneRequestCache(t *testing.T) {
	base := context.Background()
	ctx := WithStandardProjectionBSICache(base)
	cache := standardProjectionBSICacheFromContext(ctx)
	if cache == nil {
		t.Fatal("cache was not installed")
	}
	if again := WithStandardProjectionBSICache(ctx); again != ctx {
		t.Fatal("cache wrapper should preserve an existing request cache")
	}
}

func TestStandardProjectionBSICacheKeysProjectionShape(t *testing.T) {
	request := qsruntime.NativeProjectionBSIReadRequest{
		Index:         "lineitem",
		PhysicalField: "l_orderkey",
		Rownums: []qsbridge.QuantaRownum{
			101,
			102,
			103,
		},
	}
	key := standardProjectionBSICacheKeyFor(request, 10, 20)
	cache := NewStandardProjectionBSICache()
	bsi := roaring64.NewDefaultBSI()
	cache.set(key, bsi)

	if got, ok := cache.get(key); !ok || got != bsi {
		t.Fatalf("cache lookup = %#v/%t, want stored BSI", got, ok)
	}
	fieldChanged := request
	fieldChanged.PhysicalField = "l_suppkey"
	if _, ok := cache.get(standardProjectionBSICacheKeyFor(fieldChanged, 10, 20)); ok {
		t.Fatalf("cache should distinguish projected fields")
	}
	rownumsChanged := request
	rownumsChanged.Rownums = []qsbridge.QuantaRownum{103, 102, 101}
	if _, ok := cache.get(standardProjectionBSICacheKeyFor(rownumsChanged, 10, 20)); ok {
		t.Fatalf("cache should distinguish rownum request shape")
	}
	if _, ok := cache.get(standardProjectionBSICacheKeyFor(request, 10, 30)); ok {
		t.Fatalf("cache should distinguish projection time windows")
	}
}
