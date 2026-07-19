package qsinabox

import (
	"context"
	"math/big"
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
	rownumSet := standardProjectionBitmap(request.Rownums)
	cache.set(key, rownumSet, bsi)

	if got, mode, ok := cache.get(key, rownumSet); !ok || mode != "exact" || got != bsi {
		t.Fatalf("cache lookup = %#v/%q/%t, want exact stored BSI", got, mode, ok)
	}
	fieldChanged := request
	fieldChanged.PhysicalField = "l_suppkey"
	if _, _, ok := cache.get(standardProjectionBSICacheKeyFor(fieldChanged, 10, 20), rownumSet); ok {
		t.Fatalf("cache should distinguish projected fields")
	}
	rownumsChanged := request
	rownumsChanged.Rownums = []qsbridge.QuantaRownum{101, 999, 103}
	if _, _, ok := cache.get(standardProjectionBSICacheKeyFor(rownumsChanged, 10, 20), standardProjectionBitmap(rownumsChanged.Rownums)); ok {
		t.Fatalf("cache should distinguish uncovered rownum request shape")
	}
	if _, _, ok := cache.get(standardProjectionBSICacheKeyFor(request, 10, 30), rownumSet); ok {
		t.Fatalf("cache should distinguish projection time windows")
	}
}

func TestStandardProjectionBSICacheTreatsRownumsAsASet(t *testing.T) {
	request := qsruntime.NativeProjectionBSIReadRequest{
		Index:         "lineitem",
		PhysicalField: "l_orderkey",
		Rownums:       []qsbridge.QuantaRownum{101, 102, 103},
	}
	key := standardProjectionBSICacheKeyFor(request, 10, 20)
	cache := NewStandardProjectionBSICache()
	bsi := roaring64.NewDefaultBSI()
	cache.set(key, standardProjectionBitmap(request.Rownums), bsi)

	reordered := standardProjectionBitmap([]qsbridge.QuantaRownum{103, 101, 102})
	if got, mode, ok := cache.get(key, reordered); !ok || mode != "exact" || got != bsi {
		t.Fatalf("reordered cache lookup = %#v/%q/%t, want exact stored BSI", got, mode, ok)
	}
}

func TestStandardProjectionBSICacheRetainsCoveredSubset(t *testing.T) {
	request := qsruntime.NativeProjectionBSIReadRequest{
		Index:         "lineitem",
		PhysicalField: "l_orderkey",
		Rownums:       []qsbridge.QuantaRownum{101, 102, 103},
	}
	key := standardProjectionBSICacheKeyFor(request, 10, 20)
	cache := NewStandardProjectionBSICache()
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(101, big.NewInt(1001))
	bsi.SetBigValue(102, big.NewInt(1002))
	bsi.SetBigValue(103, big.NewInt(1003))
	cache.set(key, standardProjectionBitmap(request.Rownums), bsi)

	subset := standardProjectionBitmap([]qsbridge.QuantaRownum{103, 101})
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
