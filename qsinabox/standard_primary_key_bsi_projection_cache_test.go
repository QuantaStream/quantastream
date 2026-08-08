package qsinabox

import (
	"math/big"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestStandardBSIProjectionCacheBuildsBigValueLookup(t *testing.T) {
	cache := NewStandardBSIProjectionCache()
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(101, big.NewInt(9001))
	bsi.SetBigValue(102, big.NewInt(9002))
	bsi.SetBigValue(103, big.NewInt(9001))

	lookup := cache.StoreBigValueLookup("lineitem", "__qs_pk_authority", 123, 123, bsi)
	if got := standardBSIBigValueLookupColumnIDs(lookup, big.NewInt(9001)); len(got) != 2 || got[0] != 101 || got[1] != 103 {
		t.Fatalf("lookup[9001] = %#v, want [101 103]", got)
	}
	if got := standardBSIBigValueLookupColumnIDs(lookup, big.NewInt(9002)); len(got) != 1 || got[0] != 102 {
		t.Fatalf("lookup[9002] = %#v, want [102]", got)
	}

	cached, ok := cache.LookupBigValue("lineitem", "__qs_pk_authority", 123, 123, big.NewInt(9001))
	if !ok {
		t.Fatalf("LookupBigValue() ok = false, want true")
	}
	if len(cached) != 2 || cached[0] != 101 || cached[1] != 103 {
		t.Fatalf("cached lookup[9001] = %#v, want [101 103]", cached)
	}
}

func TestStandardBSIProjectionCacheStageBigValueUpdatesLookup(t *testing.T) {
	cache := NewStandardBSIProjectionCache()
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(101, big.NewInt(9001))
	cache.Store("lineitem", "__qs_pk_authority", 123, 123, bsi)
	cache.StoreBigValueLookup("lineitem", "__qs_pk_authority", 123, 123, bsi)

	cache.StageBigValue("lineitem", "__qs_pk_authority", 123, 123, 102, big.NewInt(9002))
	cached, ok := cache.LookupBigValue("lineitem", "__qs_pk_authority", 123, 123, big.NewInt(9002))
	if !ok {
		t.Fatalf("LookupBigValue() ok = false, want true")
	}
	if len(cached) != 1 || cached[0] != 102 {
		t.Fatalf("cached staged lookup = %#v, want [102]", cached)
	}

	cache.StageBigValue("lineitem", "__qs_pk_authority", 123, 123, 102, big.NewInt(9002))
	cached, _ = cache.LookupBigValue("lineitem", "__qs_pk_authority", 123, 123, big.NewInt(9002))
	if len(cached) != 1 || cached[0] != 102 {
		t.Fatalf("cached duplicate staged lookup = %#v, want [102]", cached)
	}
}
