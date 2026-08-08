package qsinabox

import (
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestStandardBSIProjectionCacheLoadOrProjectSharesInFlightProjection(t *testing.T) {
	cache := NewStandardBSIProjectionCache()
	var calls int64
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	project := func() (*roaring64.BSI, error) {
		atomic.AddInt64(&calls, 1)
		startOnce.Do(func() {
			close(started)
		})
		<-release
		bsi := roaring64.NewDefaultBSI()
		bsi.SetBigValue(101, big.NewInt(9001))
		return bsi, nil
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	worker := func() {
		defer wg.Done()
		bsi, cacheLookup, _, err := cache.LoadOrProject("lineitem", "__qs_pk_authority", 123, 123, project)
		if err != nil {
			errs <- err
			return
		}
		if !cacheLookup {
			errs <- errStandardBSIProjectionCacheTest("cacheLookup = false, want true")
			return
		}
		if bsi == nil {
			errs <- errStandardBSIProjectionCacheTest("bsi = nil")
		}
	}

	wg.Add(1)
	go worker()
	<-started
	wg.Add(1)
	go worker()
	time.Sleep(10 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("project calls = %d, want 1", got)
	}
}

type errStandardBSIProjectionCacheTest string

func (e errStandardBSIProjectionCacheTest) Error() string {
	return string(e)
}
