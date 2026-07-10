package qsbridge

import "testing"

func TestShardedValueCacheGetSetDeleteAndClear(t *testing.T) {
	cache := newShardedValueCache()
	if _, ok := cache.Get("missing"); ok {
		t.Fatalf("did not expect missing key")
	}

	cache.Set("alpha", "one")
	value, ok := cache.Get("alpha")
	if !ok {
		t.Fatalf("expected cached alpha")
	}
	if value.(string) != "one" {
		t.Fatalf("value = %#v, want one", value)
	}

	cache.Delete("alpha")
	if _, ok := cache.Get("alpha"); ok {
		t.Fatalf("expected alpha to be deleted")
	}

	cache.Set("alpha", "one")
	cache.Set("beta", "two")
	cache.Clear()
	if _, ok := cache.Get("alpha"); ok {
		t.Fatalf("expected alpha to be cleared")
	}
	if _, ok := cache.Get("beta"); ok {
		t.Fatalf("expected beta to be cleared")
	}
}

func TestShardedValueCacheDistributesKeys(t *testing.T) {
	cache := newShardedValueCache()
	seen := make(map[*shardedValueCacheShard]struct{})
	for _, key := range []string{
		"lineitem.l_shipmode",
		"lineitem.l_returnflag",
		"part.p_type",
		"orders.o_orderpriority",
		"customer.c_mktsegment",
	} {
		seen[cache.shard(key)] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("expected sample keys to use more than one shard")
	}
}

func TestShardedValueCacheEntriesReturnsSortedSnapshot(t *testing.T) {
	cache := newShardedValueCache()
	cache.Set("gamma", "three")
	cache.Set("alpha", "one")
	cache.Set("beta", "two")

	entries := cache.Entries()
	if len(entries) != 3 {
		t.Fatalf("entries = %#v, want three entries", entries)
	}
	for i, want := range []string{"alpha", "beta", "gamma"} {
		if entries[i].Key != want {
			t.Fatalf("entries = %#v, want sorted keys", entries)
		}
	}

	cache.Delete("alpha")
	if entries[0].Key != "alpha" || entries[0].Value != "one" {
		t.Fatalf("snapshot changed after cache mutation: %#v", entries)
	}
}

func TestShardedValueCacheEvictsLeastRecentlyUsedEntry(t *testing.T) {
	cache := newShardedValueCacheWithMaxEntries(shardedValueCacheShardCount)
	first, second := sameShardKeys(t, cache)

	cache.Set(first, "one")
	if _, ok := cache.Get(first); !ok {
		t.Fatalf("expected first key before eviction")
	}
	cache.Set(second, "two")

	if _, ok := cache.Get(first); ok {
		t.Fatalf("expected least recently used key to be evicted")
	}
	if value, ok := cache.Get(second); !ok || value != "two" {
		t.Fatalf("second value = %#v/%v, want two/true", value, ok)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.MaxEntries != shardedValueCacheShardCount || stats.Evictions != 1 {
		t.Fatalf("stats = %#v, want one entry and one eviction", stats)
	}
	if stats.Hits != 2 || stats.Misses != 1 {
		t.Fatalf("stats = %#v, want hit/miss counters", stats)
	}
	if stats.HitRatio() <= 0 || stats.HitRatio() >= 1 {
		t.Fatalf("hit ratio = %v, want bounded ratio", stats.HitRatio())
	}
}

func TestShardedValueCacheGetRefreshesLRUOrder(t *testing.T) {
	cache := newShardedValueCacheWithMaxEntries(shardedValueCacheShardCount * 2)
	first, second, third := sameShardKeys3(t, cache)

	cache.Set(first, "one")
	cache.Set(second, "two")
	if _, ok := cache.Get(first); !ok {
		t.Fatalf("expected first key before refresh")
	}
	cache.Set(third, "three")

	if value, ok := cache.Get(first); !ok || value != "one" {
		t.Fatalf("first value = %#v/%v, want refreshed value", value, ok)
	}
	if _, ok := cache.Get(second); ok {
		t.Fatalf("expected second key to be evicted after first refresh")
	}
	if value, ok := cache.Get(third); !ok || value != "three" {
		t.Fatalf("third value = %#v/%v, want three/true", value, ok)
	}
}

func TestShardedValueCacheSetMaxEntriesEvictsOverflow(t *testing.T) {
	cache := newShardedValueCacheWithMaxEntries(shardedValueCacheShardCount * 2)
	first, second := sameShardKeys(t, cache)
	cache.Set(first, "one")
	cache.Set(second, "two")

	cache.SetMaxEntries(shardedValueCacheShardCount)
	if _, ok := cache.Get(first); ok {
		t.Fatalf("expected oldest key to be evicted after resize")
	}
	if value, ok := cache.Get(second); !ok || value != "two" {
		t.Fatalf("second value = %#v/%v, want retained value", value, ok)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.MaxEntries != shardedValueCacheShardCount || stats.Evictions != 1 {
		t.Fatalf("stats = %#v, want resized bounded cache", stats)
	}
}

func sameShardKeys(t *testing.T, cache *shardedValueCache) (string, string) {
	t.Helper()
	first, second, _ := sameShardKeys3(t, cache)
	return first, second
}

func sameShardKeys3(t *testing.T, cache *shardedValueCache) (string, string, string) {
	t.Helper()
	byShard := make(map[*shardedValueCacheShard][]string)
	for i := 0; i < 10000; i++ {
		key := string(rune('a'+i%26)) + "-" + string(rune('a'+(i/26)%26)) + "-" + string(rune('a'+(i/676)%26))
		shard := cache.shard(key)
		byShard[shard] = append(byShard[shard], key)
		if len(byShard[shard]) == 3 {
			return byShard[shard][0], byShard[shard][1], byShard[shard][2]
		}
	}
	t.Fatalf("could not find three keys for one shard")
	return "", "", ""
}
