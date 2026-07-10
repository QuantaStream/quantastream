package qsbridge

import (
	"container/list"
	"sort"
	"sync"
)

// This file contains a tiny lock-sharded LRU cache primitive for metadata caches.
// It is deliberately internal, non-generic, and dependency-free so higher-level
// cache wrappers own type safety, invalidation semantics, and copy behavior.

const shardedValueCacheShardCount = 256

// shardedValueCache spreads independent keys across many locks to reduce contention.
type shardedValueCache struct {
	shards [shardedValueCacheShardCount]shardedValueCacheShard
}

// shardedValueCacheShard owns one map and its lock.
type shardedValueCacheShard struct {
	mu         sync.RWMutex
	values     map[string]*list.Element
	order      *list.List
	maxEntries int
	hits       uint64
	misses     uint64
	evictions  uint64
}

// shardedValueCacheItem is one LRU-managed cache entry.
type shardedValueCacheItem struct {
	key   string
	value interface{}
}

// shardedValueCacheEntry is one copied key/value pair from a cache snapshot.
type shardedValueCacheEntry struct {
	Key   string
	Value interface{}
}

// shardedValueCacheStats reports cache occupancy and hit-rate counters.
type shardedValueCacheStats struct {
	Entries    int
	MaxEntries int
	Hits       uint64
	Misses     uint64
	Evictions  uint64
}

// HitRatio returns hits divided by all lookups.
func (s shardedValueCacheStats) HitRatio() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// newShardedValueCache initializes every shard map eagerly.
func newShardedValueCache() *shardedValueCache {
	return newShardedValueCacheWithMaxEntries(0)
}

// newShardedValueCacheWithMaxEntries initializes a cache with an optional hard entry limit.
func newShardedValueCacheWithMaxEntries(maxEntries int) *shardedValueCache {
	cache := &shardedValueCache{}
	for i := range cache.shards {
		cache.shards[i].values = make(map[string]*list.Element)
		cache.shards[i].order = list.New()
	}
	cache.SetMaxEntries(maxEntries)
	return cache
}

// Get returns a cached value from the shard selected by key.
func (c *shardedValueCache) Get(key string) (interface{}, bool) {
	shard := c.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	element, ok := shard.values[key]
	if !ok {
		shard.misses++
		return nil, false
	}
	shard.hits++
	shard.order.MoveToFront(element)
	item := element.Value.(shardedValueCacheItem)
	return item.value, true
}

// Set stores value in the shard selected by key.
func (c *shardedValueCache) Set(key string, value interface{}) {
	shard := c.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if element, ok := shard.values[key]; ok {
		element.Value = shardedValueCacheItem{key: key, value: value}
		shard.order.MoveToFront(element)
		return
	}
	if shard.maxEntries == 0 {
		return
	}
	element := shard.order.PushFront(shardedValueCacheItem{key: key, value: value})
	shard.values[key] = element
	shard.evictOverflow()
}

// Delete removes key from its selected shard.
func (c *shardedValueCache) Delete(key string) {
	shard := c.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.deleteLocked(key)
}

// Clear replaces each shard map independently.
func (c *shardedValueCache) Clear() {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		shard.values = make(map[string]*list.Element)
		shard.order = list.New()
		shard.mu.Unlock()
	}
}

// SetMaxEntries updates the cache-wide hard entry limit and evicts overflow.
func (c *shardedValueCache) SetMaxEntries(maxEntries int) {
	if c == nil {
		return
	}
	limits := shardedValueCacheLimits(maxEntries, len(c.shards))
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		shard.maxEntries = limits[i]
		shard.evictOverflow()
		shard.mu.Unlock()
	}
}

// Stats returns a point-in-time aggregate of occupancy and hit-rate counters.
func (c *shardedValueCache) Stats() shardedValueCacheStats {
	if c == nil {
		return shardedValueCacheStats{}
	}
	stats := shardedValueCacheStats{}
	unbounded := false
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.RLock()
		stats.Entries += len(shard.values)
		if shard.maxEntries < 0 {
			unbounded = true
		} else {
			stats.MaxEntries += shard.maxEntries
		}
		stats.Hits += shard.hits
		stats.Misses += shard.misses
		stats.Evictions += shard.evictions
		shard.mu.RUnlock()
	}
	if unbounded {
		stats.MaxEntries = -1
	}
	return stats
}

// Entries returns a point-in-time copy of all cached key/value pairs.
func (c *shardedValueCache) Entries() []shardedValueCacheEntry {
	if c == nil {
		return nil
	}
	entries := make([]shardedValueCacheEntry, 0)
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.RLock()
		for key, element := range shard.values {
			item := element.Value.(shardedValueCacheItem)
			entries = append(entries, shardedValueCacheEntry{Key: key, Value: item.value})
		}
		shard.mu.RUnlock()
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})
	return entries
}

// shard returns the lock and map responsible for key.
func (c *shardedValueCache) shard(key string) *shardedValueCacheShard {
	return &c.shards[fnv32aString(key)%uint32(len(c.shards))]
}

func (s *shardedValueCacheShard) evictOverflow() {
	for s.maxEntries >= 0 && len(s.values) > s.maxEntries {
		back := s.order.Back()
		if back == nil {
			return
		}
		item := back.Value.(shardedValueCacheItem)
		delete(s.values, item.key)
		s.order.Remove(back)
		s.evictions++
	}
}

func (s *shardedValueCacheShard) deleteLocked(key string) {
	element, ok := s.values[key]
	if !ok {
		return
	}
	delete(s.values, key)
	s.order.Remove(element)
}

func shardedValueCacheLimits(maxEntries int, shardCount int) []int {
	limits := make([]int, shardCount)
	if maxEntries <= 0 {
		for i := range limits {
			limits[i] = -1
		}
		return limits
	}
	base := maxEntries / shardCount
	remainder := maxEntries % shardCount
	for i := range limits {
		limits[i] = base
		if i < remainder {
			limits[i]++
		}
	}
	return limits
}

// fnv32aString hashes cache keys without importing hash/fnv on hot lookup paths.
func fnv32aString(value string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := uint32(offset32)
	for i := 0; i < len(value); i++ {
		hash ^= uint32(value[i])
		hash *= prime32
	}
	return hash
}
