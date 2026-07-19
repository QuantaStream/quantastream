package qsruntime

import (
	"context"
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type directProjectionBSICacheContextKey struct{}

type directProjectionBSICacheKey struct {
	Index           string
	Field           string
	FromTimeNanos   int64
	ToTimeNanos     int64
	FromEpochMillis int64
	ToEpochMillis   int64
}

type directProjectionBSICacheEntry struct {
	RownumSet *roaring64.Bitmap
	BSI       *roaring64.BSI
}

// DirectProjectionBSICache deduplicates read-only BSI projections within one
// inabox-direct SQL execution request.
type DirectProjectionBSICache struct {
	mu      sync.Mutex
	entries map[directProjectionBSICacheKey][]directProjectionBSICacheEntry
}

// NewDirectProjectionBSICache creates an empty per-query direct BSI projection cache.
func NewDirectProjectionBSICache() *DirectProjectionBSICache {
	return &DirectProjectionBSICache{
		entries: make(map[directProjectionBSICacheKey][]directProjectionBSICacheEntry),
	}
}

// WithDirectProjectionBSICache installs a request-scoped direct BSI projection cache.
func WithDirectProjectionBSICache(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if directProjectionBSICacheFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, directProjectionBSICacheContextKey{}, NewDirectProjectionBSICache())
}

func directProjectionBSICacheFromContext(ctx context.Context) *DirectProjectionBSICache {
	if ctx == nil {
		return nil
	}
	cache, _ := ctx.Value(directProjectionBSICacheContextKey{}).(*DirectProjectionBSICache)
	return cache
}

func (c *DirectProjectionBSICache) get(key directProjectionBSICacheKey, rownumSet *roaring64.Bitmap) (*roaring64.BSI, string, bool) {
	if c == nil {
		return nil, "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.entries[key] {
		if directProjectionRownumSetsEqual(entry.RownumSet, rownumSet) {
			return entry.BSI, "exact", true
		}
	}
	for _, entry := range c.entries[key] {
		if directProjectionRownumSetCovers(entry.RownumSet, rownumSet) {
			return entry.BSI.NewBSIRetainSet(rownumSet), "retained_subset", true
		}
	}
	return nil, "", false
}

func (c *DirectProjectionBSICache) set(key directProjectionBSICacheKey, rownumSet *roaring64.Bitmap, bsi *roaring64.BSI) {
	if c == nil || rownumSet == nil || bsi == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = append(c.entries[key], directProjectionBSICacheEntry{
		RownumSet: rownumSet.Clone(),
		BSI:       bsi,
	})
}

func directProjectionBSICacheKeyFor(request NativeProjectionBSIReadRequest, fromTime, toTime int64) directProjectionBSICacheKey {
	return directProjectionBSICacheKey{
		Index:           request.Index,
		Field:           request.PhysicalField,
		FromTimeNanos:   fromTime,
		ToTimeNanos:     toTime,
		FromEpochMillis: request.FromEpochMillis,
		ToEpochMillis:   request.ToEpochMillis,
	}
}

func directProjectionRownumSetsEqual(left, right *roaring64.Bitmap) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.GetCardinality() != right.GetCardinality() {
		return false
	}
	return left.Equals(right)
}

func directProjectionRownumSetCovers(container, subset *roaring64.Bitmap) bool {
	if subset == nil {
		return true
	}
	if container == nil {
		return subset.GetCardinality() == 0
	}
	overlap := subset.Clone()
	overlap.And(container)
	return overlap.GetCardinality() == subset.GetCardinality()
}
