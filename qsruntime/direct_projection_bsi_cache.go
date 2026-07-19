package qsruntime

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sync"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type directProjectionBSICacheContextKey struct{}

type directProjectionBSICacheKey struct {
	Index           string
	Field           string
	FromTimeNanos   int64
	ToTimeNanos     int64
	RownumCount     int
	RownumDigest    uint64
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

func (c *DirectProjectionBSICache) get(key directProjectionBSICacheKey, rownumSet *roaring64.Bitmap) (*roaring64.BSI, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.entries[key] {
		if directProjectionRownumSetsEqual(entry.RownumSet, rownumSet) {
			return entry.BSI, true
		}
	}
	return nil, false
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
		RownumCount:     len(request.Rownums),
		RownumDigest:    directProjectionRownumDigest(request.Rownums),
		FromEpochMillis: request.FromEpochMillis,
		ToEpochMillis:   request.ToEpochMillis,
	}
}

func directProjectionRownumDigest(rownums []qsbridge.QuantaRownum) uint64 {
	var sum uint64
	var xor uint64
	for _, rownum := range rownums {
		hashed := directProjectionHashRownum(uint64(rownum))
		sum += hashed
		xor ^= hashed
	}
	return sum ^ (xor << 1)
}

func directProjectionHashRownum(rownum uint64) uint64 {
	hash := fnv.New64a()
	var buffer [8]byte
	binary.LittleEndian.PutUint64(buffer[:], rownum)
	_, _ = hash.Write(buffer[:])
	return hash.Sum64()
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
