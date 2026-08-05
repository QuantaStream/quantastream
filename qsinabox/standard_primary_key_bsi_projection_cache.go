package qsinabox

import (
	"math/big"
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type standardBSIProjectionCacheKey struct {
	table    string
	field    string
	fromTime int64
	toTime   int64
}

// StandardBSIProjectionCache reuses projected BSI authority snapshots within a
// loader-owned session. Stage updates are mirrored into the cached BSI so the
// cache remains coherent after the batch-local identity cache is flushed.
type StandardBSIProjectionCache struct {
	mu      sync.RWMutex
	entries map[standardBSIProjectionCacheKey]*roaring64.BSI
}

// NewStandardBSIProjectionCache creates an empty per-session projection cache.
func NewStandardBSIProjectionCache() *StandardBSIProjectionCache {
	return &StandardBSIProjectionCache{}
}

func (c *StandardBSIProjectionCache) Lookup(table, field string, fromTime, toTime int64) (*roaring64.BSI, bool) {
	if c == nil {
		return nil, false
	}
	key := standardBSIProjectionCacheKey{
		table:    table,
		field:    field,
		fromTime: fromTime,
		toTime:   toTime,
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	bsi, ok := c.entries[key]
	return bsi, ok
}

func (c *StandardBSIProjectionCache) Store(table, field string, fromTime, toTime int64, bsi *roaring64.BSI) *roaring64.BSI {
	if c == nil {
		return bsi
	}
	if bsi == nil {
		bsi = roaring64.NewDefaultBSI()
	}
	key := standardBSIProjectionCacheKey{
		table:    table,
		field:    field,
		fromTime: fromTime,
		toTime:   toTime,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[standardBSIProjectionCacheKey]*roaring64.BSI{}
	}
	if existing := c.entries[key]; existing != nil {
		return existing
	}
	c.entries[key] = bsi
	return bsi
}

func (c *StandardBSIProjectionCache) StageBigValue(table, field string, fromTime, toTime int64, columnID uint64, value *big.Int) {
	if c == nil || columnID == 0 || value == nil {
		return
	}
	key := standardBSIProjectionCacheKey{
		table:    table,
		field:    field,
		fromTime: fromTime,
		toTime:   toTime,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	bsi := c.entries[key]
	if bsi == nil {
		return
	}
	bsi.SetBigValue(columnID, value)
}
