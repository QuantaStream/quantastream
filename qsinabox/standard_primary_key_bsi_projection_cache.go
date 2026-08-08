package qsinabox

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"golang.org/x/sync/singleflight"
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
	mu         sync.RWMutex
	entries    map[standardBSIProjectionCacheKey]*roaring64.BSI
	bigLookups map[standardBSIProjectionCacheKey]map[string][]uint64
	flights    singleflight.Group
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

func (c *StandardBSIProjectionCache) LoadOrProject(table, field string, fromTime, toTime int64,
	project func() (*roaring64.BSI, error)) (*roaring64.BSI, bool, bool, error) {

	if c == nil {
		bsi, err := project()
		return bsi, false, false, err
	}
	if bsi, ok := c.Lookup(table, field, fromTime, toTime); ok {
		return bsi, true, true, nil
	}
	key := standardBSIProjectionCacheKey{
		table:    table,
		field:    field,
		fromTime: fromTime,
		toTime:   toTime,
	}
	value, err, shared := c.flights.Do(key.flightKey(), func() (interface{}, error) {
		if bsi, ok := c.Lookup(table, field, fromTime, toTime); ok {
			return bsi, nil
		}
		bsi, err := project()
		if err != nil {
			return nil, err
		}
		return c.Store(table, field, fromTime, toTime, bsi), nil
	})
	if err != nil {
		return nil, true, false, err
	}
	bsi, _ := value.(*roaring64.BSI)
	return bsi, true, shared, nil
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
	if lookup := c.bigLookups[key]; lookup != nil {
		valueKey := standardBSIBigValueLookupKey(value)
		if !standardBSIColumnIDsContain(lookup[valueKey], columnID) {
			lookup[valueKey] = append(lookup[valueKey], columnID)
		}
	}
}

func (c *StandardBSIProjectionCache) LookupBigValue(table, field string, fromTime, toTime int64, value *big.Int) ([]uint64, bool) {
	if c == nil || value == nil {
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
	lookup, ok := c.bigLookups[key]
	if !ok {
		return nil, false
	}
	return append([]uint64(nil), lookup[standardBSIBigValueLookupKey(value)]...), true
}

func (c *StandardBSIProjectionCache) StoreBigValueLookup(table, field string, fromTime, toTime int64, bsi *roaring64.BSI) map[string][]uint64 {
	if c == nil {
		return nil
	}
	key := standardBSIProjectionCacheKey{
		table:    table,
		field:    field,
		fromTime: fromTime,
		toTime:   toTime,
	}
	c.mu.RLock()
	if existing := c.bigLookups[key]; existing != nil {
		c.mu.RUnlock()
		return existing
	}
	c.mu.RUnlock()
	lookup := standardBSIBigValueLookupFromBSI(bsi)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bigLookups == nil {
		c.bigLookups = map[standardBSIProjectionCacheKey]map[string][]uint64{}
	}
	if existing := c.bigLookups[key]; existing != nil {
		return existing
	}
	c.bigLookups[key] = lookup
	return lookup
}

func standardBSIBigValueLookupFromBSI(bsi *roaring64.BSI) map[string][]uint64 {
	lookup := map[string][]uint64{}
	if bsi == nil {
		return lookup
	}
	existence := bsi.GetExistenceBitmap()
	if existence == nil || existence.IsEmpty() {
		return lookup
	}
	columnIDs := make([]uint64, 0, existence.GetCardinality())
	it := existence.Iterator()
	for it.HasNext() {
		columnIDs = append(columnIDs, it.Next())
	}
	values := bsi.GetBigValues(columnIDs)
	for i, columnID := range columnIDs {
		if i >= len(values) || values[i] == nil {
			continue
		}
		valueKey := standardBSIBigValueLookupKey(values[i])
		lookup[valueKey] = append(lookup[valueKey], columnID)
	}
	return lookup
}

func (key standardBSIProjectionCacheKey) flightKey() string {
	return fmt.Sprintf("%s\x00%s\x00%d\x00%d", key.table, key.field, key.fromTime, key.toTime)
}

func standardBSIBigValueLookupKey(value *big.Int) string {
	if value == nil {
		return ""
	}
	return value.String()
}

func standardBSIColumnIDsContain(columnIDs []uint64, columnID uint64) bool {
	for _, existing := range columnIDs {
		if existing == columnID {
			return true
		}
	}
	return false
}
