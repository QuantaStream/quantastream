package shared

//
// Client side functions and API wrappers for bulk loading functions such as SetBit and
// SetValue for bitmap and BSI fields respectively, as well and backing strings and indices.
//

import (
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"math/big"
	"sync"
	"time"
)

// BatchBuffer - Buffer for batch operations.
//
// BitmapIndex - Wrapper around client bitmap indexing APIs
// KVStore - Handle to KVStore client for string store operations
// batchBits - Calls to SetBit are batched client side and send to server once full.
// batchValues - Calls to SetValue are batched client side (BSI fields)  and send to server once full.
// batchString - batch of primary key strings to ColumnID mappings to be inserted via KVStore.BatchPut
// batchSize - Number of total entries to hold client side for both batchBits and batchValues.
// batchCount - Current count of batch entries.
// batchStringCount - Current count of batch strings.
// batchMutex - Concurrency guard for batch state mutations.
type BatchBuffer struct {
	*BitmapIndex
	KVStore                 *KVStore
	batchSize               int
	batchSets               map[string]map[string]map[uint64]map[int64]*Bitmap
	batchClears             map[string]map[string]map[uint64]map[int64]*Bitmap
	batchValues             map[string]map[string]map[int64]*roaring64.BSI
	batchClearValues        map[string]map[string]map[int64]*roaring64.Bitmap
	batchPartitionStr       map[string]map[interface{}]interface{}
	batchPrimaryKeyIdentity map[string]uint64
	batchSetCount           int
	batchClearCount         int
	batchValueCount         int
	batchClearValueCount    int
	batchPartitionStrCount  int
	batchMutex              sync.RWMutex
	lastFlushProfile        BatchBufferFlushProfile
	ModifiedAt              time.Time
	FlushedAt               time.Time
}

// BatchBufferFlushProfile captures one completed Flush attempt.
type BatchBufferFlushProfile struct {
	StartedAt                       time.Time
	FinishedAt                      time.Time
	TotalElapsed                    time.Duration
	PartitionStringElapsed          time.Duration
	PartitionStringRoute            time.Duration
	PartitionStringBuild            time.Duration
	PartitionStringStream           time.Duration
	PartitionStringStreamOpen       time.Duration
	PartitionStringStreamSend       time.Duration
	PartitionStringStreamClose      time.Duration
	PartitionStringStreamMax        time.Duration
	BitmapSetElapsed                time.Duration
	BitmapClearElapsed              time.Duration
	BSIValueElapsed                 time.Duration
	BSIClearValueElapsed            time.Duration
	PartitionStringPutCalls         int
	PartitionStringBatchCount       int
	PartitionStringEntryCount       int
	PartitionStringRoutedEntryCount int
	BitmapSetEntryCount             int
	BitmapClearEntryCount           int
	BSIValueEntryCount              int
	BSIClearValueEntryCount         int
	Error                           string
}

// NewBatchBuffer - Initializer for client side API wrappers.
func NewBatchBuffer(bi *BitmapIndex, kv *KVStore, batchSize int) *BatchBuffer {

	c := &BatchBuffer{BitmapIndex: bi, KVStore: kv, batchSize: batchSize,
		ModifiedAt: time.Now(), FlushedAt: time.Now()}
	return c
}

// LastFlushProfile returns a copy of the last Flush profile.
func (c *BatchBuffer) LastFlushProfile() BatchBufferFlushProfile {
	if c == nil {
		return BatchBufferFlushProfile{}
	}
	c.batchMutex.RLock()
	defer c.batchMutex.RUnlock()
	return c.lastFlushProfile
}

type Bitmap struct {
	Bits     *roaring64.Bitmap
	IsUpdate bool
}

// NewBitmap - Create a new bitmap with update indicator.
func NewBitmap(bitmap *roaring64.Bitmap, isUpdate bool) *Bitmap {

	return &Bitmap{Bits: bitmap, IsUpdate: isUpdate}
}

// Flush outstanding batch before.
func (c *BatchBuffer) Flush() (err error) {

	//c.batchMutex.Lock()
	//defer c.batchMutex.Unlock()

	profile := BatchBufferFlushProfile{
		StartedAt:                 time.Now(),
		PartitionStringBatchCount: len(c.batchPartitionStr),
		PartitionStringEntryCount: c.batchPartitionStrCount,
		BitmapSetEntryCount:       c.batchSetCount,
		BitmapClearEntryCount:     c.batchClearCount,
		BSIValueEntryCount:        c.batchValueCount,
		BSIClearValueEntryCount:   c.batchClearValueCount,
	}
	defer func() {
		profile.FinishedAt = time.Now()
		profile.TotalElapsed = profile.FinishedAt.Sub(profile.StartedAt)
		if err != nil {
			profile.Error = err.Error()
		}
		c.batchMutex.Lock()
		c.lastFlushProfile = profile
		c.batchMutex.Unlock()
	}()

	c.FlushedAt = profile.StartedAt

	if c.batchPartitionStr != nil {
		phaseStart := time.Now()
		putCalls, kvProfile, err := c.KVStore.BatchPutRouted(c.batchPartitionStr, true)
		if err != nil {
			return err
		}
		profile.PartitionStringPutCalls = putCalls
		profile.PartitionStringRoute = kvProfile.RouteElapsed
		profile.PartitionStringBuild = kvProfile.BuildElapsed
		profile.PartitionStringStream = kvProfile.StreamElapsed
		profile.PartitionStringStreamOpen = kvProfile.StreamOpenElapsed
		profile.PartitionStringStreamSend = kvProfile.StreamSendElapsed
		profile.PartitionStringStreamClose = kvProfile.StreamCloseElapsed
		profile.PartitionStringStreamMax = kvProfile.StreamMaxElapsed
		profile.PartitionStringRoutedEntryCount = kvProfile.RoutedEntryCount
		profile.PartitionStringElapsed = time.Since(phaseStart)
		c.batchPartitionStr = nil
		c.batchPartitionStrCount = 0
	}

	if c.batchSets != nil {
		phaseStart := time.Now()
		if err := c.BatchMutate(c.batchSets, false); err != nil {
			return err
		}
		profile.BitmapSetElapsed = time.Since(phaseStart)
		c.batchSets = nil
		c.batchSetCount = 0
	}
	if c.batchClears != nil {
		phaseStart := time.Now()
		if err := c.BatchMutate(c.batchClears, true); err != nil {
			return err
		}
		profile.BitmapClearElapsed = time.Since(phaseStart)
		c.batchClears = nil
		c.batchClearCount = 0
	}
	if c.batchValues != nil {
		phaseStart := time.Now()
		if err := c.BatchSetValue(c.batchValues); err != nil {
			return err
		}
		profile.BSIValueElapsed = time.Since(phaseStart)
		c.batchValues = nil
		c.batchValueCount = 0
	}
	if c.batchClearValues != nil {
		phaseStart := time.Now()
		if err := c.BatchClearValue(c.batchClearValues); err != nil {
			return err
		}
		profile.BSIClearValueElapsed = time.Since(phaseStart)
		c.batchClearValues = nil
		c.batchClearValueCount = 0
	}
	c.batchPrimaryKeyIdentity = nil
	return nil
}

// IsEmpty - Return true is batch is empty
func (c *BatchBuffer) IsEmpty() bool {

	c.batchMutex.RLock()
	defer c.batchMutex.RUnlock()
	return c.batchSetCount == 0 && c.batchClearCount == 0 && c.batchValueCount == 0 &&
		c.batchPartitionStrCount == 0 && c.batchClearValueCount == 0
}

// BatchSetCount - Return batch set count
func (c *BatchBuffer) BatchSetCount() int {

	c.batchMutex.RLock()
	defer c.batchMutex.RUnlock()
	return c.batchSetCount
}

// MergeInto - Merge the contents of this batch into another.
func (c *BatchBuffer) MergeInto(to *BatchBuffer) {

	c.batchMutex.RLock()
	to.batchMutex.Lock()
	defer to.batchMutex.Unlock()
	defer c.batchMutex.RUnlock()

	for indexName, index := range c.batchSets {
		for fieldName, field := range index {
			for rowID, ts := range field {
				for t, bitmap := range ts {
					if to.batchSets == nil {
						to.batchSets = make(map[string]map[string]map[uint64]map[int64]*Bitmap)
					}
					if _, ok := to.batchSets[indexName]; !ok {
						to.batchSets[indexName] = make(map[string]map[uint64]map[int64]*Bitmap)
					}
					if _, ok := to.batchSets[indexName][fieldName]; !ok {
						to.batchSets[indexName][fieldName] = make(map[uint64]map[int64]*Bitmap)
					}
					if _, ok := to.batchSets[indexName][fieldName][rowID]; !ok {
						to.batchSets[indexName][fieldName][rowID] = make(map[int64]*Bitmap)
					}
					if bmap, ok := to.batchSets[indexName][fieldName][rowID][t]; !ok {
						to.batchSets[indexName][fieldName][rowID][t] = bitmap
					} else {
						to.batchSets[indexName][fieldName][rowID][t].Bits =
							roaring64.ParOr(0, bmap.Bits, bitmap.Bits)
					}
					to.batchSetCount += int(bitmap.Bits.GetCardinality())
				}
			}
		}
	}

	for indexName, index := range c.batchClears {
		for fieldName, field := range index {
			for rowID, ts := range field {
				for t, bitmap := range ts {
					if to.batchClears == nil {
						to.batchClears = make(map[string]map[string]map[uint64]map[int64]*Bitmap)
					}
					if _, ok := to.batchClears[indexName]; !ok {
						to.batchClears[indexName] = make(map[string]map[uint64]map[int64]*Bitmap)
					}
					if _, ok := to.batchClears[indexName][fieldName]; !ok {
						to.batchClears[indexName][fieldName] = make(map[uint64]map[int64]*Bitmap)
					}
					if _, ok := to.batchClears[indexName][fieldName][rowID]; !ok {
						to.batchClears[indexName][fieldName][rowID] = make(map[int64]*Bitmap)
					}
					if bmap, ok := to.batchClears[indexName][fieldName][rowID][t]; !ok {
						to.batchClears[indexName][fieldName][rowID][t] = bitmap
					} else {
						to.batchClears[indexName][fieldName][rowID][t].Bits =
							roaring64.ParOr(0, bmap.Bits, bitmap.Bits)
					}
					to.batchClearCount += int(bitmap.Bits.GetCardinality())
				}
			}
		}
	}

	for indexName, index := range c.batchValues {
		for fieldName, field := range index {
			for t, bsi := range field {
				if to.batchValues == nil {
					to.batchValues = make(map[string]map[string]map[int64]*roaring64.BSI)
				}
				if _, ok := to.batchValues[indexName]; !ok {
					to.batchValues[indexName] = make(map[string]map[int64]*roaring64.BSI)
				}
				if _, ok := to.batchValues[indexName][fieldName]; !ok {
					to.batchValues[indexName][fieldName] = make(map[int64]*roaring64.BSI)
				}
				if toBsi, ok := to.batchValues[indexName][fieldName][t]; !ok {
					to.batchValues[indexName][fieldName][t] = bsi
				} else {
					toBsi.ParOr(0, bsi)
				}
				to.batchValueCount += int(bsi.GetCardinality())
			}
		}
	}

	for indexName, index := range c.batchClearValues {
		for fieldName, field := range index {
			for t, ebm := range field {
				if to.batchClearValues == nil {
					to.batchClearValues = make(map[string]map[string]map[int64]*roaring64.Bitmap)
				}
				if _, ok := to.batchClearValues[indexName]; !ok {
					to.batchClearValues[indexName] = make(map[string]map[int64]*roaring64.Bitmap)
				}
				if _, ok := to.batchClearValues[indexName][fieldName]; !ok {
					to.batchClearValues[indexName][fieldName] = make(map[int64]*roaring64.Bitmap)
				}
				if toEbm, ok := to.batchClearValues[indexName][fieldName][t]; !ok {
					to.batchClearValues[indexName][fieldName][t] = ebm
				} else {
					roaring64.ParOr(0, toEbm, ebm)
				}
				to.batchClearValueCount += int(ebm.GetCardinality())
			}
		}
	}

	for indexPath, valueMap := range c.batchPartitionStr {
		if to.batchPartitionStr == nil {
			to.batchPartitionStr = make(map[string]map[interface{}]interface{})
		}
		if _, ok := to.batchPartitionStr[indexPath]; !ok {
			to.batchPartitionStr[indexPath] = make(map[interface{}]interface{})
		}
		for k, v := range valueMap {
			to.batchPartitionStr[indexPath][k] = v
			to.batchPartitionStrCount++
		}
	}
	if len(c.batchPrimaryKeyIdentity) > 0 {
		if to.batchPrimaryKeyIdentity == nil {
			to.batchPrimaryKeyIdentity = make(map[string]uint64, len(c.batchPrimaryKeyIdentity))
		}
		for identity, columnID := range c.batchPrimaryKeyIdentity {
			to.batchPrimaryKeyIdentity[identity] = columnID
		}
	}

	to.ModifiedAt = time.Now()
}

// SetBit - Set a bit in a "standard" bitmap.  Operations are batched.
func (c *BatchBuffer) SetBit(index, field string, columnID, rowID uint64, ts time.Time, isUpdate bool) error {

	c.batchMutex.Lock()
	defer c.batchMutex.Unlock()

	c.ModifiedAt = time.Now()

	if c.batchSets == nil {
		c.batchSets = make(map[string]map[string]map[uint64]map[int64]*Bitmap)
	}
	if _, ok := c.batchSets[index]; !ok {
		c.batchSets[index] = make(map[string]map[uint64]map[int64]*Bitmap)
	}
	if _, ok := c.batchSets[index][field]; !ok {
		c.batchSets[index][field] = make(map[uint64]map[int64]*Bitmap)
	}
	if _, ok := c.batchSets[index][field][rowID]; !ok {
		c.batchSets[index][field][rowID] = make(map[int64]*Bitmap)
	}
	if bmap, ok := c.batchSets[index][field][rowID][ts.UnixNano()]; !ok {
		b := NewBitmap(roaring64.BitmapOf(columnID), isUpdate)
		c.batchSets[index][field][rowID][ts.UnixNano()] = b
	} else {
		bmap.Bits.Add(columnID)
	}

	c.batchSetCount++

	if c.batchSetCount >= c.batchSize {

		if err := c.BatchMutate(c.batchSets, false); err != nil {
			return err
		}
		c.batchSets = nil
		c.batchSetCount = 0
	}
	return nil
}

// ClearBit - Clear a bit in a "standard" bitmap.  Operations are batched.
func (c *BatchBuffer) ClearBit(index, field string, columnID, rowID uint64, ts time.Time) error {

	c.batchMutex.Lock()
	defer c.batchMutex.Unlock()

	c.ModifiedAt = time.Now()

	if c.batchClears == nil {
		c.batchClears = make(map[string]map[string]map[uint64]map[int64]*Bitmap)
	}
	if _, ok := c.batchClears[index]; !ok {
		c.batchClears[index] = make(map[string]map[uint64]map[int64]*Bitmap)
	}
	if _, ok := c.batchClears[index][field]; !ok {
		c.batchClears[index][field] = make(map[uint64]map[int64]*Bitmap)
	}
	if _, ok := c.batchClears[index][field][rowID]; !ok {
		c.batchClears[index][field][rowID] = make(map[int64]*Bitmap)
	}
	if bmap, ok := c.batchClears[index][field][rowID][ts.UnixNano()]; !ok {
		b := NewBitmap(roaring64.BitmapOf(columnID), true)
		c.batchClears[index][field][rowID][ts.UnixNano()] = b
	} else {
		bmap.Bits.Add(columnID)
	}

	c.batchClearCount++

	if c.batchClearCount >= c.batchSize {

		if err := c.BatchMutate(c.batchClears, true); err != nil {
			return err
		}
		c.batchClears = nil
		c.batchClearCount = 0
	}
	return nil
}

// SetValue - Set a value in a BSI  Operations are batched.
func (c *BatchBuffer) SetValue(index, field string, columnID uint64, value *big.Int, ts time.Time) error {

	c.batchMutex.Lock()
	defer c.batchMutex.Unlock()
	c.ModifiedAt = time.Now()

	if c.batchValues == nil {
		c.batchValues = make(map[string]map[string]map[int64]*roaring64.BSI)
	}
	if _, ok := c.batchValues[index]; !ok {
		c.batchValues[index] = make(map[string]map[int64]*roaring64.BSI)
	}
	if _, ok := c.batchValues[index][field]; !ok {
		c.batchValues[index][field] = make(map[int64]*roaring64.BSI)
	}
	if bmap, ok := c.batchValues[index][field][ts.UnixNano()]; !ok {
		b := roaring64.NewDefaultBSI()
		b.SetBigValue(columnID, value)
		c.batchValues[index][field][ts.UnixNano()] = b
	} else {
		bmap.SetBigValue(columnID, value)
	}

	c.batchValueCount++

	if c.batchValueCount >= c.batchSize {

		if err := c.BatchSetValue(c.batchValues); err != nil {
			return err
		}
		c.batchValues = nil
		c.batchValueCount = 0
	}
	return nil
}

// ClearValue - Clear a value in a BSI  Operations are batched.
func (c *BatchBuffer) ClearValue(index, field string, columnID uint64, ts time.Time) error {

	c.batchMutex.Lock()
	defer c.batchMutex.Unlock()

	c.ModifiedAt = time.Now()

	if c.batchClearValues == nil {
		c.batchClearValues = make(map[string]map[string]map[int64]*roaring64.Bitmap)
	}
	if _, ok := c.batchClearValues[index]; !ok {
		c.batchClearValues[index] = make(map[string]map[int64]*roaring64.Bitmap)
	}
	if _, ok := c.batchClearValues[index][field]; !ok {
		c.batchClearValues[index][field] = make(map[int64]*roaring64.Bitmap)
	}
	if bmap, ok := c.batchClearValues[index][field][ts.UnixNano()]; !ok {
		b := roaring64.BitmapOf(columnID)
		c.batchClearValues[index][field][ts.UnixNano()] = b
	} else {
		bmap.Add(columnID)
	}

	c.batchClearValueCount++

	if c.batchClearValueCount >= c.batchSize {

		if err := c.BatchClearValue(c.batchClearValues); err != nil {
			return err
		}
		c.batchClearValues = nil
		c.batchClearValueCount = 0
	}
	return nil
}

// SetPartitionedString - Create column ID to backing string index entry.
func (c *BatchBuffer) SetPartitionedString(indexPath string, key, value interface{}) error {

	c.batchMutex.Lock()
	defer c.batchMutex.Unlock()

	c.ModifiedAt = time.Now()

	if c.batchPartitionStr == nil {
		c.batchPartitionStr = make(map[string]map[interface{}]interface{})
	}
	if _, ok := c.batchPartitionStr[indexPath]; !ok {
		c.batchPartitionStr[indexPath] = make(map[interface{}]interface{})
	}
	if _, ok := c.batchPartitionStr[indexPath][key]; !ok {
		c.batchPartitionStr[indexPath][key] = value
	}

	c.batchPartitionStrCount++

	if c.batchPartitionStrCount >= c.partitionedStringFlushThreshold() {
		if _, _, err := c.KVStore.BatchPutRouted(c.batchPartitionStr, true); err != nil {
			return err
		}
		c.batchPartitionStr = nil
		c.batchPartitionStrCount = 0
	}
	return nil
}

func (c *BatchBuffer) partitionedStringFlushThreshold() int {
	if c.batchSize <= 0 {
		return 1
	}
	return c.batchSize
}

// LookupLocalCIDForString - Lookup possible columnID in local batch cache
func (c *BatchBuffer) LookupLocalCIDForString(index, lookup string) (columnID uint64, ok bool) {

	c.batchMutex.RLock()
	defer c.batchMutex.RUnlock()

	var colIDVal interface{}
	colIDVal, ok = c.batchPartitionStr[index][lookup]
	if ok {
		columnID = colIDVal.(uint64)
	}
	return
}

// SetPrimaryKeyIdentity records an unflushed BSI primary-key identity to rownum
// mapping for same-batch duplicate detection.
func (c *BatchBuffer) SetPrimaryKeyIdentity(identity []byte, columnID uint64) {
	if c == nil || len(identity) == 0 || columnID == 0 {
		return
	}

	c.batchMutex.Lock()
	defer c.batchMutex.Unlock()

	c.ModifiedAt = time.Now()
	if c.batchPrimaryKeyIdentity == nil {
		c.batchPrimaryKeyIdentity = make(map[string]uint64)
	}
	if _, exists := c.batchPrimaryKeyIdentity[string(identity)]; !exists {
		c.batchPrimaryKeyIdentity[string(identity)] = columnID
	}
}

// LookupLocalCIDForPrimaryKeyIdentity returns a rownum for an unflushed BSI
// primary-key identity staged in this batch.
func (c *BatchBuffer) LookupLocalCIDForPrimaryKeyIdentity(identity []byte) (columnID uint64, ok bool) {
	if c == nil || len(identity) == 0 {
		return 0, false
	}

	c.batchMutex.RLock()
	defer c.batchMutex.RUnlock()

	columnID, ok = c.batchPrimaryKeyIdentity[string(identity)]
	return columnID, ok
}
