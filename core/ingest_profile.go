package core

import (
	"sync"
	"time"

	"github.com/QuantaStream/quantastream/shared"
)

// RouterPutRowProfile aggregates PutRowResult timings observed by a
// SessionRouter callback.
type RouterPutRowProfile struct {
	mu      sync.Mutex
	summary RouterPutRowProfileSummary
}

// RouterPutRowProfileSummary is a point-in-time load-path profile summary.
type RouterPutRowProfileSummary struct {
	RecordCount           int
	InsertedCount         int
	ExistingCount         int
	DuplicateCount        int
	ConflictCount         int
	TotalElapsed          time.Duration
	SourceElapsed         time.Duration
	IdentityElapsed       time.Duration
	AlternateKeysElapsed  time.Duration
	ChildExpansionElapsed time.Duration
	RelationElapsed       time.Duration
	AttributeElapsed      time.Duration
	ByTable               map[string]RouterPutRowProfileCounter
	ByShard               map[string]RouterPutRowProfileCounter
}

// RouterPutRowProfileCounter is a grouped count/timing accumulator.
type RouterPutRowProfileCounter struct {
	RecordCount  int
	TotalElapsed time.Duration
}

// Callback returns the function shape expected by SessionRouterConfig.
func (p *RouterPutRowProfile) Callback() func(shardID string, record IngestRecord, result PutRowResult) {
	return p.Observe
}

// Observe records one completed PutRowResult.
func (p *RouterPutRowProfile) Observe(shardID string, record IngestRecord, result PutRowResult) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureMaps()
	p.summary.RecordCount++
	if result.Inserted {
		p.summary.InsertedCount++
	}
	if result.ExistingRow {
		p.summary.ExistingCount++
	}
	if result.Duplicate {
		p.summary.DuplicateCount++
	}
	if result.Conflict {
		p.summary.ConflictCount++
	}
	p.summary.TotalElapsed += result.TotalElapsed
	p.summary.SourceElapsed += result.SourceElapsed
	p.summary.IdentityElapsed += result.IdentityElapsed
	p.summary.AlternateKeysElapsed += result.AlternateKeysElapsed
	p.summary.ChildExpansionElapsed += result.ChildExpansionElapsed
	p.summary.RelationElapsed += result.RelationElapsed
	p.summary.AttributeElapsed += result.AttributeElapsed
	tableName := firstNonEmpty(result.TableName, record.TableName)
	if tableName != "" {
		p.summary.ByTable[tableName] = addRouterPutRowProfileCounter(p.summary.ByTable[tableName], result.TotalElapsed)
	}
	if shardID != "" {
		p.summary.ByShard[shardID] = addRouterPutRowProfileCounter(p.summary.ByShard[shardID], result.TotalElapsed)
	}
}

// Snapshot returns a stable copy of the current profile summary.
func (p *RouterPutRowProfile) Snapshot() RouterPutRowProfileSummary {
	if p == nil {
		return RouterPutRowProfileSummary{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.summary.copy()
}

func (p *RouterPutRowProfile) ensureMaps() {
	if p.summary.ByTable == nil {
		p.summary.ByTable = map[string]RouterPutRowProfileCounter{}
	}
	if p.summary.ByShard == nil {
		p.summary.ByShard = map[string]RouterPutRowProfileCounter{}
	}
}

func (s RouterPutRowProfileSummary) copy() RouterPutRowProfileSummary {
	cp := s
	cp.ByTable = copyRouterPutRowProfileCounters(s.ByTable)
	cp.ByShard = copyRouterPutRowProfileCounters(s.ByShard)
	return cp
}

func copyRouterPutRowProfileCounters(src map[string]RouterPutRowProfileCounter) map[string]RouterPutRowProfileCounter {
	if src == nil {
		return nil
	}
	dst := make(map[string]RouterPutRowProfileCounter, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func addRouterPutRowProfileCounter(counter RouterPutRowProfileCounter, elapsed time.Duration) RouterPutRowProfileCounter {
	counter.RecordCount++
	counter.TotalElapsed += elapsed
	return counter
}

// RouterFlushProfile aggregates BatchBuffer flush timings observed by
// SessionRouter-owned sessions.
type RouterFlushProfile struct {
	mu      sync.Mutex
	summary RouterFlushProfileSummary
}

// RouterFlushProfileSummary is a point-in-time aggregate of session flush
// profiles.
type RouterFlushProfileSummary struct {
	FlushCount                int
	ErrorCount                int
	TotalElapsed              time.Duration
	PartitionStringElapsed    time.Duration
	BitmapSetElapsed          time.Duration
	BitmapClearElapsed        time.Duration
	BSIValueElapsed           time.Duration
	BSIClearValueElapsed      time.Duration
	PartitionStringBatchCount int
	PartitionStringEntryCount int
	BitmapSetEntryCount       int
	BitmapClearEntryCount     int
	BSIValueEntryCount        int
	BSIClearValueEntryCount   int
	ByTable                   map[string]RouterFlushProfileCounter
	ByShard                   map[string]RouterFlushProfileCounter
}

// RouterFlushProfileCounter is a grouped flush count/timing accumulator.
type RouterFlushProfileCounter struct {
	FlushCount   int
	TotalElapsed time.Duration
	EntryCount   int
	ErrorCount   int
}

// Callback returns the function shape expected by SessionRouterConfig.
func (p *RouterFlushProfile) Callback() func(shardID string, tableName string, profile shared.BatchBufferFlushProfile) {
	return p.Observe
}

// Observe records one completed BatchBuffer flush profile.
func (p *RouterFlushProfile) Observe(shardID string, tableName string, profile shared.BatchBufferFlushProfile) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureMaps()
	p.summary.FlushCount++
	if profile.Error != "" {
		p.summary.ErrorCount++
	}
	p.summary.TotalElapsed += profile.TotalElapsed
	p.summary.PartitionStringElapsed += profile.PartitionStringElapsed
	p.summary.BitmapSetElapsed += profile.BitmapSetElapsed
	p.summary.BitmapClearElapsed += profile.BitmapClearElapsed
	p.summary.BSIValueElapsed += profile.BSIValueElapsed
	p.summary.BSIClearValueElapsed += profile.BSIClearValueElapsed
	p.summary.PartitionStringBatchCount += profile.PartitionStringBatchCount
	p.summary.PartitionStringEntryCount += profile.PartitionStringEntryCount
	p.summary.BitmapSetEntryCount += profile.BitmapSetEntryCount
	p.summary.BitmapClearEntryCount += profile.BitmapClearEntryCount
	p.summary.BSIValueEntryCount += profile.BSIValueEntryCount
	p.summary.BSIClearValueEntryCount += profile.BSIClearValueEntryCount
	if tableName != "" {
		p.summary.ByTable[tableName] = addRouterFlushProfileCounter(p.summary.ByTable[tableName], profile)
	}
	if shardID != "" {
		p.summary.ByShard[shardID] = addRouterFlushProfileCounter(p.summary.ByShard[shardID], profile)
	}
}

// Snapshot returns a stable copy of the current flush profile summary.
func (p *RouterFlushProfile) Snapshot() RouterFlushProfileSummary {
	if p == nil {
		return RouterFlushProfileSummary{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.summary.copy()
}

func (p *RouterFlushProfile) ensureMaps() {
	if p.summary.ByTable == nil {
		p.summary.ByTable = map[string]RouterFlushProfileCounter{}
	}
	if p.summary.ByShard == nil {
		p.summary.ByShard = map[string]RouterFlushProfileCounter{}
	}
}

func (s RouterFlushProfileSummary) copy() RouterFlushProfileSummary {
	cp := s
	cp.ByTable = copyRouterFlushProfileCounters(s.ByTable)
	cp.ByShard = copyRouterFlushProfileCounters(s.ByShard)
	return cp
}

func copyRouterFlushProfileCounters(src map[string]RouterFlushProfileCounter) map[string]RouterFlushProfileCounter {
	if src == nil {
		return nil
	}
	dst := make(map[string]RouterFlushProfileCounter, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func addRouterFlushProfileCounter(counter RouterFlushProfileCounter, profile shared.BatchBufferFlushProfile) RouterFlushProfileCounter {
	counter.FlushCount++
	counter.TotalElapsed += profile.TotalElapsed
	counter.EntryCount += profile.PartitionStringEntryCount + profile.BitmapSetEntryCount + profile.BitmapClearEntryCount +
		profile.BSIValueEntryCount + profile.BSIClearValueEntryCount
	if profile.Error != "" {
		counter.ErrorCount++
	}
	return counter
}
