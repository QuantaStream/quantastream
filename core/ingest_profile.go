package core

import (
	"sync"
	"time"
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
