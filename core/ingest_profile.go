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
	RecordCount           int                                   `json:"record_count"`
	ChildRowCount         int                                   `json:"child_row_count"`
	LogicalRowCount       int                                   `json:"logical_row_count"`
	InsertedCount         int                                   `json:"inserted_count"`
	ExistingCount         int                                   `json:"existing_count"`
	DuplicateCount        int                                   `json:"duplicate_count"`
	ConflictCount         int                                   `json:"conflict_count"`
	TotalElapsed          time.Duration                         `json:"total_elapsed_nanos"`
	SourceElapsed         time.Duration                         `json:"source_elapsed_nanos"`
	IdentityElapsed       time.Duration                         `json:"identity_elapsed_nanos"`
	ChildExpansionElapsed time.Duration                         `json:"child_expansion_elapsed_nanos"`
	ChildTraversalElapsed time.Duration                         `json:"child_traversal_elapsed_nanos"`
	RelationElapsed       time.Duration                         `json:"relation_elapsed_nanos"`
	AttributeElapsed      time.Duration                         `json:"attribute_elapsed_nanos"`
	PrimaryKey            PrimaryKeyResolveProfile              `json:"primary_key"`
	PrimaryKeyByTable     map[string]PrimaryKeyResolveProfile   `json:"primary_key_by_table,omitempty"`
	ByTable               map[string]RouterPutRowProfileCounter `json:"by_table,omitempty"`
	ByShard               map[string]RouterPutRowProfileCounter `json:"by_shard,omitempty"`
}

// RouterPutRowProfileCounter is a grouped count/timing accumulator.
type RouterPutRowProfileCounter struct {
	RecordCount     int                      `json:"record_count"`
	ChildRowCount   int                      `json:"child_row_count"`
	LogicalRowCount int                      `json:"logical_row_count"`
	TotalElapsed    time.Duration            `json:"total_elapsed_nanos"`
	PrimaryKey      PrimaryKeyResolveProfile `json:"primary_key"`
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
	childRows := result.ChildRowCount
	logicalRows := result.LogicalRowCount
	if logicalRows <= 0 {
		logicalRows = 1 + childRows
	}
	p.summary.ChildRowCount += childRows
	p.summary.LogicalRowCount += logicalRows
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
	p.summary.ChildExpansionElapsed += result.ChildExpansionElapsed
	p.summary.ChildTraversalElapsed += result.ChildTraversalElapsed
	p.summary.RelationElapsed += result.RelationElapsed
	p.summary.AttributeElapsed += result.AttributeElapsed
	p.summary.PrimaryKey = p.summary.PrimaryKey.add(result.PrimaryKey)
	if len(result.PrimaryKeyByTable) > 0 {
		p.summary.PrimaryKeyByTable = addPrimaryKeyResolveProfilesByTable(p.summary.PrimaryKeyByTable, result.PrimaryKeyByTable)
	} else {
		tableName := firstNonEmpty(result.TableName, record.TableName)
		if tableName != "" {
			p.summary.PrimaryKeyByTable = addPrimaryKeyResolveProfileForTable(p.summary.PrimaryKeyByTable, tableName, result.PrimaryKey)
		}
	}
	tableName := firstNonEmpty(result.TableName, record.TableName)
	if tableName != "" {
		p.summary.ByTable[tableName] = addRouterPutRowProfileCounter(p.summary.ByTable[tableName], result)
	}
	if shardID != "" {
		p.summary.ByShard[shardID] = addRouterPutRowProfileCounter(p.summary.ByShard[shardID], result)
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
	cp.PrimaryKeyByTable = copyPrimaryKeyResolveProfiles(s.PrimaryKeyByTable)
	cp.ByTable = copyRouterPutRowProfileCounters(s.ByTable)
	cp.ByShard = copyRouterPutRowProfileCounters(s.ByShard)
	return cp
}

func copyPrimaryKeyResolveProfiles(src map[string]PrimaryKeyResolveProfile) map[string]PrimaryKeyResolveProfile {
	if src == nil {
		return nil
	}
	dst := make(map[string]PrimaryKeyResolveProfile, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func addPrimaryKeyResolveProfilesByTable(dst map[string]PrimaryKeyResolveProfile, src map[string]PrimaryKeyResolveProfile) map[string]PrimaryKeyResolveProfile {
	for tableName, profile := range src {
		dst = addPrimaryKeyResolveProfileForTable(dst, tableName, profile)
	}
	return dst
}

func addPrimaryKeyResolveProfileForTable(dst map[string]PrimaryKeyResolveProfile, tableName string, profile PrimaryKeyResolveProfile) map[string]PrimaryKeyResolveProfile {
	if tableName == "" {
		return dst
	}
	if dst == nil {
		dst = map[string]PrimaryKeyResolveProfile{}
	}
	dst[tableName] = dst[tableName].add(profile)
	return dst
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

func addRouterPutRowProfileCounter(counter RouterPutRowProfileCounter, result PutRowResult) RouterPutRowProfileCounter {
	logicalRows := result.LogicalRowCount
	if logicalRows <= 0 {
		logicalRows = 1 + result.ChildRowCount
	}
	counter.RecordCount++
	counter.ChildRowCount += result.ChildRowCount
	counter.LogicalRowCount += logicalRows
	counter.TotalElapsed += result.TotalElapsed
	counter.PrimaryKey = counter.PrimaryKey.add(result.PrimaryKey)
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
	FlushCount                int                                  `json:"flush_count"`
	ErrorCount                int                                  `json:"error_count"`
	TotalElapsed              time.Duration                        `json:"total_elapsed_nanos"`
	PartitionStringElapsed    time.Duration                        `json:"partition_string_elapsed_nanos"`
	BitmapSetElapsed          time.Duration                        `json:"bitmap_set_elapsed_nanos"`
	BitmapClearElapsed        time.Duration                        `json:"bitmap_clear_elapsed_nanos"`
	BSIValueElapsed           time.Duration                        `json:"bsi_value_elapsed_nanos"`
	BSIClearValueElapsed      time.Duration                        `json:"bsi_clear_value_elapsed_nanos"`
	PartitionStringBatchCount int                                  `json:"partition_string_batch_count"`
	PartitionStringEntryCount int                                  `json:"partition_string_entry_count"`
	BitmapSetEntryCount       int                                  `json:"bitmap_set_entry_count"`
	BitmapClearEntryCount     int                                  `json:"bitmap_clear_entry_count"`
	BSIValueEntryCount        int                                  `json:"bsi_value_entry_count"`
	BSIClearValueEntryCount   int                                  `json:"bsi_clear_value_entry_count"`
	ByTable                   map[string]RouterFlushProfileCounter `json:"by_table,omitempty"`
	ByShard                   map[string]RouterFlushProfileCounter `json:"by_shard,omitempty"`
}

// RouterFlushProfileCounter is a grouped flush count/timing accumulator.
type RouterFlushProfileCounter struct {
	FlushCount   int           `json:"flush_count"`
	TotalElapsed time.Duration `json:"total_elapsed_nanos"`
	EntryCount   int           `json:"entry_count"`
	ErrorCount   int           `json:"error_count"`
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

// RouterDrainWorkerProfile records one SessionRouter worker's shutdown drain.
type RouterDrainWorkerProfile struct {
	ShardID      string        `json:"shard_id"`
	SessionCount int           `json:"session_count"`
	Elapsed      time.Duration `json:"elapsed_nanos"`
	Error        string        `json:"error,omitempty"`
}

// RouterDrainProfile aggregates worker shutdown drain observations.
type RouterDrainProfile struct {
	mu      sync.Mutex
	summary RouterDrainProfileSummary
}

// RouterDrainProfileSummary is a point-in-time aggregate of router drain work.
type RouterDrainProfileSummary struct {
	WorkerCount  int                                  `json:"worker_count"`
	SessionCount int                                  `json:"session_count"`
	ErrorCount   int                                  `json:"error_count"`
	TotalElapsed time.Duration                        `json:"total_elapsed_nanos"`
	MaxElapsed   time.Duration                        `json:"max_elapsed_nanos"`
	ByShard      map[string]RouterDrainProfileCounter `json:"by_shard,omitempty"`
}

// RouterDrainProfileCounter is a grouped worker drain accumulator.
type RouterDrainProfileCounter struct {
	WorkerCount  int           `json:"worker_count"`
	SessionCount int           `json:"session_count"`
	TotalElapsed time.Duration `json:"total_elapsed_nanos"`
	MaxElapsed   time.Duration `json:"max_elapsed_nanos"`
	ErrorCount   int           `json:"error_count"`
}

// Callback returns the function shape expected by SessionRouterConfig.
func (p *RouterDrainProfile) Callback() func(profile RouterDrainWorkerProfile) {
	return p.Observe
}

// Observe records one worker's shutdown drain profile.
func (p *RouterDrainProfile) Observe(profile RouterDrainWorkerProfile) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureMaps()
	p.summary.WorkerCount++
	p.summary.SessionCount += profile.SessionCount
	p.summary.TotalElapsed += profile.Elapsed
	if profile.Elapsed > p.summary.MaxElapsed {
		p.summary.MaxElapsed = profile.Elapsed
	}
	if profile.Error != "" {
		p.summary.ErrorCount++
	}
	if profile.ShardID != "" {
		p.summary.ByShard[profile.ShardID] = addRouterDrainProfileCounter(p.summary.ByShard[profile.ShardID], profile)
	}
}

// Snapshot returns a stable copy of the current drain profile summary.
func (p *RouterDrainProfile) Snapshot() RouterDrainProfileSummary {
	if p == nil {
		return RouterDrainProfileSummary{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.summary.copy()
}

func (p *RouterDrainProfile) ensureMaps() {
	if p.summary.ByShard == nil {
		p.summary.ByShard = map[string]RouterDrainProfileCounter{}
	}
}

func (s RouterDrainProfileSummary) copy() RouterDrainProfileSummary {
	cp := s
	cp.ByShard = copyRouterDrainProfileCounters(s.ByShard)
	return cp
}

func copyRouterDrainProfileCounters(src map[string]RouterDrainProfileCounter) map[string]RouterDrainProfileCounter {
	if src == nil {
		return nil
	}
	dst := make(map[string]RouterDrainProfileCounter, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func addRouterDrainProfileCounter(counter RouterDrainProfileCounter, profile RouterDrainWorkerProfile) RouterDrainProfileCounter {
	counter.WorkerCount++
	counter.SessionCount += profile.SessionCount
	counter.TotalElapsed += profile.Elapsed
	if profile.Elapsed > counter.MaxElapsed {
		counter.MaxElapsed = profile.Elapsed
	}
	if profile.Error != "" {
		counter.ErrorCount++
	}
	return counter
}
