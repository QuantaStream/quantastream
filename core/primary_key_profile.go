package core

import "time"

// PrimaryKeyResolveProfile captures primary-key identity lookup work for one or
// more PutRow operations.
type PrimaryKeyResolveProfile struct {
	ResolveCount            int           `json:"resolve_count"`
	LookupRequiredCount     int           `json:"lookup_required_count"`
	LocalCacheLookupCount   int           `json:"local_cache_lookup_count"`
	LocalCacheHitCount      int           `json:"local_cache_hit_count"`
	KVLookupCount           int           `json:"kv_lookup_count"`
	KVHitCount              int           `json:"kv_hit_count"`
	RownumAllocationCount   int           `json:"rownum_allocation_count"`
	ProvidedColumnIDCount   int           `json:"provided_column_id_count"`
	DirectColumnIDCount     int           `json:"direct_column_id_count"`
	BatchCacheWriteCount    int           `json:"batch_cache_write_count"`
	TotalElapsed            time.Duration `json:"total_elapsed_nanos"`
	LocalCacheLookupElapsed time.Duration `json:"local_cache_lookup_elapsed_nanos"`
	KVLookupElapsed         time.Duration `json:"kv_lookup_elapsed_nanos"`
	RownumAllocationElapsed time.Duration `json:"rownum_allocation_elapsed_nanos"`
	BatchCacheWriteElapsed  time.Duration `json:"batch_cache_write_elapsed_nanos"`
}

func (p PrimaryKeyResolveProfile) add(other PrimaryKeyResolveProfile) PrimaryKeyResolveProfile {
	p.ResolveCount += other.ResolveCount
	p.LookupRequiredCount += other.LookupRequiredCount
	p.LocalCacheLookupCount += other.LocalCacheLookupCount
	p.LocalCacheHitCount += other.LocalCacheHitCount
	p.KVLookupCount += other.KVLookupCount
	p.KVHitCount += other.KVHitCount
	p.RownumAllocationCount += other.RownumAllocationCount
	p.ProvidedColumnIDCount += other.ProvidedColumnIDCount
	p.DirectColumnIDCount += other.DirectColumnIDCount
	p.BatchCacheWriteCount += other.BatchCacheWriteCount
	p.TotalElapsed += other.TotalElapsed
	p.LocalCacheLookupElapsed += other.LocalCacheLookupElapsed
	p.KVLookupElapsed += other.KVLookupElapsed
	p.RownumAllocationElapsed += other.RownumAllocationElapsed
	p.BatchCacheWriteElapsed += other.BatchCacheWriteElapsed
	return p
}
