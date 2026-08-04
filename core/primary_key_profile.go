package core

import "time"

// PrimaryKeyResolveProfile captures primary-key identity lookup work for one or
// more PutRow operations.
type PrimaryKeyResolveProfile struct {
	ResolveCount                 int           `json:"resolve_count"`
	LookupRequiredCount          int           `json:"lookup_required_count"`
	AssumeNewCount               int           `json:"assume_new_count"`
	LocalCacheLookupCount        int           `json:"local_cache_lookup_count"`
	LocalCacheHitCount           int           `json:"local_cache_hit_count"`
	SkippedLocalCacheLookupCount int           `json:"skipped_local_cache_lookup_count"`
	KVLookupCount                int           `json:"kv_lookup_count"`
	KVHitCount                   int           `json:"kv_hit_count"`
	SkippedKVLookupCount         int           `json:"skipped_kv_lookup_count"`
	BSILookupCount               int           `json:"bsi_lookup_count"`
	BSIHitCount                  int           `json:"bsi_hit_count"`
	SkippedBSILookupCount        int           `json:"skipped_bsi_lookup_count"`
	BSIStageWriteCount           int           `json:"bsi_stage_write_count"`
	RownumAllocationCount        int           `json:"rownum_allocation_count"`
	ProvidedColumnIDCount        int           `json:"provided_column_id_count"`
	DirectColumnIDCount          int           `json:"direct_column_id_count"`
	BatchCacheWriteCount         int           `json:"batch_cache_write_count"`
	TotalElapsed                 time.Duration `json:"total_elapsed_nanos"`
	LocalCacheLookupElapsed      time.Duration `json:"local_cache_lookup_elapsed_nanos"`
	KVLookupElapsed              time.Duration `json:"kv_lookup_elapsed_nanos"`
	BSILookupElapsed             time.Duration `json:"bsi_lookup_elapsed_nanos"`
	BSIStageWriteElapsed         time.Duration `json:"bsi_stage_write_elapsed_nanos"`
	RownumAllocationElapsed      time.Duration `json:"rownum_allocation_elapsed_nanos"`
	BatchCacheWriteElapsed       time.Duration `json:"batch_cache_write_elapsed_nanos"`
}

func (p PrimaryKeyResolveProfile) add(other PrimaryKeyResolveProfile) PrimaryKeyResolveProfile {
	p.ResolveCount += other.ResolveCount
	p.LookupRequiredCount += other.LookupRequiredCount
	p.AssumeNewCount += other.AssumeNewCount
	p.LocalCacheLookupCount += other.LocalCacheLookupCount
	p.LocalCacheHitCount += other.LocalCacheHitCount
	p.SkippedLocalCacheLookupCount += other.SkippedLocalCacheLookupCount
	p.KVLookupCount += other.KVLookupCount
	p.KVHitCount += other.KVHitCount
	p.SkippedKVLookupCount += other.SkippedKVLookupCount
	p.BSILookupCount += other.BSILookupCount
	p.BSIHitCount += other.BSIHitCount
	p.SkippedBSILookupCount += other.SkippedBSILookupCount
	p.BSIStageWriteCount += other.BSIStageWriteCount
	p.RownumAllocationCount += other.RownumAllocationCount
	p.ProvidedColumnIDCount += other.ProvidedColumnIDCount
	p.DirectColumnIDCount += other.DirectColumnIDCount
	p.BatchCacheWriteCount += other.BatchCacheWriteCount
	p.TotalElapsed += other.TotalElapsed
	p.LocalCacheLookupElapsed += other.LocalCacheLookupElapsed
	p.KVLookupElapsed += other.KVLookupElapsed
	p.BSILookupElapsed += other.BSILookupElapsed
	p.BSIStageWriteElapsed += other.BSIStageWriteElapsed
	p.RownumAllocationElapsed += other.RownumAllocationElapsed
	p.BatchCacheWriteElapsed += other.BatchCacheWriteElapsed
	return p
}
