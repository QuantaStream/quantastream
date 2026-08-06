package core

import "time"

// PrimaryKeyResolveProfile captures primary-key identity lookup work for one or
// more PutRow operations.
type PrimaryKeyResolveProfile struct {
	ResolveCount                  int            `json:"resolve_count"`
	LookupRequiredCount           int            `json:"lookup_required_count"`
	AssumeNewCount                int            `json:"assume_new_count"`
	LocalCacheLookupCount         int            `json:"local_cache_lookup_count"`
	LocalCacheHitCount            int            `json:"local_cache_hit_count"`
	SkippedLocalCacheLookupCount  int            `json:"skipped_local_cache_lookup_count"`
	BSILookupCount                int            `json:"bsi_lookup_count"`
	BSIHitCount                   int            `json:"bsi_hit_count"`
	SkippedBSILookupCount         int            `json:"skipped_bsi_lookup_count"`
	BSIFallbackCount              int            `json:"bsi_fallback_count"`
	BSIFallbackReasons            map[string]int `json:"bsi_fallback_reasons,omitempty"`
	BSIProjectionCacheLookupCount int            `json:"bsi_projection_cache_lookup_count"`
	BSIProjectionCacheHitCount    int            `json:"bsi_projection_cache_hit_count"`
	BSIStageWriteCount            int            `json:"bsi_stage_write_count"`
	RownumAllocationCount         int            `json:"rownum_allocation_count"`
	ProvidedColumnIDCount         int            `json:"provided_column_id_count"`
	DirectColumnIDCount           int            `json:"direct_column_id_count"`
	BatchCacheWriteCount          int            `json:"batch_cache_write_count"`
	TotalElapsed                  time.Duration  `json:"total_elapsed_nanos"`
	LocalCacheLookupElapsed       time.Duration  `json:"local_cache_lookup_elapsed_nanos"`
	BSIIdentityEncodeElapsed      time.Duration  `json:"bsi_identity_encode_elapsed_nanos"`
	BSIAuthorityEncodeElapsed     time.Duration  `json:"bsi_authority_encode_elapsed_nanos"`
	BSILookupElapsed              time.Duration  `json:"bsi_lookup_elapsed_nanos"`
	BSIProjectionElapsed          time.Duration  `json:"bsi_projection_elapsed_nanos"`
	BSICompareElapsed             time.Duration  `json:"bsi_compare_elapsed_nanos"`
	BSIMatchExtractionElapsed     time.Duration  `json:"bsi_match_extraction_elapsed_nanos"`
	BSIStageWriteElapsed          time.Duration  `json:"bsi_stage_write_elapsed_nanos"`
	RownumAllocationElapsed       time.Duration  `json:"rownum_allocation_elapsed_nanos"`
	BatchCacheWriteElapsed        time.Duration  `json:"batch_cache_write_elapsed_nanos"`
}

func (p PrimaryKeyResolveProfile) add(other PrimaryKeyResolveProfile) PrimaryKeyResolveProfile {
	p.ResolveCount += other.ResolveCount
	p.LookupRequiredCount += other.LookupRequiredCount
	p.AssumeNewCount += other.AssumeNewCount
	p.LocalCacheLookupCount += other.LocalCacheLookupCount
	p.LocalCacheHitCount += other.LocalCacheHitCount
	p.SkippedLocalCacheLookupCount += other.SkippedLocalCacheLookupCount
	p.BSILookupCount += other.BSILookupCount
	p.BSIHitCount += other.BSIHitCount
	p.SkippedBSILookupCount += other.SkippedBSILookupCount
	p.BSIFallbackCount += other.BSIFallbackCount
	p.BSIFallbackReasons = addPrimaryKeyResolveReasonCounts(p.BSIFallbackReasons, other.BSIFallbackReasons)
	p.BSIProjectionCacheLookupCount += other.BSIProjectionCacheLookupCount
	p.BSIProjectionCacheHitCount += other.BSIProjectionCacheHitCount
	p.BSIStageWriteCount += other.BSIStageWriteCount
	p.RownumAllocationCount += other.RownumAllocationCount
	p.ProvidedColumnIDCount += other.ProvidedColumnIDCount
	p.DirectColumnIDCount += other.DirectColumnIDCount
	p.BatchCacheWriteCount += other.BatchCacheWriteCount
	p.TotalElapsed += other.TotalElapsed
	p.LocalCacheLookupElapsed += other.LocalCacheLookupElapsed
	p.BSIIdentityEncodeElapsed += other.BSIIdentityEncodeElapsed
	p.BSIAuthorityEncodeElapsed += other.BSIAuthorityEncodeElapsed
	p.BSILookupElapsed += other.BSILookupElapsed
	p.BSIProjectionElapsed += other.BSIProjectionElapsed
	p.BSICompareElapsed += other.BSICompareElapsed
	p.BSIMatchExtractionElapsed += other.BSIMatchExtractionElapsed
	p.BSIStageWriteElapsed += other.BSIStageWriteElapsed
	p.RownumAllocationElapsed += other.RownumAllocationElapsed
	p.BatchCacheWriteElapsed += other.BatchCacheWriteElapsed
	return p
}

// RecordBSIFallback annotates profile output when a BSI-capable resolver has
// to delegate back to another authority path.
func (p *PrimaryKeyResolveProfile) RecordBSIFallback(reason string) {
	if p == nil {
		return
	}
	p.BSIFallbackCount++
	if reason == "" {
		reason = "unknown"
	}
	if p.BSIFallbackReasons == nil {
		p.BSIFallbackReasons = map[string]int{}
	}
	p.BSIFallbackReasons[reason]++
}

func addPrimaryKeyResolveReasonCounts(dst map[string]int, src map[string]int) map[string]int {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = map[string]int{}
	}
	for reason, count := range src {
		dst[reason] += count
	}
	return dst
}
