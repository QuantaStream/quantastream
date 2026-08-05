package core

import "sync"

// PrimaryKeyShadowProfileSummary captures aggregate shadow resolver results.
type PrimaryKeyShadowProfileSummary struct {
	ComparisonCount      int            `json:"comparison_count"`
	MatchCount           int            `json:"match_count"`
	MismatchCount        int            `json:"mismatch_count"`
	SkipCount            int            `json:"skip_count"`
	AuthorityErrorCount  int            `json:"authority_error_count"`
	ShadowErrorCount     int            `json:"shadow_error_count"`
	AuthorityExistingRow int            `json:"authority_existing_row_count"`
	ShadowExistingRow    int            `json:"shadow_existing_row_count"`
	ExistingRowMatch     int            `json:"existing_row_match_count"`
	ByReason             map[string]int `json:"by_reason,omitempty"`
	FirstIssue           string         `json:"first_issue,omitempty"`
}

// PrimaryKeyShadowProfile records shadow resolver comparison events.
type PrimaryKeyShadowProfile struct {
	mu      sync.Mutex
	summary PrimaryKeyShadowProfileSummary
}

// Callback returns a shadow comparison observer backed by the profile.
func (p *PrimaryKeyShadowProfile) Callback() PrimaryKeyShadowObserver {
	if p == nil {
		return nil
	}
	return p.Observe
}

// Observe records one shadow resolver comparison.
func (p *PrimaryKeyShadowProfile) Observe(comparison PrimaryKeyShadowComparison) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.summary.ByReason == nil {
		p.summary.ByReason = map[string]int{}
	}
	p.summary.ComparisonCount++
	p.summary.ByReason[comparison.Reason]++
	if comparison.AuthorityResult.ExistingRow {
		p.summary.AuthorityExistingRow++
	}
	if comparison.ShadowResult.ExistingRow {
		p.summary.ShadowExistingRow++
	}
	if comparison.Match && comparison.AuthorityResult.ExistingRow && comparison.ShadowResult.ExistingRow {
		p.summary.ExistingRowMatch++
	}
	switch comparison.Reason {
	case PrimaryKeyShadowMatchReason:
		p.summary.MatchCount++
	case PrimaryKeyShadowAuthorityErrorReason:
		p.summary.AuthorityErrorCount++
	case PrimaryKeyShadowShadowErrorReason:
		p.summary.ShadowErrorCount++
	case PrimaryKeyShadowNoAuthorityColumnIDReason:
		p.summary.SkipCount++
	default:
		p.summary.MismatchCount++
	}
	if !comparison.Match && comparison.Reason != PrimaryKeyShadowNoAuthorityColumnIDReason && p.summary.FirstIssue == "" {
		p.summary.FirstIssue = comparison.String()
	}
}

// Snapshot returns a thread-safe copy of the profile counters.
func (p *PrimaryKeyShadowProfile) Snapshot() PrimaryKeyShadowProfileSummary {
	if p == nil {
		return PrimaryKeyShadowProfileSummary{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	summary := p.summary
	summary.ByReason = copyPrimaryKeyShadowProfileReasonCounts(p.summary.ByReason)
	return summary
}

func copyPrimaryKeyShadowProfileReasonCounts(counts map[string]int) map[string]int {
	if len(counts) == 0 {
		return nil
	}
	clone := make(map[string]int, len(counts))
	for reason, count := range counts {
		clone[reason] = count
	}
	return clone
}
