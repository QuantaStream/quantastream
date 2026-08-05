package core

import "testing"

func TestPrimaryKeyShadowProfileCountsMatchesAndExistingRows(t *testing.T) {
	profile := &PrimaryKeyShadowProfile{}

	profile.Observe(PrimaryKeyShadowComparison{
		AuthorityResult: PrimaryKeyResolveResult{ColumnID: 7, ExistingRow: true},
		ShadowResult:    PrimaryKeyResolveResult{ColumnID: 7, ExistingRow: true},
		Match:           true,
		Reason:          PrimaryKeyShadowMatchReason,
	})

	summary := profile.Snapshot()
	if summary.ComparisonCount != 1 || summary.MatchCount != 1 || summary.MismatchCount != 0 {
		t.Fatalf("summary = %+v, want one clean match", summary)
	}
	if summary.AuthorityExistingRow != 1 || summary.ShadowExistingRow != 1 || summary.ExistingRowMatch != 1 {
		t.Fatalf("summary = %+v, want existing-row agreement", summary)
	}
	if got := summary.ByReason[PrimaryKeyShadowMatchReason]; got != 1 {
		t.Fatalf("match reason count = %d, want 1", got)
	}
}

func TestPrimaryKeyShadowProfileCountsMismatchesAndCopiesReasonMap(t *testing.T) {
	profile := &PrimaryKeyShadowProfile{}

	profile.Observe(PrimaryKeyShadowComparison{
		TableName:       "orders",
		PrimaryKey:      "o_orderkey",
		LookupValue:     "101",
		AuthorityResult: PrimaryKeyResolveResult{ColumnID: 7},
		ShadowResult:    PrimaryKeyResolveResult{ColumnID: 8},
		Reason:          PrimaryKeyShadowColumnIDReason,
	})
	summary := profile.Snapshot()
	summary.ByReason[PrimaryKeyShadowColumnIDReason] = 99

	next := profile.Snapshot()
	if next.ComparisonCount != 1 || next.MismatchCount != 1 || next.MatchCount != 0 {
		t.Fatalf("summary = %+v, want one mismatch", next)
	}
	if next.FirstIssue == "" {
		t.Fatalf("summary = %+v, want first issue", next)
	}
	if got := next.ByReason[PrimaryKeyShadowColumnIDReason]; got != 1 {
		t.Fatalf("reason count = %d, want snapshot map copy", got)
	}
}

func TestPrimaryKeyShadowProfileCountsSkipsWithoutFirstIssue(t *testing.T) {
	profile := &PrimaryKeyShadowProfile{}

	profile.Observe(PrimaryKeyShadowComparison{
		Reason: PrimaryKeyShadowNoAuthorityColumnIDReason,
	})

	summary := profile.Snapshot()
	if summary.ComparisonCount != 1 || summary.SkipCount != 1 || summary.MismatchCount != 0 {
		t.Fatalf("summary = %+v, want one skip", summary)
	}
	if summary.FirstIssue != "" {
		t.Fatalf("first issue = %q, want empty for skipped validation", summary.FirstIssue)
	}
}
