package qsbridge

import "testing"

func TestQuantaBitmapQueryResultReportsCandidateCount(t *testing.T) {
	result := QuantaBitmapQueryResult{
		Rownums: []QuantaRownum{10, 11, 12},
	}

	if got := result.CandidateCount(); got != 3 {
		t.Fatalf("candidate count = %d, want 3", got)
	}
}

func TestQuantaBitmapQueryResultCloneDoesNotAliasRownums(t *testing.T) {
	result := QuantaBitmapQueryResult{
		Rownums: []QuantaRownum{10},
	}

	cloned := result.Clone()
	result.Rownums[0] = 99
	if cloned.Rownums[0] != 10 {
		t.Fatalf("cloned rownum = %d, want 10", cloned.Rownums[0])
	}
}
