package qsbridge

import "testing"

func TestQuantaExecutionResultReportsCandidateCount(t *testing.T) {
	result := QuantaExecutionResult{
		RowSet: QuantaProjectedRowSet{
			Rownums: []QuantaRownum{1001, 1002},
		},
		Count: 2,
	}

	if got := result.CandidateCount(); got != 2 {
		t.Fatalf("candidate count = %d, want 2", got)
	}
}
