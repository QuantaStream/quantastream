package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestBitmapQueryResultAliasesQSBridgeQuantaBitmapQueryResult(t *testing.T) {
	result := BitmapQueryResult{
		Rownums: []qsbridge.QuantaRownum{10, 11, 12},
	}
	var bridged qsbridge.QuantaBitmapQueryResult = result

	if got := bridged.CandidateCount(); got != 3 {
		t.Fatalf("candidate count = %d, want 3", got)
	}
}

func TestBitmapQueryResultAdapterCopiesRownums(t *testing.T) {
	bitmap := BitmapQueryResult{
		Success: true,
		Count:   2,
		Rownums: []qsbridge.QuantaRownum{1001, 1002},
	}

	result := BitmapQueryResultAdapter{}.ToExecutionResult(bitmap)
	bitmap.Rownums[0] = 9999
	if result.Count != 2 {
		t.Fatalf("count = %d, want 2", result.Count)
	}
	if result.RowSet.Rownums[0] != 1001 {
		t.Fatalf("rownum = %d, want 1001", result.RowSet.Rownums[0])
	}
}

func TestBitmapQueryResultAdapterReportsErrorMessage(t *testing.T) {
	result := BitmapQueryResultAdapter{}.ToExecutionResult(BitmapQueryResult{
		Success:      false,
		ErrorMessage: "bitmap query failed",
	})

	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected bitmap error diagnostics")
	}
	if got := result.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}
