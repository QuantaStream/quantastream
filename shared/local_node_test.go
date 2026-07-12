package shared

import "testing"

func TestLocalNodeServicesReadinessRequiresBitmapAndKV(t *testing.T) {
	readiness := (LocalNodeServices{}).Readiness()
	if readiness.Ready {
		t.Fatalf("empty local services reported ready")
	}
	if len(readiness.Blockers) != 2 {
		t.Fatalf("blockers = %v, want missing bitmap and kv blockers", readiness.Blockers)
	}
	if len(readiness.StreamingRisks) == 0 {
		t.Fatalf("streaming risks were not reported")
	}
}

func TestDefaultLocalNodeStreamingRisksNamesMutationAndLookupGates(t *testing.T) {
	risks := DefaultLocalNodeStreamingRisks()
	var sawBatchMutate, sawBatchLookup bool
	for _, risk := range risks {
		switch risk.Method {
		case "BatchMutate":
			sawBatchMutate = true
		case "BatchLookup":
			sawBatchLookup = true
		}
	}
	if !sawBatchMutate {
		t.Fatalf("BatchMutate risk missing from %+v", risks)
	}
	if !sawBatchLookup {
		t.Fatalf("BatchLookup risk missing from %+v", risks)
	}
}
