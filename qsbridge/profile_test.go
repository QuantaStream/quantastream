package qsbridge

import "testing"

func TestExecutionRequestProfileCarriesExplainWhenRequested(t *testing.T) {
	prepared := PreparedPlan{
		Supported: true,
		Inspection: InspectionReport{
			Logical: PlanExplanation{
				Nodes: []PlanNodeExplanation{{Summary: "scan(orders,fields=1)"}},
			},
			Physical: PhysicalPlanExplanation{
				Nodes: []PhysicalNodeExplanation{{Summary: "physical_scan(orders)"}},
			},
		},
	}
	request := prepared.ExecutionRequest(ExecutionOptions{
		RequestID:      "req-1",
		TraceExplain:   true,
		IncludeProfile: true,
	})

	profile := request.ExecutionProfile()
	if profile.Empty() {
		t.Fatalf("expected profile metadata")
	}
	if profile.RequestID != "req-1" || !profile.TraceExplain || !profile.IncludeProfile {
		t.Fatalf("profile = %#v, want request options", profile)
	}
	if profile.LogicalPlan != "scan(orders,fields=1)" || profile.PhysicalPlan != "physical_scan(orders)" {
		t.Fatalf("profile plans = %q/%q, want explain text", profile.LogicalPlan, profile.PhysicalPlan)
	}
}

func TestExecutionRequestProfileStaysEmptyWhenNotRequested(t *testing.T) {
	request := PreparedPlan{Supported: true}.ExecutionRequest(ExecutionOptions{})
	if !request.ExecutionProfile().Empty() {
		t.Fatalf("profile = %#v, want empty profile", request.ExecutionProfile())
	}
	if !request.EmptyResult().Profile.Empty() {
		t.Fatalf("result profile = %#v, want empty profile", request.EmptyResult().Profile)
	}
}

func TestExecutionResultWithProfileCopiesMutableMetadata(t *testing.T) {
	result := ExecutionResult{}.WithProfile(ExecutionProfile{
		RequestID: "req-1",
		Timings: []ExecutionTiming{{
			Name:    "scan",
			Elapsed: 10,
			Unit:    "ms",
		}},
		Counters: []ExecutionCounter{{
			Name:  "rows",
			Value: 2,
		}},
		Diagnostics: DiagnosticSet{{
			Code:   DiagnosticNativeBlocker,
			Fields: []FieldRef{{Name: "original"}},
		}},
	})
	result.Profile.Timings[0].Name = "mutated"
	result.Profile.Counters[0].Name = "mutated"
	result.Profile.Diagnostics[0].Fields[0].Name = "mutated"

	second := ExecutionResult{}.WithProfile(ExecutionProfile{
		Timings:     []ExecutionTiming{{Name: "scan"}},
		Counters:    []ExecutionCounter{{Name: "rows"}},
		Diagnostics: DiagnosticSet{{Fields: []FieldRef{{Name: "original"}}}},
	})
	if second.Profile.Timings[0].Name != "scan" || second.Profile.Counters[0].Name != "rows" {
		t.Fatalf("profile clone leaked timing/counter mutation: %#v", second.Profile)
	}
	if second.Profile.Diagnostics[0].Fields[0].Name != "original" {
		t.Fatalf("profile clone leaked diagnostic mutation: %#v", second.Profile.Diagnostics)
	}
}

func TestBatchExecutionRequestProfileUsesBatchOptions(t *testing.T) {
	request := BatchExecutionRequest{
		Options: ExecutionOptions{RequestID: "batch-1", IncludeProfile: true},
	}
	profile := request.ExecutionProfile()
	if profile.RequestID != "batch-1" || !profile.IncludeProfile {
		t.Fatalf("profile = %#v, want batch profile options", profile)
	}
}
