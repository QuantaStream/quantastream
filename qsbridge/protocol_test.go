package qsbridge

import "testing"

func TestProtocolProfileSupportsCapabilities(t *testing.T) {
	profile := NewProtocolProfile(
		ProtocolMySQL,
		"go-sql-driver",
		ProtocolCapabilityPreparedStatements,
		ProtocolCapabilityCancellation,
	)

	if !profile.Supports(ProtocolCapabilityPreparedStatements) {
		t.Fatalf("expected prepared statement capability")
	}
	if profile.Supports(ProtocolCapabilityBatchExecution) {
		t.Fatalf("did not expect batch capability")
	}
}

func TestProtocolNegotiationAllowsSupportedPreparedRequest(t *testing.T) {
	profile := NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityPreparedStatements,
		ProtocolCapabilityStreamingResults,
		ProtocolCapabilityForwardOnlyCursor,
		ProtocolCapabilityCancellation,
		ProtocolCapabilityExplain,
		ProtocolCapabilityStructuredExplain,
		ProtocolCapabilityPlanCachePolicy,
	)

	negotiation := profile.NegotiateExecution(ProtocolPreparedExecution, ExecutionOptions{
		RequestID:    "req-1",
		Streaming:    true,
		Cursor:       CursorForwardOnly,
		Cancelable:   true,
		TraceExplain: true,
	})
	if !negotiation.Supported() {
		t.Fatalf("diagnostics = %#v, want supported negotiation", negotiation.Diagnostics)
	}
	if negotiation.Mode != ProtocolPreparedExecution {
		t.Fatalf("mode = %q, want prepared", negotiation.Mode)
	}
	if !negotiation.Options.Streaming || !negotiation.Options.Cancelable {
		t.Fatalf("options = %#v, want requested options copied", negotiation.Options)
	}
	if !negotiation.Profile.Supports(ProtocolCapabilityStructuredExplain) ||
		!negotiation.Profile.Supports(ProtocolCapabilityPlanCachePolicy) {
		t.Fatalf("profile capabilities = %#v, want structured explain and cache policy metadata support", negotiation.Profile.Capabilities)
	}
}

func TestProtocolNegotiationRejectsUnsupportedMode(t *testing.T) {
	profile := NewProtocolProfile(ProtocolHTTP, "http")

	negotiation := profile.NegotiateExecution(ProtocolBatchExecution, ExecutionOptions{})
	if negotiation.Supported() {
		t.Fatalf("expected batch mode to be rejected")
	}
	if !containsDiagnosticCode(negotiation.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", negotiation.Diagnostics)
	}
}

func TestProtocolNegotiationRejectsUnsupportedOptions(t *testing.T) {
	profile := NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)

	negotiation := profile.NegotiateExecution(ProtocolPreparedExecution, ExecutionOptions{
		Streaming:      true,
		Cursor:         CursorForwardOnly,
		Cancelable:     true,
		TraceExplain:   true,
		IncludeProfile: true,
	})
	if negotiation.Supported() {
		t.Fatalf("expected unsupported options to reject negotiation")
	}
	if got := len(negotiation.Diagnostics.Codes()); got != 5 {
		t.Fatalf("diagnostics = %#v, want five unsupported option diagnostics", negotiation.Diagnostics)
	}
}

func TestProtocolNegotiationCarriesExecutionOptionDiagnostics(t *testing.T) {
	profile := NewProtocolProfile(ProtocolGo, "native", ProtocolCapabilityStreamingResults)

	negotiation := profile.NegotiateExecution("", ExecutionOptions{MaxRows: -1})
	if negotiation.Supported() {
		t.Fatalf("expected invalid execution option to reject negotiation")
	}
	if negotiation.Mode != ProtocolSimpleExecution {
		t.Fatalf("mode = %q, want default simple mode", negotiation.Mode)
	}
	if !containsDiagnosticCode(negotiation.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", negotiation.Diagnostics)
	}
}

func TestProtocolProfileCopiesMutableCapabilities(t *testing.T) {
	capabilities := []ProtocolCapability{ProtocolCapabilityCancellation}
	profile := NewProtocolProfile(ProtocolMySQL, "mysql", capabilities...)
	capabilities[0] = ProtocolCapabilityBatchExecution
	clone := profile.Clone()
	clone.Capabilities[0] = ProtocolCapabilityBatchExecution

	if !profile.Supports(ProtocolCapabilityCancellation) {
		t.Fatalf("profile leaked capability mutation: %#v", profile.Capabilities)
	}
	if profile.Supports(ProtocolCapabilityBatchExecution) {
		t.Fatalf("profile picked up clone mutation: %#v", profile.Capabilities)
	}
}
