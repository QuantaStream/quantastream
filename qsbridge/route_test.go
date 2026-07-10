package qsbridge

import "testing"

func TestPreparedPlanRouteSelectsNativeWhenSupported(t *testing.T) {
	decision := PreparedPlan{Supported: true}.Route(CompatibilityRoutingPolicy())
	if decision.Kind != RouteNative || decision.Reason != RouteReasonNativeSupported || !decision.NativeEligible {
		t.Fatalf("decision = %#v, want native supported", decision)
	}
	if !decision.Supported() {
		t.Fatalf("expected route decision to be supported")
	}
}

func TestPreparedPlanRouteFallsBackWhenUnsupportedInCompatibilityMode(t *testing.T) {
	decision := PreparedPlan{
		Supported: false,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedPredicate, PhasePlan, "unsupported predicate"),
		},
	}.Route(CompatibilityRoutingPolicy())
	if decision.Kind != RouteLegacyFallback || decision.Reason != RouteReasonNativeUnsupported {
		t.Fatalf("decision = %#v, want legacy fallback for unsupported native plan", decision)
	}
	if !decision.Supported() {
		t.Fatalf("expected fallback decision to be supported")
	}
}

func TestPreparedPlanRouteRejectsWhenNativeOnly(t *testing.T) {
	decision := PreparedPlan{
		Supported: false,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedPredicate, PhasePlan, "unsupported predicate"),
		},
	}.Route(NativeOnlyRoutingPolicy())
	if decision.Kind != RouteRejected || decision.Reason != RouteReasonFallbackDisabled {
		t.Fatalf("decision = %#v, want rejected native-only route", decision)
	}
	if decision.Supported() {
		t.Fatalf("expected rejected decision to be unsupported")
	}
	if !containsDiagnosticCode(decision.Diagnostics.Codes(), DiagnosticRouteRejected) {
		t.Fatalf("diagnostics = %#v, want route rejected diagnostic", decision.Diagnostics.Codes())
	}
}

func TestPreparedPlanRouteCanForceLegacy(t *testing.T) {
	decision := PreparedPlan{Supported: true}.Route(LegacyOnlyRoutingPolicy())
	if decision.Kind != RouteLegacyFallback || decision.Reason != RouteReasonLegacyForced || decision.NativeEligible {
		t.Fatalf("decision = %#v, want forced legacy fallback", decision)
	}
}

func TestPreparedPlanRouteFallsBackWhenNativeRoutingDisabled(t *testing.T) {
	decision := PreparedPlan{Supported: true}.Route(RoutingPolicy{
		Mode:          RouteModeCompatibility,
		NativeRouting: NativeRouteDisabled,
	})
	if decision.Kind != RouteLegacyFallback || decision.Reason != RouteReasonNativeDisabled || decision.NativeEligible {
		t.Fatalf("decision = %#v, want disabled-native fallback", decision)
	}
}

func TestPreparedPlanRouteRejectsWhenNativeOnlyAndNativeRoutingDisabled(t *testing.T) {
	decision := PreparedPlan{Supported: true}.Route(RoutingPolicy{
		Mode:          RouteModeNativeOnly,
		NativeRouting: NativeRouteDisabled,
	})
	if decision.Kind != RouteRejected || decision.Reason != RouteReasonNativeDisabled {
		t.Fatalf("decision = %#v, want rejected disabled-native route", decision)
	}
	if !containsDiagnosticCode(decision.Diagnostics.Codes(), DiagnosticRouteRejected) {
		t.Fatalf("diagnostics = %#v, want route rejected", decision.Diagnostics.Codes())
	}
}

func TestExecutionRequestRouteSeparatesRequestInvalidReason(t *testing.T) {
	request := ExecutionRequest{
		Supported: false,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticParameterTypeMismatch, PhaseBind, "bad parameter"),
		},
	}
	decision := request.Route(CompatibilityRoutingPolicy())
	if decision.Kind != RouteLegacyFallback || decision.Reason != RouteReasonRequestInvalid {
		t.Fatalf("decision = %#v, want request-invalid fallback", decision)
	}
}

func TestPlanResultRouteClonesDiagnostics(t *testing.T) {
	result := PlanResult{
		Supported: false,
		Diagnostics: DiagnosticSet{{
			Code:   DiagnosticUnsupportedPredicate,
			Fields: []FieldRef{{Name: "original"}},
		}},
	}
	decision := result.Route(CompatibilityRoutingPolicy())
	decision.Diagnostics[0].Fields[0].Name = "mutated"
	if result.Diagnostics[0].Fields[0].Name != "original" {
		t.Fatalf("route decision leaked mutable diagnostics")
	}
}
