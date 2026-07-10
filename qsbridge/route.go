package qsbridge

// RouteMode describes how adapters should choose native planning vs legacy fallback.
type RouteMode string

const (
	// RouteModeCompatibility uses native when supported and otherwise falls back.
	RouteModeCompatibility RouteMode = ""
	// RouteModeNativeOnly rejects statements that cannot use the native scaffold.
	RouteModeNativeOnly RouteMode = "native_only"
	// RouteModeLegacyOnly always chooses the legacy path.
	RouteModeLegacyOnly RouteMode = "legacy_only"
)

// RouteKind is the adapter-facing outcome of a route decision.
type RouteKind string

const (
	// RouteNative means the adapter may pass the request to a native executor.
	RouteNative RouteKind = "native"
	// RouteLegacyFallback means the adapter should use the legacy runtime path.
	RouteLegacyFallback RouteKind = "legacy_fallback"
	// RouteRejected means policy does not allow either native execution or fallback.
	RouteRejected RouteKind = "rejected"
)

// RouteReason explains why a route was chosen.
type RouteReason string

const (
	// RouteReasonNativeSupported means native planning is supported.
	RouteReasonNativeSupported RouteReason = "native_supported"
	// RouteReasonLegacyForced means policy forced the legacy path.
	RouteReasonLegacyForced RouteReason = "legacy_forced"
	// RouteReasonNativeUnsupported means native planning produced blockers.
	RouteReasonNativeUnsupported RouteReason = "native_unsupported"
	// RouteReasonRequestInvalid means execute-time values or options were invalid.
	RouteReasonRequestInvalid RouteReason = "request_invalid"
	// RouteReasonFallbackDisabled means native failed and policy rejected fallback.
	RouteReasonFallbackDisabled RouteReason = "fallback_disabled"
	// RouteReasonNativeDisabled means native routing is disabled by feature gate.
	RouteReasonNativeDisabled RouteReason = "native_disabled"
)

// NativeRouteGate controls whether native execution may be selected.
type NativeRouteGate string

const (
	// NativeRouteDefault treats native routing as enabled.
	NativeRouteDefault NativeRouteGate = ""
	// NativeRouteEnabled explicitly allows native routing.
	NativeRouteEnabled NativeRouteGate = "enabled"
	// NativeRouteDisabled prevents native routing from being selected.
	NativeRouteDisabled NativeRouteGate = "disabled"
)

// RoutingPolicy controls native-vs-legacy route selection.
type RoutingPolicy struct {
	Mode          RouteMode
	NativeRouting NativeRouteGate
}

// RoutePolicyProfile describes one named routing policy for adapter metadata.
type RoutePolicyProfile struct {
	Name                 string
	Policy               RoutingPolicy
	Default              bool
	NativeAllowed        bool
	FallbackAllowed      bool
	RejectsUnsupported   bool
	NativeRoutingEnabled bool
	Detail               string
}

// RoutePolicySummary aggregates named routing policy profile capabilities.
type RoutePolicySummary struct {
	PolicyCount                int
	DefaultCount               int
	NativeAllowedCount         int
	FallbackAllowedCount       int
	RejectsUnsupportedCount    int
	NativeRoutingEnabledCount  int
	NativeRoutingDisabledCount int
}

// CompatibilityRoutingPolicy returns the normal compatibility-preserving policy.
func CompatibilityRoutingPolicy() RoutingPolicy {
	return RoutingPolicy{Mode: RouteModeCompatibility}
}

// NativeOnlyRoutingPolicy returns a policy that rejects unsupported native plans.
func NativeOnlyRoutingPolicy() RoutingPolicy {
	return RoutingPolicy{Mode: RouteModeNativeOnly}
}

// LegacyOnlyRoutingPolicy returns a policy that always selects legacy fallback.
func LegacyOnlyRoutingPolicy() RoutingPolicy {
	return RoutingPolicy{Mode: RouteModeLegacyOnly}
}

// DefaultRoutePolicyProfiles returns named route policy metadata.
func DefaultRoutePolicyProfiles() []RoutePolicyProfile {
	return cloneRoutePolicyProfiles(defaultRoutePolicyProfiles)
}

// DefaultRoutePolicySummary returns aggregate metadata for named route policies.
func DefaultRoutePolicySummary() RoutePolicySummary {
	return SummarizeRoutePolicyProfiles(DefaultRoutePolicyProfiles())
}

// SummarizeRoutePolicyProfiles aggregates named route policy profiles.
func SummarizeRoutePolicyProfiles(profiles []RoutePolicyProfile) RoutePolicySummary {
	summary := RoutePolicySummary{PolicyCount: len(profiles)}
	for _, profile := range profiles {
		if profile.Default {
			summary.DefaultCount++
		}
		if profile.NativeAllowed {
			summary.NativeAllowedCount++
		}
		if profile.FallbackAllowed {
			summary.FallbackAllowedCount++
		}
		if profile.RejectsUnsupported {
			summary.RejectsUnsupportedCount++
		}
		if profile.NativeRoutingEnabled {
			summary.NativeRoutingEnabledCount++
		}
		if profile.Policy.NativeRouting == NativeRouteDisabled {
			summary.NativeRoutingDisabledCount++
		}
	}
	return summary
}

// RouteDecision records an adapter-facing routing decision.
type RouteDecision struct {
	Kind           RouteKind
	Reason         RouteReason
	Diagnostics    DiagnosticSet
	NativeEligible bool
}

// Supported reports whether the decision can proceed through either route.
func (d RouteDecision) Supported() bool {
	return d.Kind == RouteNative || d.Kind == RouteLegacyFallback
}

// Route chooses native, fallback, or rejection for this planning result.
func (r PlanResult) Route(policy RoutingPolicy) RouteDecision {
	return routeFromDiagnostics(r.Supported, r.Diagnostics, policy, false)
}

// Route chooses native, fallback, or rejection for this prepared plan.
func (p PreparedPlan) Route(policy RoutingPolicy) RouteDecision {
	return routeFromDiagnostics(p.Supported, p.Diagnostics, policy, false)
}

// Route chooses native, fallback, or rejection for this execution request.
func (r ExecutionRequest) Route(policy RoutingPolicy) RouteDecision {
	return routeFromDiagnostics(r.SupportedForExecution(), r.Diagnostics, policy, true)
}

func routeFromDiagnostics(nativeSupported bool, diagnostics DiagnosticSet, policy RoutingPolicy, executionRequest bool) RouteDecision {
	diagnostics = cloneDiagnosticSet(diagnostics)
	if policy.Mode == RouteModeLegacyOnly {
		return RouteDecision{
			Kind:           RouteLegacyFallback,
			Reason:         RouteReasonLegacyForced,
			Diagnostics:    diagnostics,
			NativeEligible: false,
		}
	}
	if policy.NativeRouting == NativeRouteDisabled {
		if policy.Mode == RouteModeNativeOnly {
			diagnostics = append(diagnostics, ErrorDiagnostic(
				DiagnosticRouteRejected,
				PhasePlan,
				"native routing is disabled",
			))
			return RouteDecision{
				Kind:           RouteRejected,
				Reason:         RouteReasonNativeDisabled,
				Diagnostics:    diagnostics,
				NativeEligible: false,
			}
		}
		return RouteDecision{
			Kind:           RouteLegacyFallback,
			Reason:         RouteReasonNativeDisabled,
			Diagnostics:    diagnostics,
			NativeEligible: false,
		}
	}
	if nativeSupported && !diagnostics.BlocksNative() {
		return RouteDecision{
			Kind:           RouteNative,
			Reason:         RouteReasonNativeSupported,
			Diagnostics:    diagnostics,
			NativeEligible: true,
		}
	}
	reason := RouteReasonNativeUnsupported
	if executionRequest {
		reason = RouteReasonRequestInvalid
	}
	if policy.Mode == RouteModeNativeOnly {
		diagnostics = append(diagnostics, ErrorDiagnostic(
			DiagnosticRouteRejected,
			PhasePlan,
			"native routing required but native request is unsupported",
		))
		return RouteDecision{
			Kind:           RouteRejected,
			Reason:         RouteReasonFallbackDisabled,
			Diagnostics:    diagnostics,
			NativeEligible: false,
		}
	}
	return RouteDecision{
		Kind:           RouteLegacyFallback,
		Reason:         reason,
		Diagnostics:    diagnostics,
		NativeEligible: false,
	}
}

var defaultRoutePolicyProfiles = []RoutePolicyProfile{
	{
		Name:                 "compatibility",
		Policy:               CompatibilityRoutingPolicy(),
		Default:              true,
		NativeAllowed:        true,
		FallbackAllowed:      true,
		NativeRoutingEnabled: true,
		Detail:               "choose native when supported and otherwise preserve behavior through legacy fallback",
	},
	{
		Name:                 "native_only",
		Policy:               NativeOnlyRoutingPolicy(),
		NativeAllowed:        true,
		RejectsUnsupported:   true,
		NativeRoutingEnabled: true,
		Detail:               "reject statements that cannot use the native scaffold",
	},
	{
		Name:            "legacy_only",
		Policy:          LegacyOnlyRoutingPolicy(),
		FallbackAllowed: true,
		Detail:          "force legacy fallback even when native planning is supported",
	},
	{
		Name: "compatibility_native_disabled",
		Policy: RoutingPolicy{
			Mode:          RouteModeCompatibility,
			NativeRouting: NativeRouteDisabled,
		},
		FallbackAllowed: true,
		Detail:          "preserve behavior through legacy fallback while native routing is disabled",
	},
	{
		Name: "native_only_disabled",
		Policy: RoutingPolicy{
			Mode:          RouteModeNativeOnly,
			NativeRouting: NativeRouteDisabled,
		},
		RejectsUnsupported: true,
		Detail:             "reject all native-only requests while native routing is disabled",
	},
}

func cloneRoutePolicyProfiles(profiles []RoutePolicyProfile) []RoutePolicyProfile {
	return append([]RoutePolicyProfile(nil), profiles...)
}
