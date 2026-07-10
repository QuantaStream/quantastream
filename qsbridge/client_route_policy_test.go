package qsbridge

import "testing"

func TestDefaultRoutePolicyProfilesDescribeRoutingModes(t *testing.T) {
	profiles := DefaultRoutePolicyProfiles()
	if len(profiles) != 5 {
		t.Fatalf("profiles = %#v, want five route policy profiles", profiles)
	}
	compatibility := routePolicyProfileByName(profiles, "compatibility")
	if compatibility == nil || !compatibility.Default || !compatibility.NativeAllowed ||
		!compatibility.FallbackAllowed || compatibility.RejectsUnsupported {
		t.Fatalf("compatibility = %#v, want default native-or-fallback policy", compatibility)
	}
	nativeOnly := routePolicyProfileByName(profiles, "native_only")
	if nativeOnly == nil || !nativeOnly.NativeAllowed || nativeOnly.FallbackAllowed ||
		!nativeOnly.RejectsUnsupported || !nativeOnly.NativeRoutingEnabled {
		t.Fatalf("native_only = %#v, want rejecting native-only policy", nativeOnly)
	}
	disabled := routePolicyProfileByName(profiles, "compatibility_native_disabled")
	if disabled == nil || disabled.NativeAllowed || !disabled.FallbackAllowed ||
		disabled.Policy.NativeRouting != NativeRouteDisabled {
		t.Fatalf("disabled = %#v, want fallback-only disabled-native policy", disabled)
	}
}

func TestRoutePolicyProfilesMatchRouteDecisions(t *testing.T) {
	supported := PreparedPlan{Supported: true}
	unsupported := PreparedPlan{
		Supported: false,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedPredicate, PhasePlan, "unsupported predicate"),
		},
	}
	for _, profile := range DefaultRoutePolicyProfiles() {
		decision := supported.Route(profile.Policy)
		if profile.NativeAllowed && profile.NativeRoutingEnabled && decision.Kind != RouteNative {
			t.Fatalf("profile = %#v decision = %#v, want native route for supported plan", profile, decision)
		}
		if !profile.NativeAllowed && decision.Kind == RouteNative {
			t.Fatalf("profile = %#v decision = %#v, did not expect native route", profile, decision)
		}
		unsupportedDecision := unsupported.Route(profile.Policy)
		if profile.RejectsUnsupported && unsupportedDecision.Kind != RouteRejected {
			t.Fatalf("profile = %#v decision = %#v, want rejected unsupported route", profile, unsupportedDecision)
		}
		if profile.FallbackAllowed && !profile.NativeAllowed && unsupportedDecision.Kind != RouteLegacyFallback {
			t.Fatalf("profile = %#v decision = %#v, want fallback route", profile, unsupportedDecision)
		}
	}
}

func TestPlanningServiceListClientRoutePoliciesReturnsProfiles(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientRoutePolicies(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported route policy metadata", exchange)
	}
	if len(exchange.Rows) != 5 {
		t.Fatalf("rows = %#v, want route policy profiles", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 9 || exchange.ResultSchema.Columns[0].Name != "Name" {
		t.Fatalf("schema = %#v, want route policy columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per route policy", exchange.Result)
	}
}

func TestPlanningServiceListClientRoutePoliciesReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ListClientRoutePolicies(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientRoutePoliciesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientRoutePolicies(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientRoutePolicies(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].Name == "mutated" {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Name" || again.ResultSchema.Columns[0].Name != "Name" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func routePolicyProfileByName(profiles []RoutePolicyProfile, name string) *RoutePolicyProfile {
	for i := range profiles {
		if profiles[i].Name == name {
			return &profiles[i]
		}
	}
	return nil
}
