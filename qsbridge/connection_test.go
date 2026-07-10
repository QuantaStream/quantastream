package qsbridge

import "testing"

func TestNewConnectionRequestCarriesProtocolAuthAndCapabilities(t *testing.T) {
	protocol := NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	auth := AuthenticationRequest{
		User:          "moli",
		DefaultSchema: "quanta",
		Method:        AuthenticationMethodMySQLPassword,
		Attributes:    map[string]string{"client": "mysql"},
	}

	request := NewConnectionRequest(
		"session-1",
		protocol,
		auth,
		ClientCapabilityPreparedStatements,
		ClientCapabilitySessionTracking,
	)
	if request.SessionID != "session-1" || request.Authentication.SessionID != "session-1" {
		t.Fatalf("session ids = %q/%q, want session-1", request.SessionID, request.Authentication.SessionID)
	}
	if !request.Supports(ClientCapabilityPreparedStatements) || !request.Protocol.Supports(ProtocolCapabilityPreparedStatements) {
		t.Fatalf("request = %#v, want protocol and client capabilities", request)
	}
	if request.Attributes["client"] != "mysql" {
		t.Fatalf("attributes = %#v, want auth attributes copied", request.Attributes)
	}
}

func TestConnectionRequestAuthenticateCreatesConnectionContext(t *testing.T) {
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults),
		AuthenticationRequest{
			User:          "moli",
			DefaultSchema: "quanta",
			Attributes:    map[string]string{"program": "mysql"},
		},
		ClientCapabilitySessionTracking,
	)

	context := request.Authenticate(staticAuthenticator{
		principal: AuthenticationPrincipal{
			User:          "moli",
			Roles:         []RoleName{"reader"},
			DefaultSchema: "quanta",
			Attributes:    map[string]string{"sql_mode": "ansi"},
		},
	})
	if !context.Supported() {
		t.Fatalf("diagnostics = %#v, want supported connection", context.Diagnostics)
	}
	if context.Session.ID != "session-1" || context.Session.User != "moli" || context.Session.CurrentSchema != "quanta" {
		t.Fatalf("session = %#v, want authenticated session metadata", context.Session)
	}
	if !context.Session.HasRole("reader") || !context.Supports(ClientCapabilitySessionTracking) {
		t.Fatalf("context = %#v, want role and session tracking capability", context)
	}
	if context.Session.Variables["sql_mode"] != "ansi" {
		t.Fatalf("session variables = %#v, want principal attributes", context.Session.Variables)
	}
}

func TestConnectionRequestAuthenticateCarriesDeniedDiagnostics(t *testing.T) {
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql"),
		AuthenticationRequest{User: "moli"},
	)

	context := request.Authenticate(denyingAuthenticator{})
	if context.Supported() {
		t.Fatalf("expected denied connection context")
	}
	if context.Session.ID != "session-1" || context.Session.User != "" {
		t.Fatalf("session = %#v, want unauthenticated session id only", context.Session)
	}
	if !containsDiagnosticCode(context.Diagnostics.Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", context.Diagnostics)
	}
}

func TestConnectionRequestCopiesMutableMetadata(t *testing.T) {
	capabilities := []ClientCapability{ClientCapabilityPreparedStatements}
	auth := AuthenticationRequest{
		User:       "moli",
		Attributes: map[string]string{"client": "mysql"},
	}
	protocol := NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	request := NewConnectionRequest("session-1", protocol, auth, capabilities...)
	capabilities[0] = ClientCapabilityBatching
	auth.Attributes["client"] = "mutated"
	protocol.Capabilities[0] = ProtocolCapabilityBatchExecution

	clone := request.Clone()
	clone.Capabilities[0] = ClientCapabilityBatching
	clone.Authentication.Attributes["client"] = "clone-mutated"
	clone.Protocol.Capabilities[0] = ProtocolCapabilityBatchExecution

	if !request.Supports(ClientCapabilityPreparedStatements) || request.Supports(ClientCapabilityBatching) {
		t.Fatalf("request capabilities leaked mutation: %#v", request.Capabilities)
	}
	if request.Authentication.Attributes["client"] != "mysql" || request.Attributes["client"] != "mysql" {
		t.Fatalf("request attributes leaked mutation: %#v %#v", request.Authentication.Attributes, request.Attributes)
	}
	if !request.Protocol.Supports(ProtocolCapabilityPreparedStatements) || request.Protocol.Supports(ProtocolCapabilityBatchExecution) {
		t.Fatalf("protocol leaked mutation: %#v", request.Protocol.Capabilities)
	}
}

func TestConnectionRequestNegotiateCapabilitiesDefaultsToAdvertisedCapabilities(t *testing.T) {
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql"),
		AuthenticationRequest{User: "moli"},
		ClientCapabilityPreparedStatements,
		ClientCapabilitySessionTracking,
	)

	negotiation := request.NegotiateCapabilities(ConnectionCapabilityPolicy{})
	if !negotiation.Supported() {
		t.Fatalf("diagnostics = %#v, want supported default negotiation", negotiation.Diagnostics)
	}
	if len(negotiation.Accepted) != 2 || !negotiation.Accepted.Has(ClientCapabilityPreparedStatements) || !negotiation.Accepted.Has(ClientCapabilitySessionTracking) {
		t.Fatalf("accepted = %#v, want advertised capabilities", negotiation.Accepted)
	}
}

func TestConnectionRequestNegotiateCapabilitiesAppliesRequiredAndOptionalPolicy(t *testing.T) {
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql"),
		AuthenticationRequest{User: "moli"},
		ClientCapabilityTLS,
		ClientCapabilitySessionTracking,
	)

	negotiation := request.NegotiateCapabilities(ConnectionCapabilityPolicy{
		Required: ClientCapabilities{ClientCapabilityTLS},
		Optional: ClientCapabilities{ClientCapabilityCompression, ClientCapabilitySessionTracking},
	})
	if !negotiation.Supported() {
		t.Fatalf("diagnostics = %#v, want supported capability negotiation", negotiation.Diagnostics)
	}
	if len(negotiation.Accepted) != 2 || !negotiation.Accepted.Has(ClientCapabilityTLS) || !negotiation.Accepted.Has(ClientCapabilitySessionTracking) {
		t.Fatalf("accepted = %#v, want required tls plus optional session tracking", negotiation.Accepted)
	}
	if negotiation.Accepted.Has(ClientCapabilityCompression) {
		t.Fatalf("accepted = %#v, did not expect missing optional compression", negotiation.Accepted)
	}
}

func TestConnectionRequestNegotiateCapabilitiesRejectsMissingRequiredCapability(t *testing.T) {
	request := NewConnectionRequest(
		"session-1",
		NewProtocolProfile(ProtocolMySQL, "mysql"),
		AuthenticationRequest{User: "moli"},
		ClientCapabilitySessionTracking,
	)

	negotiation := request.NegotiateCapabilities(ConnectionCapabilityPolicy{
		Required: ClientCapabilities{ClientCapabilityTLS},
		Optional: ClientCapabilities{ClientCapabilitySessionTracking},
	})
	if negotiation.Supported() {
		t.Fatalf("expected missing required capability to reject negotiation")
	}
	if !containsDiagnosticCode(negotiation.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", negotiation.Diagnostics)
	}
	if !negotiation.Accepted.Has(ClientCapabilitySessionTracking) {
		t.Fatalf("accepted = %#v, want supported optional capability preserved", negotiation.Accepted)
	}
}

func TestConnectionContextPlanSessionReturnsCopy(t *testing.T) {
	context := ConnectionContext{
		Session: SessionContext{
			User:      "moli",
			Roles:     []RoleName{"reader"},
			Variables: map[string]string{"autocommit": "1"},
		},
	}

	session := context.PlanSession()
	session.Roles[0] = "writer"
	session.Variables["autocommit"] = "0"
	if context.Session.Roles[0] != "reader" || context.Session.Variables["autocommit"] != "1" {
		t.Fatalf("context session leaked mutation: %#v", context.Session)
	}
}

func TestConnectionContextPlanRequestUsesAuthenticatedSession(t *testing.T) {
	trace := NewOptimizationTrace()
	trace.Add(RewriteAdvisoryRecord(RewriteJoinReorder, "consider selective driver"))
	context := ConnectionContext{
		Session: SessionContext{
			ID:            "session-1",
			User:          "moli",
			CurrentSchema: "quanta",
			Roles:         []RoleName{"reader"},
		},
	}

	request := context.PlanRequest("select 1", ConnectionPlanOptions{
		DefaultSchema:  "fallback",
		CatalogVersion: "catalog-v1",
		Scope: PhysicalScope{
			Shards:    ShardSet{Shards: []ShardID{"s1"}},
			Replicas:  []ReplicaID{"r1"},
			Placement: PlacementLocal,
		},
		Optimization: trace,
	})
	if request.SQL != "select 1" || request.DefaultSchema != "quanta" || request.CatalogVersion != "catalog-v1" {
		t.Fatalf("request = %#v, want sql/schema/catalog metadata", request)
	}
	if request.Session.ID != "session-1" || request.Session.User != "moli" || !request.Session.HasRole("reader") {
		t.Fatalf("session = %#v, want authenticated connection session", request.Session)
	}
	if !request.Scope.Shards.Contains("s1") || request.Scope.Replicas[0] != "r1" || request.Scope.Placement != PlacementLocal {
		t.Fatalf("scope = %#v, want copied physical scope", request.Scope)
	}
	if len(request.Optimization.Advisories()) != 1 {
		t.Fatalf("optimization = %#v, want advisory copied", request.Optimization)
	}
}

func TestConnectionContextPlanRequestFallsBackToOptionSchema(t *testing.T) {
	context := ConnectionContext{
		Session: SessionContext{User: "moli"},
	}

	request := context.PlanRequest("select 1", ConnectionPlanOptions{DefaultSchema: "fallback"})
	if request.DefaultSchema != "fallback" {
		t.Fatalf("default schema = %q, want fallback", request.DefaultSchema)
	}
}

func TestConnectionContextPlanRequestCopiesMutableInputs(t *testing.T) {
	trace := NewOptimizationTrace()
	trace.Add(RewriteAdvisoryRecord(RewriteHiddenProjection, "hidden field"))
	context := ConnectionContext{
		Session: SessionContext{
			Roles:     []RoleName{"reader"},
			Variables: map[string]string{"autocommit": "1"},
		},
	}
	options := ConnectionPlanOptions{
		Scope: PhysicalScope{
			Shards:   ShardSet{Shards: []ShardID{"s1"}},
			Replicas: []ReplicaID{"r1"},
		},
		Optimization: trace,
	}

	request := context.PlanRequest("select 1", options)
	request.Session.Roles[0] = "writer"
	request.Session.Variables["autocommit"] = "0"
	request.Scope.Shards.Shards[0] = "s2"
	request.Scope.Replicas[0] = "r2"
	request.Optimization.Rewrites[0].Reason = "mutated"
	if context.Session.Roles[0] != "reader" || context.Session.Variables["autocommit"] != "1" {
		t.Fatalf("context session leaked mutation: %#v", context.Session)
	}
	if options.Scope.Shards.Shards[0] != "s1" || options.Scope.Replicas[0] != "r1" {
		t.Fatalf("options scope leaked mutation: %#v", options.Scope)
	}
	if trace.Rewrites[0].Reason != "hidden field" {
		t.Fatalf("trace leaked mutation: %#v", trace)
	}
}

type staticAuthenticator struct {
	principal AuthenticationPrincipal
}

func (a staticAuthenticator) Authenticate(request AuthenticationRequest) AuthenticationDecision {
	return request.Allow(a.principal)
}

type denyingAuthenticator struct{}

func (denyingAuthenticator) Authenticate(request AuthenticationRequest) AuthenticationDecision {
	return request.Deny("bad credentials")
}
