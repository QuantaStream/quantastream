package qsbridge

import "testing"

func TestAuthenticationRequestDelegatesToAuthenticator(t *testing.T) {
	request := AuthenticationRequest{
		SessionID:     "session-1",
		User:          "moli",
		DefaultSchema: "quanta",
		Method:        AuthenticationMethodMySQLPassword,
		Attributes: map[string]string{
			"client": "mysql",
		},
	}

	decision := request.Authenticate(allowingAuthenticator{})
	if !decision.Supported() {
		t.Fatalf("decision = %#v, want authenticated", decision)
	}
	if decision.Principal.User != "moli" || decision.Principal.DefaultSchema != "quanta" {
		t.Fatalf("principal = %#v, want request defaults", decision.Principal)
	}
	if len(decision.Principal.Roles) != 1 || decision.Principal.Roles[0] != "reader" {
		t.Fatalf("roles = %#v, want reader", decision.Principal.Roles)
	}
}

func TestAuthenticationRequestNilAuthenticatorDenies(t *testing.T) {
	decision := AuthenticationRequest{User: "moli"}.Authenticate(nil)
	if decision.Supported() {
		t.Fatalf("expected nil authenticator to deny")
	}
	if !containsDiagnosticCode(decision.Diagnostics.Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", decision.Diagnostics.Codes())
	}
	protocol, ok := decision.Diagnostics.FirstProtocolError()
	if !ok {
		t.Fatalf("expected protocol error")
	}
	if protocol.VendorCode != mysqlErrorAccessDenied {
		t.Fatalf("protocol = %#v, want access denied vendor code", protocol)
	}
}

func TestAuthenticationDecisionCreatesSessionContext(t *testing.T) {
	decision := AuthenticationRequest{
		User:          "moli",
		DefaultSchema: "quanta",
	}.Allow(AuthenticationPrincipal{
		Roles: []RoleName{"reader", "writer"},
		Attributes: map[string]string{
			"tenant": "qa",
		},
	})

	session := decision.SessionContext("session-1")
	if session.ID != "session-1" || session.User != "moli" || session.CurrentSchema != "quanta" {
		t.Fatalf("session = %#v, want authenticated session metadata", session)
	}
	if len(session.Roles) != 2 || session.Variables["tenant"] != "qa" {
		t.Fatalf("session roles/vars = %#v/%#v, want copied principal metadata", session.Roles, session.Variables)
	}
}

func TestAuthenticationMetadataCopiesMutableInputs(t *testing.T) {
	request := AuthenticationRequest{
		Attributes: map[string]string{"client": "mysql"},
	}
	cloned := request.Clone()
	cloned.Attributes["client"] = "mutated"
	if request.Attributes["client"] != "mysql" {
		t.Fatalf("request attributes were mutated: %#v", request.Attributes)
	}

	decision := AuthenticationRequest{User: "moli"}.Allow(AuthenticationPrincipal{
		Roles:      []RoleName{"reader"},
		Attributes: map[string]string{"tenant": "qa"},
	})
	decision.Principal.Roles[0] = "mutated"
	decision.Principal.Attributes["tenant"] = "mutated"
	second := AuthenticationRequest{User: "moli"}.Allow(AuthenticationPrincipal{
		Roles:      []RoleName{"reader"},
		Attributes: map[string]string{"tenant": "qa"},
	})
	if second.Principal.Roles[0] != "reader" || second.Principal.Attributes["tenant"] != "qa" {
		t.Fatalf("principal copy is unstable: %#v", second.Principal)
	}
}

func TestAuthenticationDeniedDecisionDoesNotCreatePrincipalSession(t *testing.T) {
	session := AuthenticationRequest{User: "moli"}.Deny("bad password").SessionContext("session-1")
	if session.ID != "session-1" {
		t.Fatalf("session id = %q, want session-1", session.ID)
	}
	if session.User != "" || session.CurrentSchema != "" {
		t.Fatalf("denied session = %#v, want only id metadata", session)
	}
}

type allowingAuthenticator struct{}

func (allowingAuthenticator) Authenticate(request AuthenticationRequest) AuthenticationDecision {
	return request.Allow(AuthenticationPrincipal{Roles: []RoleName{"reader"}})
}
