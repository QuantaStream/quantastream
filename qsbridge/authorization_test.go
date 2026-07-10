package qsbridge

import "testing"

func TestAuthorizationRequestCarriesSessionAndAccessMetadata(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders"}
	field := FieldRef{Table: orders, Name: "o_orderkey"}
	result := PlanResult{
		Session: SessionContext{User: "moli", Roles: []RoleName{"reader"}},
		Query: QueryIR{
			Kind:       QueryKindSelect,
			Sources:    []TableInstance{orders},
			Projection: []ProjectionColumn{{Expr: Field(field)}},
		},
	}

	request := result.AuthorizationRequest()
	if request.Session.User != "moli" {
		t.Fatalf("session = %#v, want copied user", request.Session)
	}
	if len(request.Requirements) != 1 || request.Requirements[0].Privilege != AccessSelect {
		t.Fatalf("requirements = %#v, want select access", request.Requirements)
	}
	request.Session.Roles[0] = "mutated"
	request.Requirements[0].Fields[0].Name = "mutated"
	second := result.AuthorizationRequest()
	if second.Session.Roles[0] != "reader" || second.Requirements[0].Fields[0].Name != "o_orderkey" {
		t.Fatalf("authorization request leaked mutable metadata")
	}
}

func TestAuthorizationRequestAuthorizeAllowsWhenNoPolicyOwner(t *testing.T) {
	request := AuthorizationRequest{
		Session: SessionContext{User: "moli"},
		Requirements: []AccessRequirement{{
			Privilege: AccessSelect,
			Table:     TableInstance{Table: "orders"},
		}},
	}

	decision := request.Authorize(nil)
	if !decision.Supported() {
		t.Fatalf("decision = %#v, want default allow without policy owner", decision)
	}
	decision.Requirements[0].Table.Table = "mutated"
	if request.Requirements[0].Table.Table != "orders" {
		t.Fatalf("allow decision leaked mutable requirement")
	}
}

func TestAuthorizationRequestDenyCreatesAccessDeniedDiagnostic(t *testing.T) {
	requirement := AccessRequirement{
		Privilege: AccessUpdate,
		Table:     TableInstance{Table: "orders"},
		Fields:    []FieldRef{{Name: "o_totalprice"}},
	}
	request := AuthorizationRequest{Requirements: []AccessRequirement{requirement}}

	decision := request.Deny(requirement, "update denied")
	if decision.Supported() {
		t.Fatalf("expected denied authorization decision")
	}
	if got := decision.Diagnostics.Codes()[0]; got != DiagnosticAccessDenied {
		t.Fatalf("diagnostic = %q, want access denied", got)
	}
	protocol, ok := decision.Diagnostics.FirstProtocolError()
	if !ok {
		t.Fatalf("expected protocol error")
	}
	if protocol.SQLState != SQLStateSyntaxError || protocol.VendorCode != mysqlErrorAccessDenied {
		t.Fatalf("protocol = %#v, want access denied mapping", protocol)
	}
}

func TestExecutionRequestsExposeAuthorizationRequest(t *testing.T) {
	prepared := PreparedPlan{
		Supported: true,
		Session:   SessionContext{User: "moli"},
		Access: []AccessRequirement{{
			Privilege: AccessSelect,
			Table:     TableInstance{Table: "orders"},
		}},
	}
	execution := prepared.ExecutionRequest(ExecutionOptions{})
	batch := prepared.BatchExecutionRequest(ExecutionOptions{}, ParameterValues())

	if len(execution.AuthorizationRequest().Requirements) != 1 {
		t.Fatalf("execution auth = %#v, want one requirement", execution.AuthorizationRequest())
	}
	if len(batch.AuthorizationRequest().Requirements) != 1 {
		t.Fatalf("batch auth = %#v, want one requirement", batch.AuthorizationRequest())
	}
}

type denyingAuthorizer struct{}

func (denyingAuthorizer) AuthorizeAccess(request AuthorizationRequest) AuthorizationDecision {
	return request.Deny(request.Requirements[0], "denied by test")
}

func TestAuthorizationRequestDelegatesToPolicyOwner(t *testing.T) {
	request := AuthorizationRequest{Requirements: []AccessRequirement{{
		Privilege: AccessSelect,
		Table:     TableInstance{Table: "orders"},
	}}}

	decision := request.Authorize(denyingAuthorizer{})
	if decision.Supported() {
		t.Fatalf("expected delegated deny decision")
	}
	if got := decision.Diagnostics.Codes()[0]; got != DiagnosticAccessDenied {
		t.Fatalf("diagnostic = %q, want access denied", got)
	}
}
