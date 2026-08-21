package qsbridge

import "testing"

func TestAccessPolicyAllowsDirectUserGrant(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders"}
	policy := NewAccessPolicy(AccessGrant{
		PrincipalKind: AccessPrincipalUser,
		Principal:     "moli",
		Privilege:     AccessSelect,
		Table:         orders,
	})

	decision := AuthorizationRequest{
		Session: SessionContext{User: "moli"},
		Requirements: []AccessRequirement{{
			Privilege: AccessSelect,
			Table:     orders,
		}},
	}.Authorize(policy)
	if !decision.Supported() {
		t.Fatalf("decision = %#v, want allowed user grant", decision)
	}
}

func TestAccessPolicyAllowsRoleGrant(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders"}
	policy := NewAccessPolicy(AccessGrant{
		PrincipalKind: AccessPrincipalRole,
		Principal:     "reader",
		Privilege:     AccessSelect,
		Table:         orders,
	})

	decision := AuthorizationRequest{
		Session: SessionContext{User: "moli", Roles: []RoleName{"reader"}},
		Requirements: []AccessRequirement{{
			Privilege: AccessSelect,
			Table:     orders,
		}},
	}.Authorize(policy)
	if !decision.Supported() {
		t.Fatalf("decision = %#v, want allowed role grant", decision)
	}
}

func TestAccessPolicyMatchesQueryLocalTableIDByStableCatalogName(t *testing.T) {
	grantTable := TableInstance{Schema: "quanta", Table: "orders"}
	boundTable := TableInstance{ID: "orders_1", Schema: "quanta", Table: "orders"}
	policy := NewAccessPolicy(AccessGrant{
		PrincipalKind: AccessPrincipalRole,
		Principal:     "reader",
		Privilege:     AccessSelect,
		Table:         grantTable,
	})

	decision := AuthorizationRequest{
		Session: SessionContext{User: "moli", Roles: []RoleName{"reader"}},
		Requirements: []AccessRequirement{{
			Privilege: AccessSelect,
			Table:     boundTable,
		}},
	}.Authorize(policy)
	if !decision.Supported() {
		t.Fatalf("decision = %#v, want stable catalog-name grant to match query-local table id", decision)
	}
}

func TestAccessPolicyDeniesMissingGrant(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders"}
	policy := NewAccessPolicy()

	decision := AuthorizationRequest{
		Session: SessionContext{User: "moli"},
		Requirements: []AccessRequirement{{
			Privilege: AccessSelect,
			Table:     orders,
		}},
	}.Authorize(policy)
	if decision.Supported() {
		t.Fatalf("expected missing grant to deny")
	}
	if !containsDiagnosticCode(decision.Diagnostics.Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", decision.Diagnostics.Codes())
	}
}

func TestAccessPolicyChecksColumnScopedGrants(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders"}
	allowedField := FieldRef{Table: orders, Name: "o_orderkey"}
	deniedField := FieldRef{Table: orders, Name: "o_totalprice"}
	policy := NewAccessPolicy(AccessGrant{
		PrincipalKind: AccessPrincipalUser,
		Principal:     "moli",
		Privilege:     AccessSelect,
		Table:         orders,
		Fields:        []FieldRef{allowedField},
	})

	allowed := AuthorizationRequest{
		Session: SessionContext{User: "moli"},
		Requirements: []AccessRequirement{{
			Privilege: AccessSelect,
			Table:     orders,
			Fields:    []FieldRef{allowedField},
		}},
	}.Authorize(policy)
	if !allowed.Supported() {
		t.Fatalf("allowed = %#v, want column grant to pass", allowed)
	}

	denied := AuthorizationRequest{
		Session: SessionContext{User: "moli"},
		Requirements: []AccessRequirement{{
			Privilege: AccessSelect,
			Table:     orders,
			Fields:    []FieldRef{deniedField},
		}},
	}.Authorize(policy)
	if denied.Supported() {
		t.Fatalf("expected ungranted column to deny")
	}
}

func TestAccessPolicyRequiresMatchingPrivilegeAndTable(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders"}
	customer := TableInstance{ID: "customer", Schema: "quanta", Table: "customer"}
	policy := NewAccessPolicy(AccessGrant{
		PrincipalKind: AccessPrincipalUser,
		Principal:     "moli",
		Privilege:     AccessSelect,
		Table:         orders,
	})

	updateDecision := AuthorizationRequest{
		Session: SessionContext{User: "moli"},
		Requirements: []AccessRequirement{{
			Privilege: AccessUpdate,
			Table:     orders,
		}},
	}.Authorize(policy)
	if updateDecision.Supported() {
		t.Fatalf("expected mismatched privilege to deny")
	}

	tableDecision := AuthorizationRequest{
		Session: SessionContext{User: "moli"},
		Requirements: []AccessRequirement{{
			Privilege: AccessSelect,
			Table:     customer,
		}},
	}.Authorize(policy)
	if tableDecision.Supported() {
		t.Fatalf("expected mismatched table to deny")
	}
}

func TestAccessPolicyCopiesMutableGrants(t *testing.T) {
	orders := TableInstance{ID: "orders"}
	field := FieldRef{Table: orders, Name: "o_orderkey"}
	grant := AccessGrant{
		PrincipalKind: AccessPrincipalUser,
		Principal:     "moli",
		Privilege:     AccessSelect,
		Table:         orders,
		Fields:        []FieldRef{field},
	}
	policy := NewAccessPolicy(grant)
	grant.Fields[0].Name = "mutated"
	grants := policy.Grants()
	grants[0].Fields[0].Name = "mutated-again"

	second := policy.Grants()
	if second[0].Fields[0].Name != "o_orderkey" {
		t.Fatalf("policy leaked grant mutation: %#v", second)
	}
}
