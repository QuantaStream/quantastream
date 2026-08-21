package qsbridge

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDecodeAccessPolicyFileLoadsRoleAndUserGrants(t *testing.T) {
	policy, err := DecodeAccessPolicyFile([]byte(`
grants:
  - principal_kind: role
    principal: reader
    privilege: select
    schema: quanta
    table: orders
    fields:
      - o_orderkey
      - o_orderdate
  - principal_kind: user
    principal: loader
    privilege: insert
    schema: quanta
    table: lineitem
`))
	if err != nil {
		t.Fatalf("DecodeAccessPolicyFile failed: %v", err)
	}
	grants := policy.Grants()
	if len(grants) != 2 {
		t.Fatalf("grants = %#v, want two grants", grants)
	}
	if grants[0].PrincipalKind != AccessPrincipalRole || grants[0].Principal != "reader" || grants[0].Privilege != AccessSelect {
		t.Fatalf("role grant = %#v", grants[0])
	}
	if grants[0].Table.Schema != "quanta" || grants[0].Table.Table != "orders" || len(grants[0].Fields) != 2 {
		t.Fatalf("role grant table/fields = %#v", grants[0])
	}
	if grants[1].PrincipalKind != AccessPrincipalUser || grants[1].Principal != "loader" || grants[1].Privilege != AccessInsert {
		t.Fatalf("user grant = %#v", grants[1])
	}
}

func TestLoadAccessPolicyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-policy.yaml")
	if err := os.WriteFile(path, []byte(`
grants:
  - principal_kind: role
    principal: reader
    privilege: select
    schema: quanta
    table: orders
`), 0o600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	policy, err := LoadAccessPolicyFile(path)
	if err != nil {
		t.Fatalf("LoadAccessPolicyFile failed: %v", err)
	}
	decision := AuthorizationRequest{
		Session: SessionContext{Roles: []RoleName{"reader"}},
		Requirements: []AccessRequirement{{
			Privilege: AccessSelect,
			Table:     TableInstance{ID: "orders_1", Schema: "quanta", Table: "orders"},
		}},
	}.Authorize(policy)
	if !decision.Supported() {
		t.Fatalf("decision = %#v, want loaded role grant to authorize query-local table", decision)
	}
}

func TestDecodeAccessPolicyFileRejectsInvalidGrants(t *testing.T) {
	for _, content := range []string{
		`grants: []`,
		`
grants:
  - principal_kind: team
    principal: reader
    privilege: select
    table: orders
`,
		`
grants:
  - principal_kind: role
    principal: ""
    privilege: select
    table: orders
`,
		`
grants:
  - principal_kind: role
    principal: reader
    privilege: merge
    table: orders
`,
		`
grants:
  - principal_kind: role
    principal: reader
    privilege: select
    table: orders
    fields: [o_orderkey, o_orderkey]
`,
	} {
		_, err := DecodeAccessPolicyFile([]byte(content))
		if err == nil {
			t.Fatalf("DecodeAccessPolicyFile(%q) succeeded, want validation error", content)
		}
	}
}

func TestDecodeAccessPolicyFileRejectsUnknownFields(t *testing.T) {
	_, err := DecodeAccessPolicyFile([]byte(`
grants:
  - principal_kind: role
    principal: reader
    privilege: select
    table: orders
    unexpected: value
`))
	if err == nil || !strings.Contains(err.Error(), "field unexpected not found") {
		t.Fatalf("err = %v, want strict YAML unknown-field error", err)
	}
}
