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

func TestUpsertAccessPolicyFileCreatesAndReplacesGrant(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-policy.yaml")
	grant := AccessGrant{
		PrincipalKind: AccessPrincipalRole,
		Principal:     "reader",
		Privilege:     AccessSelect,
		Table:         TableInstance{Schema: "quanta", Table: "orders"},
		Fields:        []FieldRef{{Name: "o_orderkey"}},
	}
	if _, err := UpsertAccessPolicyFile(path, grant); err != nil {
		t.Fatalf("UpsertAccessPolicyFile create failed: %v", err)
	}
	grant.Fields = []FieldRef{{Name: "o_orderdate"}}
	if _, err := UpsertAccessPolicyFile(path, grant); err != nil {
		t.Fatalf("UpsertAccessPolicyFile replace failed: %v", err)
	}
	policy, err := LoadAccessPolicyFile(path)
	if err != nil {
		t.Fatalf("LoadAccessPolicyFile failed: %v", err)
	}
	grants := policy.Grants()
	if len(grants) != 1 || len(grants[0].Fields) != 1 || grants[0].Fields[0].Name != "o_orderdate" {
		t.Fatalf("grants = %#v, want replaced field grant", grants)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat policy file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("policy file mode = %v, want 0600", info.Mode().Perm())
	}
}

func TestRemoveAccessPolicyFileRemovesGrantButKeepsFileValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-policy.yaml")
	grants := []AccessGrant{
		{
			PrincipalKind: AccessPrincipalRole,
			Principal:     "reader",
			Privilege:     AccessSelect,
			Table:         TableInstance{Schema: "quanta", Table: "orders"},
		},
		{
			PrincipalKind: AccessPrincipalUser,
			Principal:     "loader",
			Privilege:     AccessInsert,
			Table:         TableInstance{Schema: "quanta", Table: "lineitem"},
		},
	}
	if err := SaveAccessPolicyFile(path, grants); err != nil {
		t.Fatalf("SaveAccessPolicyFile failed: %v", err)
	}
	remaining, removed, err := RemoveAccessPolicyFile(path, grants[1])
	if err != nil {
		t.Fatalf("RemoveAccessPolicyFile failed: %v", err)
	}
	if !removed || len(remaining) != 1 || remaining[0].Principal != "reader" {
		t.Fatalf("remaining=%#v removed=%t", remaining, removed)
	}
	if _, removed, err := RemoveAccessPolicyFile(path, grants[1]); err != nil || removed {
		t.Fatalf("missing removal removed=%t err=%v, want no-op", removed, err)
	}
	if _, _, err := RemoveAccessPolicyFile(path, grants[0]); err == nil || !strings.Contains(err.Error(), "last access policy grant") {
		t.Fatalf("remove last err = %v, want last-grant guard", err)
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
