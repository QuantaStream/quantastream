package admin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestAccessUpsertListAndRemoveCommandsManagePolicyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-policy.yaml")
	ctx := &Context{}

	if err := (&AccessUpsertCmd{
		PolicyFile:    path,
		PrincipalKind: "role",
		Principal:     "reader",
		Privilege:     "select",
		Schema:        "quanta",
		Table:         "orders",
		Fields:        "o_orderkey,o_orderdate",
	}).Run(ctx); err != nil {
		t.Fatalf("AccessUpsertCmd.Run reader returned error: %v", err)
	}
	if err := (&AccessUpsertCmd{
		PolicyFile:    path,
		PrincipalKind: "user",
		Principal:     "loader",
		Privilege:     "insert",
		Schema:        "quanta",
		Table:         "lineitem",
	}).Run(ctx); err != nil {
		t.Fatalf("AccessUpsertCmd.Run loader returned error: %v", err)
	}
	if err := (&AccessListCmd{PolicyFile: path}).Run(ctx); err != nil {
		t.Fatalf("AccessListCmd.Run returned error: %v", err)
	}
	policy, err := qsbridge.LoadAccessPolicyFile(path)
	if err != nil {
		t.Fatalf("LoadAccessPolicyFile failed: %v", err)
	}
	grants := policy.Grants()
	if len(grants) != 2 {
		t.Fatalf("grants = %#v, want 2", grants)
	}
	if len(grants[0].Fields) != 2 || grants[0].Fields[0].Name != "o_orderkey" || grants[0].Fields[1].Name != "o_orderdate" {
		t.Fatalf("reader fields = %#v", grants[0].Fields)
	}
	if err := (&AccessRemoveCmd{
		PolicyFile:    path,
		PrincipalKind: "user",
		Principal:     "loader",
		Privilege:     "insert",
		Schema:        "quanta",
		Table:         "lineitem",
	}).Run(ctx); err != nil {
		t.Fatalf("AccessRemoveCmd.Run returned error: %v", err)
	}
	policy, err = qsbridge.LoadAccessPolicyFile(path)
	if err != nil {
		t.Fatalf("LoadAccessPolicyFile after remove failed: %v", err)
	}
	grants = policy.Grants()
	if len(grants) != 1 || grants[0].Principal != "reader" {
		t.Fatalf("grants after remove = %#v", grants)
	}
}

func TestAccessUpsertReplacesMatchingGrant(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-policy.yaml")
	base := AccessUpsertCmd{
		PolicyFile:    path,
		PrincipalKind: "ROLE",
		Principal:     "reader",
		Privilege:     "SELECT",
		Schema:        "quanta",
		Table:         "orders",
		Fields:        "o_orderkey",
	}
	if err := (&base).Run(&Context{}); err != nil {
		t.Fatalf("AccessUpsertCmd.Run create returned error: %v", err)
	}
	base.Fields = "o_orderdate"
	if err := (&base).Run(&Context{}); err != nil {
		t.Fatalf("AccessUpsertCmd.Run replace returned error: %v", err)
	}
	policy, err := qsbridge.LoadAccessPolicyFile(path)
	if err != nil {
		t.Fatalf("LoadAccessPolicyFile failed: %v", err)
	}
	grants := policy.Grants()
	if len(grants) != 1 || len(grants[0].Fields) != 1 || grants[0].Fields[0].Name != "o_orderdate" {
		t.Fatalf("grants = %#v, want replaced grant", grants)
	}
}

func TestAccessUpsertRejectsDuplicateFields(t *testing.T) {
	err := (&AccessUpsertCmd{
		PolicyFile:    filepath.Join(t.TempDir(), "access-policy.yaml"),
		PrincipalKind: "role",
		Principal:     "reader",
		Privilege:     "select",
		Table:         "orders",
		Fields:        "o_orderkey,o_orderkey",
	}).Run(&Context{})
	if err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("AccessUpsertCmd.Run error = %v, want duplicate field error", err)
	}
}

func TestAccessRemoveRejectsMissingAndLastGrant(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-policy.yaml")
	if err := (&AccessUpsertCmd{
		PolicyFile:    path,
		PrincipalKind: "role",
		Principal:     "reader",
		Privilege:     "select",
		Table:         "orders",
	}).Run(&Context{}); err != nil {
		t.Fatalf("AccessUpsertCmd.Run returned error: %v", err)
	}
	if err := (&AccessRemoveCmd{
		PolicyFile:    path,
		PrincipalKind: "role",
		Principal:     "writer",
		Privilege:     "select",
		Table:         "orders",
	}).Run(&Context{}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("AccessRemoveCmd.Run missing error = %v, want not found", err)
	}
	if err := (&AccessRemoveCmd{
		PolicyFile:    path,
		PrincipalKind: "role",
		Principal:     "reader",
		Privilege:     "select",
		Table:         "orders",
	}).Run(&Context{}); err == nil || !strings.Contains(err.Error(), "last access policy grant") {
		t.Fatalf("AccessRemoveCmd.Run last error = %v, want last-grant guard", err)
	}
}

func TestAccessCommandsWritePrivatePolicyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-policy.yaml")
	if err := (&AccessUpsertCmd{
		PolicyFile:    path,
		PrincipalKind: "role",
		Principal:     "reader",
		Privilege:     "select",
		Table:         "orders",
	}).Run(&Context{}); err != nil {
		t.Fatalf("AccessUpsertCmd.Run returned error: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat policy file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("policy file mode = %v, want 0600", info.Mode().Perm())
	}
}
