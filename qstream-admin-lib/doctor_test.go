package admin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

func TestDoctorLocalCmdPassesValidLocalInputs(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	configDir := filepath.Join(root, "configuration")
	writeAdminBackupTestFile(t, configDir, "cities/schema.yaml", "tableName: cities\n")
	writeAdminBackupTestFile(t, configDir, "CATALOG_OBJECTS", "cities table\n")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("MkdirAll data: %v", err)
	}
	authPath := filepath.Join(root, "accounts.yaml")
	if err := qsmysql.SaveStaticAccountFile(authPath, []qsmysql.StaticAccount{
		qsmysql.StaticAccountWithPasswordVerifiers("root", "root", "quanta"),
	}); err != nil {
		t.Fatalf("SaveStaticAccountFile returned error: %v", err)
	}
	accessPath := filepath.Join(root, "access-policy.yaml")
	if err := qsbridge.SaveAccessPolicyFile(accessPath, []qsbridge.AccessGrant{{
		PrincipalKind: qsbridge.AccessPrincipalUser,
		Principal:     "root",
		Privilege:     qsbridge.AccessSelect,
		Table:         qsbridge.TableInstance{Table: "*"},
	}}); err != nil {
		t.Fatalf("SaveAccessPolicyFile returned error: %v", err)
	}

	output, err := captureAdminBackupStdout(t, func() error {
		return (&DoctorLocalCmd{
			DataDir:          dataDir,
			ConfigDir:        configDir,
			AuthAccountFile:  authPath,
			AccessPolicyFile: accessPath,
		}).Run(&Context{})
	})
	if err != nil {
		t.Fatalf("DoctorLocalCmd.Run returned error: %v\n%s", err, output)
	}
	assertAdminBackupOutputContains(t, output,
		"doctor_scope=local",
		"doctor_check name=data_dir status=PASS",
		"doctor_check name=config_dir status=PASS",
		"doctor_check name=auth_account_file status=PASS",
		"doctor_check name=access_policy_file status=PASS",
		"doctor_result=PASS",
	)
}

func TestDoctorLocalCmdFailsInvalidAccessPolicy(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	configDir := filepath.Join(root, "configuration")
	writeAdminBackupTestFile(t, configDir, "cities/schema.yaml", "tableName: cities\n")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("MkdirAll data: %v", err)
	}
	accessPath := filepath.Join(root, "access-policy.yaml")
	if err := os.WriteFile(accessPath, []byte(`
grants:
  - principal_kind: role
    principal: reader
    privilege: merge
    table: cities
`), 0o600); err != nil {
		t.Fatalf("WriteFile access policy: %v", err)
	}

	output, err := captureAdminBackupStdout(t, func() error {
		return (&DoctorLocalCmd{
			DataDir:          dataDir,
			ConfigDir:        configDir,
			AccessPolicyFile: accessPath,
		}).Run(&Context{})
	})
	if err == nil || !strings.Contains(err.Error(), "doctor local failed") {
		t.Fatalf("DoctorLocalCmd.Run error = %v, want failed doctor\n%s", err, output)
	}
	assertAdminBackupOutputContains(t, output,
		"doctor_check name=access_policy_file status=FAIL",
		"doctor_result=FAIL failures=1",
	)
}
