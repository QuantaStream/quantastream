package qsmysql

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDecodeStaticAccountFileLoadsAccounts(t *testing.T) {
	accounts, err := DecodeStaticAccountFile([]byte(`
accounts:
  - username: bench
    password: secret
    default_database: quanta
`))
	if err != nil {
		t.Fatalf("DecodeStaticAccountFile failed: %v", err)
	}
	if len(accounts) != 1 || accounts[0].Username != "bench" || accounts[0].Password != "secret" || accounts[0].DefaultDatabase != "quanta" {
		t.Fatalf("accounts = %#v", accounts)
	}
}

func TestDecodeStaticAccountFileRejectsDuplicateUsers(t *testing.T) {
	_, err := DecodeStaticAccountFile([]byte(`
accounts:
  - username: bench
  - username: bench
`))
	if err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("err = %v, want duplicated user error", err)
	}
}

func TestDecodeStaticAccountFileRejectsMalformedVerifier(t *testing.T) {
	_, err := DecodeStaticAccountFile([]byte(`
accounts:
  - username: bench
    caching_sha2_password_verifier: nope
`))
	if err == nil || !strings.Contains(err.Error(), "invalid caching_sha2_password_verifier") {
		t.Fatalf("err = %v, want verifier error", err)
	}
}

func TestLoadStaticAccountFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.yaml")
	if err := os.WriteFile(path, []byte(`
accounts:
  - username: bench
    mysql_native_password_verifier: `+mysqlNativeVerifier("secret")+`
`), 0o600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	accounts, err := LoadStaticAccountFile(path)
	if err != nil {
		t.Fatalf("LoadStaticAccountFile failed: %v", err)
	}
	if len(accounts) != 1 || accounts[0].Username != "bench" || accounts[0].MySQLNativePasswordVerifier == "" {
		t.Fatalf("accounts = %#v", accounts)
	}
}
