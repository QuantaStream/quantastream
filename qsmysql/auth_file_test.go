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

func TestUpsertStaticAccountFileCreatesAndReplacesAccount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.yaml")
	if _, err := UpsertStaticAccountFile(path, StaticAccountWithPasswordVerifiers("bench", "secret", "quanta")); err != nil {
		t.Fatalf("UpsertStaticAccountFile create failed: %v", err)
	}
	if _, err := UpsertStaticAccountFile(path, StaticAccountWithPasswordVerifiers("bench", "newsecret", "analytics")); err != nil {
		t.Fatalf("UpsertStaticAccountFile replace failed: %v", err)
	}
	accounts, err := LoadStaticAccountFile(path)
	if err != nil {
		t.Fatalf("LoadStaticAccountFile failed: %v", err)
	}
	if len(accounts) != 1 || accounts[0].Username != "bench" || accounts[0].DefaultDatabase != "analytics" {
		t.Fatalf("accounts = %#v", accounts)
	}
	if accounts[0].Password != "" || accounts[0].CachingSHA2PasswordVerifier == "" || accounts[0].MySQLNativePasswordVerifier == "" {
		t.Fatalf("account should contain verifier hashes only: %#v", accounts[0])
	}
}

func TestRemoveStaticAccountFileRemovesAccountButKeepsFileValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.yaml")
	accounts := []StaticAccount{
		StaticAccountWithPasswordVerifiers("bench", "secret", "quanta"),
		StaticAccountWithPasswordVerifiers("reader", "secret", "quanta"),
	}
	if err := SaveStaticAccountFile(path, accounts); err != nil {
		t.Fatalf("SaveStaticAccountFile failed: %v", err)
	}
	remaining, removed, err := RemoveStaticAccountFile(path, "reader")
	if err != nil {
		t.Fatalf("RemoveStaticAccountFile failed: %v", err)
	}
	if !removed || len(remaining) != 1 || remaining[0].Username != "bench" {
		t.Fatalf("remaining=%#v removed=%t", remaining, removed)
	}
	if _, removed, err := RemoveStaticAccountFile(path, "missing"); err != nil || removed {
		t.Fatalf("missing removal removed=%t err=%v, want no-op", removed, err)
	}
	if _, _, err := RemoveStaticAccountFile(path, "bench"); err == nil || !strings.Contains(err.Error(), "last mysql static auth account") {
		t.Fatalf("remove last err = %v, want last-account guard", err)
	}
}
