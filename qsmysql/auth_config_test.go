package qsmysql

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAuthConfigDefaultsToPermissive(t *testing.T) {
	authenticator, err := (AuthConfig{}).Authenticator("quanta")
	if err != nil {
		t.Fatalf("Authenticator failed: %v", err)
	}
	if _, ok := authenticator.(PermissiveAuthenticator); !ok {
		t.Fatalf("authenticator = %T, want PermissiveAuthenticator", authenticator)
	}
	if got := (AuthConfig{}).SummaryMode("quanta"); got != AuthModePermissive {
		t.Fatalf("SummaryMode = %q, want permissive", got)
	}
	if got := (AuthConfig{}).SummaryUser("quanta"); got != "" {
		t.Fatalf("SummaryUser = %q, want hidden for permissive", got)
	}
}

func TestAuthConfigBuildsStaticAuthenticator(t *testing.T) {
	config := AuthConfig{Mode: "STATIC", Username: "bench", Password: "secret"}
	authenticator, err := config.Authenticator("quanta")
	if err != nil {
		t.Fatalf("Authenticator failed: %v", err)
	}
	static, ok := authenticator.(StaticAuthenticator)
	if !ok {
		t.Fatalf("authenticator = %T, want StaticAuthenticator", authenticator)
	}
	if len(static.Accounts) != 1 || static.Accounts[0].Username != "bench" || static.Accounts[0].Password != "secret" || static.Accounts[0].DefaultDatabase != "quanta" {
		t.Fatalf("accounts = %#v", static.Accounts)
	}
	if got := config.SummaryMode("quanta"); got != AuthModeStatic {
		t.Fatalf("SummaryMode = %q, want static", got)
	}
	if got := config.SummaryUser("quanta"); got != "bench" {
		t.Fatalf("SummaryUser = %q, want bench", got)
	}
}

func TestAuthConfigBuildsStaticAuthenticatorFromAccountFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.yaml")
	if err := os.WriteFile(path, []byte(`
accounts:
  - username: bench
    password: secret
    default_database: quanta
    roles: [reader]
`), 0o600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	config := AuthConfig{Mode: "static", AccountFile: path, Username: "ignored", Password: "ignored"}
	authenticator, err := config.Authenticator("quanta")
	if err != nil {
		t.Fatalf("Authenticator failed: %v", err)
	}
	static, ok := authenticator.(StaticAuthenticator)
	if !ok {
		t.Fatalf("authenticator = %T, want StaticAuthenticator", authenticator)
	}
	if len(static.Accounts) != 1 || static.Accounts[0].Username != "bench" || static.Accounts[0].Password != "secret" {
		t.Fatalf("accounts = %#v", static.Accounts)
	}
	if len(static.Accounts[0].Roles) != 1 || static.Accounts[0].Roles[0] != "reader" {
		t.Fatalf("accounts = %#v", static.Accounts)
	}
	if got := config.SummaryUser("quanta"); got != "" {
		t.Fatalf("SummaryUser = %q, want hidden when account file is configured", got)
	}
	if got := config.SummaryAccountFile("quanta"); got != path {
		t.Fatalf("SummaryAccountFile = %q, want %q", got, path)
	}
}

func TestAuthConfigRejectsUnknownMode(t *testing.T) {
	if _, err := (AuthConfig{Mode: "jwt"}).Authenticator("quanta"); err == nil {
		t.Fatalf("Authenticator should reject unsupported mode")
	}
}
