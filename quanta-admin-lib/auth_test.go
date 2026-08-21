package admin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsmysql"
)

func TestAuthUpsertListAndRemoveCommandsManageAccountFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.yaml")
	ctx := &Context{}

	if err := (&AuthUpsertCmd{
		AccountFile:     path,
		User:            "bench",
		Password:        "secret",
		DefaultDatabase: "quanta",
	}).Run(ctx); err != nil {
		t.Fatalf("AuthUpsertCmd.Run returned error: %v", err)
	}
	if err := (&AuthUpsertCmd{
		AccountFile:     path,
		User:            "reader",
		Password:        "readerpass",
		DefaultDatabase: "quanta",
	}).Run(ctx); err != nil {
		t.Fatalf("AuthUpsertCmd.Run reader returned error: %v", err)
	}
	if err := (&AuthListCmd{AccountFile: path}).Run(ctx); err != nil {
		t.Fatalf("AuthListCmd.Run returned error: %v", err)
	}
	accounts, err := qsmysql.LoadStaticAccountFile(path)
	if err != nil {
		t.Fatalf("LoadStaticAccountFile failed: %v", err)
	}
	if len(accounts) != 2 {
		t.Fatalf("accounts = %#v, want 2", accounts)
	}
	for _, account := range accounts {
		if account.Password != "" || account.MySQLNativePasswordVerifier == "" || account.CachingSHA2PasswordVerifier == "" {
			t.Fatalf("account should contain verifier hashes only: %#v", account)
		}
	}
	if err := (&AuthRemoveCmd{AccountFile: path, User: "reader"}).Run(ctx); err != nil {
		t.Fatalf("AuthRemoveCmd.Run returned error: %v", err)
	}
	accounts, err = qsmysql.LoadStaticAccountFile(path)
	if err != nil {
		t.Fatalf("LoadStaticAccountFile after remove failed: %v", err)
	}
	if len(accounts) != 1 || accounts[0].Username != "bench" {
		t.Fatalf("accounts after remove = %#v", accounts)
	}
}

func TestAuthUpsertUsesPasswordEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.yaml")
	t.Setenv("QUANTASTREAM_AUTH_PASSWORD", "secret")

	if err := (&AuthUpsertCmd{AccountFile: path, User: "bench"}).Run(&Context{}); err != nil {
		t.Fatalf("AuthUpsertCmd.Run returned error: %v", err)
	}
	accounts, err := qsmysql.LoadStaticAccountFile(path)
	if err != nil {
		t.Fatalf("LoadStaticAccountFile failed: %v", err)
	}
	if len(accounts) != 1 || accounts[0].CachingSHA2PasswordVerifier == "" {
		t.Fatalf("accounts = %#v", accounts)
	}
}

func TestAuthUpsertRequiresPassword(t *testing.T) {
	err := (&AuthUpsertCmd{AccountFile: filepath.Join(t.TempDir(), "accounts.yaml"), User: "bench"}).Run(&Context{})
	if err == nil || !strings.Contains(err.Error(), "password is required") {
		t.Fatalf("AuthUpsertCmd.Run error = %v, want password requirement", err)
	}
}

func TestAuthRemoveRejectsMissingAndLastAccount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.yaml")
	if err := (&AuthUpsertCmd{AccountFile: path, User: "bench", Password: "secret"}).Run(&Context{}); err != nil {
		t.Fatalf("AuthUpsertCmd.Run returned error: %v", err)
	}
	if err := (&AuthRemoveCmd{AccountFile: path, User: "missing"}).Run(&Context{}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("AuthRemoveCmd.Run missing error = %v, want not found", err)
	}
	if err := (&AuthRemoveCmd{AccountFile: path, User: "bench"}).Run(&Context{}); err == nil || !strings.Contains(err.Error(), "last mysql static auth account") {
		t.Fatalf("AuthRemoveCmd.Run last error = %v, want last-account guard", err)
	}
}

func TestAuthHashPasswordRequiresPassword(t *testing.T) {
	t.Setenv("QUANTASTREAM_AUTH_PASSWORD", "")
	err := (&AuthHashPasswordCmd{}).Run(&Context{})
	if err == nil || !strings.Contains(err.Error(), "password is required") {
		t.Fatalf("AuthHashPasswordCmd.Run error = %v, want password requirement", err)
	}
}

func TestAuthHashPasswordUsesPasswordEnvironment(t *testing.T) {
	t.Setenv("QUANTASTREAM_AUTH_PASSWORD", "secret")
	if err := (&AuthHashPasswordCmd{}).Run(&Context{}); err != nil {
		t.Fatalf("AuthHashPasswordCmd.Run returned error: %v", err)
	}
}

func TestAuthCommandsWritePrivateAccountFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.yaml")
	if err := (&AuthUpsertCmd{AccountFile: path, User: "bench", Password: "secret"}).Run(&Context{}); err != nil {
		t.Fatalf("AuthUpsertCmd.Run returned error: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat account file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("account file mode = %v, want 0600", info.Mode().Perm())
	}
}
