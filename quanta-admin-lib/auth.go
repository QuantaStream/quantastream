package admin

import (
	"fmt"
	"os"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

// AuthCmd groups local MySQL auth account-file operations.
type AuthCmd struct {
	List         AuthListCmd         `cmd:"" help:"List accounts in a static auth account file."`
	Upsert       AuthUpsertCmd       `cmd:"" help:"Create or update one static auth account."`
	Remove       AuthRemoveCmd       `cmd:"" help:"Remove one static auth account."`
	HashPassword AuthHashPasswordCmd `cmd:"" name:"hash-password" help:"Print verifier hashes for a password."`
}

type AuthListCmd struct {
	AccountFile string `help:"YAML static auth account file." required:""`
}

type AuthUpsertCmd struct {
	AccountFile     string `help:"YAML static auth account file." required:""`
	User            string `help:"Account username." required:""`
	Password        string `help:"Account password. Prefer QUANTASTREAM_AUTH_PASSWORD to avoid shell history."`
	DefaultDatabase string `help:"Default database/schema for the account."`
	Roles           string `help:"Comma-separated effective roles for this account."`
}

type AuthRemoveCmd struct {
	AccountFile string `help:"YAML static auth account file." required:""`
	User        string `help:"Account username to remove." required:""`
}

type AuthHashPasswordCmd struct {
	Password string `help:"Password to hash. Prefer QUANTASTREAM_AUTH_PASSWORD to avoid shell history."`
}

func (c *AuthListCmd) Run(ctx *Context) error {
	accounts, err := qsmysql.LoadStaticAccountFile(c.AccountFile)
	if err != nil {
		return err
	}
	printAuthAccounts(c.AccountFile, accounts)
	return nil
}

func (c *AuthUpsertCmd) Run(ctx *Context) error {
	password, err := authCommandPassword(c.Password)
	if err != nil {
		return err
	}
	account := qsmysql.StaticAccountWithPasswordVerifiers(c.User, password, c.DefaultDatabase)
	roles, err := parseAuthRoles(c.Roles)
	if err != nil {
		return err
	}
	account.Roles = roles
	accounts, err := qsmysql.UpsertStaticAccountFile(c.AccountFile, account)
	if err != nil {
		return err
	}
	fmt.Printf("auth_account_upserted=%s\n", account.Username)
	printAuthAccounts(c.AccountFile, accounts)
	return nil
}

func (c *AuthRemoveCmd) Run(ctx *Context) error {
	accounts, removed, err := qsmysql.RemoveStaticAccountFile(c.AccountFile, c.User)
	if err != nil {
		return err
	}
	if !removed {
		return fmt.Errorf("mysql static auth account %q not found", c.User)
	}
	fmt.Printf("auth_account_removed=%s\n", strings.TrimSpace(c.User))
	printAuthAccounts(c.AccountFile, accounts)
	return nil
}

func (c *AuthHashPasswordCmd) Run(ctx *Context) error {
	password, err := authCommandPassword(c.Password)
	if err != nil {
		return err
	}
	account := qsmysql.StaticAccountWithPasswordVerifiers("example", password, "")
	fmt.Printf("mysql_native_password_verifier=%s\n", account.MySQLNativePasswordVerifier)
	fmt.Printf("caching_sha2_password_verifier=%s\n", account.CachingSHA2PasswordVerifier)
	return nil
}

func authCommandPassword(flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	if value := os.Getenv("QUANTASTREAM_AUTH_PASSWORD"); value != "" {
		return value, nil
	}
	return "", fmt.Errorf("password is required; pass --password or set QUANTASTREAM_AUTH_PASSWORD")
}

func printAuthAccounts(path string, accounts []qsmysql.StaticAccount) {
	fmt.Printf("auth_account_file=%s\n", path)
	fmt.Printf("auth_account_count=%d\n", len(accounts))
	for _, account := range accounts {
		fmt.Printf(
			"auth_account username=%s default_database=%s roles=%s password_cleartext=%t mysql_native_password_verifier=%t caching_sha2_password_verifier=%t\n",
			account.Username,
			account.DefaultDatabase,
			authRoleSummary(account.Roles),
			account.Password != "",
			account.MySQLNativePasswordVerifier != "",
			account.CachingSHA2PasswordVerifier != "",
		)
	}
}

func parseAuthRoles(value string) ([]qsbridge.RoleName, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	roles := make([]qsbridge.RoleName, 0, len(parts))
	seen := make(map[qsbridge.RoleName]struct{}, len(parts))
	for _, part := range parts {
		role := qsbridge.RoleName(strings.TrimSpace(part))
		if role == "" {
			return nil, fmt.Errorf("auth role is empty")
		}
		if _, ok := seen[role]; ok {
			return nil, fmt.Errorf("auth role %q is duplicated", role)
		}
		seen[role] = struct{}{}
		roles = append(roles, role)
	}
	return roles, nil
}

func authRoleSummary(roles []qsbridge.RoleName) string {
	if len(roles) == 0 {
		return ""
	}
	values := make([]string, 0, len(roles))
	for _, role := range roles {
		values = append(values, string(role))
	}
	return strings.Join(values, ",")
}
