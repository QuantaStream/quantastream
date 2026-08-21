package qsmysql

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
	"gopkg.in/yaml.v2"
)

// StaticAccountFile is the YAML shape for file-backed static auth.
type StaticAccountFile struct {
	Accounts []StaticAccount `yaml:"accounts"`
}

// LoadStaticAccountFile loads a YAML account file for static MySQL auth.
func LoadStaticAccountFile(path string) ([]StaticAccount, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("mysql static auth account file path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return DecodeStaticAccountFile(data)
}

// DecodeStaticAccountFile decodes and validates a static account YAML document.
func DecodeStaticAccountFile(data []byte) ([]StaticAccount, error) {
	var file StaticAccountFile
	if err := yaml.UnmarshalStrict(data, &file); err != nil {
		return nil, err
	}
	return validateStaticAccounts(file.Accounts)
}

// SaveStaticAccountFile writes accounts to a YAML account file atomically enough
// for local admin workflows.
func SaveStaticAccountFile(path string, accounts []StaticAccount) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("mysql static auth account file path is empty")
	}
	normalized, err := validateStaticAccounts(accounts)
	if err != nil {
		return err
	}
	data, err := yaml.Marshal(StaticAccountFile{Accounts: normalized})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// UpsertStaticAccountFile inserts or replaces account by username.
func UpsertStaticAccountFile(path string, account StaticAccount) ([]StaticAccount, error) {
	var accounts []StaticAccount
	if _, err := os.Stat(path); err == nil {
		loaded, err := LoadStaticAccountFile(path)
		if err != nil {
			return nil, err
		}
		accounts = loaded
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	account.Username = strings.TrimSpace(account.Username)
	account.DefaultDatabase = strings.TrimSpace(account.DefaultDatabase)
	replaced := false
	for i := range accounts {
		if accounts[i].Username == account.Username {
			accounts[i] = account
			replaced = true
			break
		}
	}
	if !replaced {
		accounts = append(accounts, account)
	}
	if err := SaveStaticAccountFile(path, accounts); err != nil {
		return nil, err
	}
	return accounts, nil
}

// RemoveStaticAccountFile removes username from a static account file.
func RemoveStaticAccountFile(path, username string) ([]StaticAccount, bool, error) {
	accounts, err := LoadStaticAccountFile(path)
	if err != nil {
		return nil, false, err
	}
	username = strings.TrimSpace(username)
	remaining := accounts[:0]
	removed := false
	for _, account := range accounts {
		if account.Username == username {
			removed = true
			continue
		}
		remaining = append(remaining, account)
	}
	if !removed {
		return accounts, false, nil
	}
	if len(remaining) == 0 {
		return nil, false, fmt.Errorf("cannot remove the last mysql static auth account")
	}
	if err := SaveStaticAccountFile(path, remaining); err != nil {
		return nil, false, err
	}
	return remaining, true, nil
}

func validateStaticAccounts(accounts []StaticAccount) ([]StaticAccount, error) {
	if len(accounts) == 0 {
		return nil, fmt.Errorf("mysql static auth account file has no accounts")
	}
	seen := make(map[string]struct{}, len(accounts))
	normalized := make([]StaticAccount, 0, len(accounts))
	for i, account := range accounts {
		account.Username = strings.TrimSpace(account.Username)
		account.DefaultDatabase = strings.TrimSpace(account.DefaultDatabase)
		account.MySQLNativePasswordVerifier = strings.TrimSpace(account.MySQLNativePasswordVerifier)
		account.CachingSHA2PasswordVerifier = strings.TrimSpace(account.CachingSHA2PasswordVerifier)
		roles, err := normalizeStaticAccountRoles(account.Roles)
		if err != nil {
			return nil, fmt.Errorf("mysql static auth account %q: %w", account.Username, err)
		}
		account.Roles = roles
		if account.Username == "" {
			return nil, fmt.Errorf("mysql static auth account %d has empty username", i+1)
		}
		if _, ok := seen[account.Username]; ok {
			return nil, fmt.Errorf("mysql static auth account %q is duplicated", account.Username)
		}
		if account.MySQLNativePasswordVerifier != "" {
			if _, ok := decodeHexVerifier(account.MySQLNativePasswordVerifier, sha1VerifierSize); !ok {
				return nil, fmt.Errorf("mysql static auth account %q has invalid mysql_native_password_verifier", account.Username)
			}
		}
		if account.CachingSHA2PasswordVerifier != "" {
			if _, ok := decodeHexVerifier(account.CachingSHA2PasswordVerifier, sha256VerifierSize); !ok {
				return nil, fmt.Errorf("mysql static auth account %q has invalid caching_sha2_password_verifier", account.Username)
			}
		}
		seen[account.Username] = struct{}{}
		normalized = append(normalized, account)
	}
	return normalized, nil
}

func normalizeStaticAccountRoles(roles []qsbridge.RoleName) ([]qsbridge.RoleName, error) {
	if len(roles) == 0 {
		return nil, nil
	}
	seen := make(map[qsbridge.RoleName]struct{}, len(roles))
	normalized := make([]qsbridge.RoleName, 0, len(roles))
	for _, role := range roles {
		role = qsbridge.RoleName(strings.TrimSpace(string(role)))
		if role == "" {
			return nil, fmt.Errorf("role is empty")
		}
		if _, ok := seen[role]; ok {
			return nil, fmt.Errorf("role %q is duplicated", role)
		}
		seen[role] = struct{}{}
		normalized = append(normalized, role)
	}
	return normalized, nil
}
