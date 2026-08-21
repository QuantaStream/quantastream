package qsmysql

import (
	"fmt"
	"os"
	"strings"

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
	if len(file.Accounts) == 0 {
		return nil, fmt.Errorf("mysql static auth account file has no accounts")
	}
	seen := make(map[string]struct{}, len(file.Accounts))
	accounts := make([]StaticAccount, 0, len(file.Accounts))
	for i, account := range file.Accounts {
		account.Username = strings.TrimSpace(account.Username)
		account.DefaultDatabase = strings.TrimSpace(account.DefaultDatabase)
		account.MySQLNativePasswordVerifier = strings.TrimSpace(account.MySQLNativePasswordVerifier)
		account.CachingSHA2PasswordVerifier = strings.TrimSpace(account.CachingSHA2PasswordVerifier)
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
		accounts = append(accounts, account)
	}
	return accounts, nil
}
