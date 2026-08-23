package qsmysql

import (
	"fmt"
	"strings"
)

const (
	// AuthModePermissive preserves local/dev compatibility by accepting valid handshakes.
	AuthModePermissive = "permissive"
	// AuthModeStatic validates one configured MySQL-compatible account.
	AuthModeStatic  = "static"
	defaultAuthUser = "MOLIG004"
)

// AuthConfig is the shared MySQL front-door authentication configuration.
type AuthConfig struct {
	Mode        string
	Username    string
	Password    string
	Database    string
	AccountFile string
}

// WithDefaults returns a normalized auth configuration.
func (c AuthConfig) WithDefaults(defaultDatabase string) AuthConfig {
	c.Mode = strings.ToLower(strings.TrimSpace(c.Mode))
	if c.Mode == "" {
		c.Mode = AuthModePermissive
	}
	if strings.TrimSpace(c.Username) == "" {
		c.Username = defaultAuthUser
	}
	if strings.TrimSpace(c.Database) == "" {
		c.Database = strings.TrimSpace(defaultDatabase)
	}
	return c
}

// Authenticator builds the protocol authenticator selected by this config.
func (c AuthConfig) Authenticator(defaultDatabase string) (Authenticator, error) {
	c = c.WithDefaults(defaultDatabase)
	switch c.Mode {
	case AuthModePermissive:
		return PermissiveAuthenticator{DefaultDatabase: c.Database}, nil
	case AuthModeStatic:
		if strings.TrimSpace(c.AccountFile) != "" {
			accounts, err := LoadStaticAccountFile(c.AccountFile)
			if err != nil {
				return nil, fmt.Errorf("load mysql static auth account file: %w", err)
			}
			return StaticAuthenticator{Accounts: accounts}, nil
		}
		return StaticAuthenticator{Accounts: []StaticAccount{{
			Username:        c.Username,
			Password:        c.Password,
			DefaultDatabase: c.Database,
		}}}, nil
	default:
		return nil, fmt.Errorf("unsupported mysql auth mode %q", c.Mode)
	}
}

// SummaryMode returns the normalized mode string safe for startup output.
func (c AuthConfig) SummaryMode(defaultDatabase string) string {
	return c.WithDefaults(defaultDatabase).Mode
}

// SummaryUser returns the configured account name safe for startup output.
func (c AuthConfig) SummaryUser(defaultDatabase string) string {
	c = c.WithDefaults(defaultDatabase)
	if c.Mode != AuthModeStatic || strings.TrimSpace(c.AccountFile) != "" {
		return ""
	}
	return c.Username
}

// SummaryAccountFile returns the account-file path safe for startup output.
func (c AuthConfig) SummaryAccountFile(defaultDatabase string) string {
	c = c.WithDefaults(defaultDatabase)
	if c.Mode != AuthModeStatic {
		return ""
	}
	return strings.TrimSpace(c.AccountFile)
}
