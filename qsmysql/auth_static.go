package qsmysql

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

const (
	mysqlNativePasswordPluginName = "mysql_native_password"
	mysqlClearPasswordPluginName  = "mysql_clear_password"
	sha1VerifierSize              = sha1.Size
	sha256VerifierSize            = sha256.Size
)

// StaticAccount is a small built-in account/password identity used by the
// public MySQL-compatible auth path before enterprise auth providers are wired.
type StaticAccount struct {
	Username                    string              `yaml:"username"`
	Password                    string              `yaml:"password,omitempty"`
	MySQLNativePasswordVerifier string              `yaml:"mysql_native_password_verifier,omitempty"`
	CachingSHA2PasswordVerifier string              `yaml:"caching_sha2_password_verifier,omitempty"`
	DefaultDatabase             string              `yaml:"default_database,omitempty"`
	Roles                       []qsbridge.RoleName `yaml:"roles,omitempty"`
}

// StaticAuthenticator validates decoded MySQL password auth tokens against an
// explicit in-memory account list. Authorization/RBAC remains a separate layer.
type StaticAuthenticator struct {
	Accounts []StaticAccount
}

// StaticAccountWithPasswordVerifiers returns an account record that can verify
// normal MySQL password tokens without storing the cleartext password.
func StaticAccountWithPasswordVerifiers(username, password, defaultDatabase string) StaticAccount {
	return StaticAccount{
		Username:                    strings.TrimSpace(username),
		MySQLNativePasswordVerifier: mysqlNativeVerifier(password),
		CachingSHA2PasswordVerifier: cachingSHA2Verifier(password),
		DefaultDatabase:             strings.TrimSpace(defaultDatabase),
	}
}

// Authenticate validates the request against the configured static accounts.
func (a StaticAuthenticator) Authenticate(ctx context.Context, request AuthRequest) (AuthDecision, error) {
	if err := ctx.Err(); err != nil {
		return AuthDecision{}, err
	}
	account, ok := a.account(request.Username)
	if !ok {
		return staticAuthDenied(request, fmt.Sprintf("access denied for user %q", request.Username)), nil
	}
	if !staticAuthPasswordMatches(account, request) {
		return staticAuthDenied(request, fmt.Sprintf("access denied for user %q", request.Username)), nil
	}
	database := request.Database
	if database == "" {
		database = account.DefaultDatabase
	}
	return AuthDecision{
		Accepted:       true,
		Username:       request.Username,
		Database:       database,
		Roles:          append([]qsbridge.RoleName(nil), account.Roles...),
		AuthPluginName: staticAuthPluginName(request.AuthPluginName),
	}, nil
}

func (a StaticAuthenticator) account(username string) (StaticAccount, bool) {
	for _, account := range a.Accounts {
		if account.Username == username {
			return account, true
		}
	}
	return StaticAccount{}, false
}

func staticAuthDenied(request AuthRequest, message string) AuthDecision {
	return AuthDecision{
		Accepted: false,
		Username: request.Username,
		Database: request.Database,
		Failure: qsbridge.ProtocolError{
			SQLState:   qsbridge.SQLStateGeneralError,
			VendorCode: 1045,
			Message:    message,
		},
	}
}

func staticAuthPasswordMatches(account StaticAccount, request AuthRequest) bool {
	if account.Password == "" && account.MySQLNativePasswordVerifier == "" && account.CachingSHA2PasswordVerifier == "" {
		return len(request.AuthResponse) == 0
	}
	switch staticAuthPluginName(request.AuthPluginName) {
	case cachingSHA2PasswordPluginName:
		if account.Password != "" {
			return constantTimeBytesEqual(request.AuthResponse, cachingSHA2PasswordToken(request.AuthPluginData, account.Password))
		}
		return cachingSHA2VerifierMatches(account.CachingSHA2PasswordVerifier, request.AuthPluginData, request.AuthResponse)
	case mysqlNativePasswordPluginName:
		if account.Password != "" {
			return constantTimeBytesEqual(request.AuthResponse, mysqlNativePasswordToken(request.AuthPluginData, account.Password))
		}
		return mysqlNativeVerifierMatches(account.MySQLNativePasswordVerifier, request.AuthPluginData, request.AuthResponse)
	case mysqlClearPasswordPluginName:
		return account.Password != "" && constantTimeStringEqual(string(bytes.TrimSuffix(request.AuthResponse, []byte{0})), account.Password)
	default:
		return false
	}
}

func staticAuthPluginName(plugin string) string {
	if plugin == "" {
		return defaultAuthPluginName
	}
	return plugin
}

func mysqlNativePasswordToken(seed []byte, password string) []byte {
	if password == "" {
		return nil
	}
	stage1 := sha1.Sum([]byte(password))
	stage2 := sha1.Sum(stage1[:])
	h := sha1.New()
	_, _ = h.Write(seed)
	_, _ = h.Write(stage2[:])
	scramble := h.Sum(nil)
	token := make([]byte, len(stage1))
	for i := range stage1 {
		token[i] = stage1[i] ^ scramble[i]
	}
	return token
}

func mysqlNativeVerifier(password string) string {
	if password == "" {
		return ""
	}
	stage1 := sha1.Sum([]byte(password))
	stage2 := sha1.Sum(stage1[:])
	return hex.EncodeToString(stage2[:])
}

func mysqlNativeVerifierMatches(verifier string, seed []byte, token []byte) bool {
	expected, ok := decodeHexVerifier(verifier, sha1.Size)
	if !ok || len(token) != sha1.Size {
		return false
	}
	h := sha1.New()
	_, _ = h.Write(seed)
	_, _ = h.Write(expected)
	scramble := h.Sum(nil)
	stage1 := make([]byte, len(token))
	for i := range token {
		stage1[i] = token[i] ^ scramble[i]
	}
	candidate := sha1.Sum(stage1)
	return constantTimeBytesEqual(candidate[:], expected)
}

func cachingSHA2PasswordToken(seed []byte, password string) []byte {
	if password == "" {
		return nil
	}
	message1 := sha256.Sum256([]byte(password))
	message1Hash := sha256.Sum256(message1[:])
	h := sha256.New()
	_, _ = h.Write(message1Hash[:])
	_, _ = h.Write(seed)
	message2 := h.Sum(nil)
	token := make([]byte, len(message1))
	for i := range message1 {
		token[i] = message1[i] ^ message2[i]
	}
	return token
}

func cachingSHA2Verifier(password string) string {
	if password == "" {
		return ""
	}
	message1 := sha256.Sum256([]byte(password))
	message1Hash := sha256.Sum256(message1[:])
	return hex.EncodeToString(message1Hash[:])
}

func cachingSHA2VerifierMatches(verifier string, seed []byte, token []byte) bool {
	expected, ok := decodeHexVerifier(verifier, sha256.Size)
	if !ok || len(token) != sha256.Size {
		return false
	}
	h := sha256.New()
	_, _ = h.Write(expected)
	_, _ = h.Write(seed)
	message2 := h.Sum(nil)
	message1 := make([]byte, len(token))
	for i := range token {
		message1[i] = token[i] ^ message2[i]
	}
	candidate := sha256.Sum256(message1)
	return constantTimeBytesEqual(candidate[:], expected)
}

func decodeHexVerifier(value string, size int) ([]byte, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != size {
		return nil, false
	}
	return decoded, true
}

func constantTimeBytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare(left, right) == 1
}

func constantTimeStringEqual(left, right string) bool {
	return constantTimeBytesEqual([]byte(left), []byte(right))
}
