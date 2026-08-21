package qsmysql

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"

	"github.com/QuantaStream/quantastream/qsbridge"
)

const (
	mysqlNativePasswordPluginName = "mysql_native_password"
	mysqlClearPasswordPluginName  = "mysql_clear_password"
)

// StaticAccount is a small built-in account/password identity used by the
// public MySQL-compatible auth path before enterprise auth providers are wired.
type StaticAccount struct {
	Username        string
	Password        string
	DefaultDatabase string
}

// StaticAuthenticator validates decoded MySQL password auth tokens against an
// explicit in-memory account list. Authorization/RBAC remains a separate layer.
type StaticAuthenticator struct {
	Accounts []StaticAccount
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
	if !staticAuthPasswordMatches(account.Password, request) {
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

func staticAuthPasswordMatches(password string, request AuthRequest) bool {
	if password == "" {
		return len(request.AuthResponse) == 0
	}
	switch staticAuthPluginName(request.AuthPluginName) {
	case cachingSHA2PasswordPluginName:
		return constantTimeBytesEqual(request.AuthResponse, cachingSHA2PasswordToken(request.AuthPluginData, password))
	case mysqlNativePasswordPluginName:
		return constantTimeBytesEqual(request.AuthResponse, mysqlNativePasswordToken(request.AuthPluginData, password))
	case mysqlClearPasswordPluginName:
		return constantTimeStringEqual(string(bytes.TrimSuffix(request.AuthResponse, []byte{0})), password)
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

func constantTimeBytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare(left, right) == 1
}

func constantTimeStringEqual(left, right string) bool {
	return constantTimeBytesEqual([]byte(left), []byte(right))
}
