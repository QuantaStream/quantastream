package qsmysql

import (
	"context"
	"fmt"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// AuthRequest is the protocol-neutral authentication input decoded from the MySQL handshake response.
type AuthRequest struct {
	ConnectionID      uint32
	Username          string
	Database          string
	AuthPluginName    string
	AuthPluginData    []byte
	AuthResponse      []byte
	CapabilityFlags   CapabilityFlag
	CharacterSet      CharacterSet
	MaxPacketSize     uint32
	HandshakeResponse HandshakeResponse41
}

// AuthDecision is the outcome of authenticating a MySQL connection attempt.
type AuthDecision struct {
	Accepted       bool
	Username       string
	Database       string
	Roles          []qsbridge.RoleName
	AuthPluginName string
	Failure        qsbridge.ProtocolError
}

// Authenticator validates a decoded MySQL handshake response.
type Authenticator interface {
	Authenticate(context.Context, AuthRequest) (AuthDecision, error)
}

// PermissiveAuthenticator accepts every syntactically valid handshake response.
type PermissiveAuthenticator struct{}

// Authenticate accepts the request while preserving the requested session metadata.
func (PermissiveAuthenticator) Authenticate(ctx context.Context, request AuthRequest) (AuthDecision, error) {
	if err := ctx.Err(); err != nil {
		return AuthDecision{}, err
	}
	return AuthDecision{
		Accepted:       true,
		Username:       request.Username,
		Database:       request.Database,
		AuthPluginName: request.AuthPluginName,
	}, nil
}

// RejectingAuthenticator is a small test/dev implementation that always rejects.
type RejectingAuthenticator struct {
	Message string
}

// Authenticate rejects the request with an access-denied protocol error.
func (a RejectingAuthenticator) Authenticate(ctx context.Context, request AuthRequest) (AuthDecision, error) {
	if err := ctx.Err(); err != nil {
		return AuthDecision{}, err
	}
	message := a.Message
	if message == "" {
		message = fmt.Sprintf("access denied for user %q", request.Username)
	}
	return AuthDecision{
		Accepted: false,
		Username: request.Username,
		Database: request.Database,
		Failure: qsbridge.ProtocolError{
			SQLState:   qsbridge.SQLStateGeneralError,
			VendorCode: 1045,
			Message:    message,
		},
	}, nil
}

func authRequestFromHandshake(connection Connection, response HandshakeResponse41, authPluginData []byte) AuthRequest {
	return AuthRequest{
		ConnectionID:      connection.ID,
		Username:          response.Username,
		Database:          response.Database,
		AuthPluginName:    response.AuthPluginName,
		AuthPluginData:    append([]byte(nil), authPluginData...),
		AuthResponse:      append([]byte(nil), response.AuthResponse...),
		CapabilityFlags:   response.CapabilityFlags,
		CharacterSet:      response.CharacterSet,
		MaxPacketSize:     response.MaxPacketSize,
		HandshakeResponse: response,
	}
}
