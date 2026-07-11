package qsmysql

import (
	"context"
	"fmt"
)

// PacketReadWriter is the packet-level transport contract used by one MySQL session.
type PacketReadWriter interface {
	PacketReader
	PacketWriter
}

// SessionRunnerConfig captures the socket-free inputs for one MySQL protocol session.
type SessionRunnerConfig struct {
	ConnectionID   uint32
	AuthPluginData []byte
	Handshake      HandshakeV10
	Stream         PacketReadWriter
	Handler        CommandHandler
	Authenticator  Authenticator
}

// SessionRunner drives one MySQL connection over an already-mounted packet stream.
type SessionRunner struct {
	Connection    Connection
	Handshake     HandshakeV10
	Stream        PacketReadWriter
	Handler       CommandHandler
	Authenticator Authenticator
}

// NewSessionRunner returns a connection runner with deterministic default handshake data.
func NewSessionRunner(config SessionRunnerConfig) SessionRunner {
	connectionID := config.ConnectionID
	if connectionID == 0 {
		connectionID = 1
	}
	handshake := config.Handshake
	if handshake.ServerVersion == "" {
		authPluginData := config.AuthPluginData
		if len(authPluginData) == 0 {
			authPluginData = []byte("quantastream-auth-seed")
		}
		handshake = NewDefaultHandshake(connectionID, authPluginData)
	}
	authenticator := config.Authenticator
	if authenticator == nil {
		authenticator = PermissiveAuthenticator{}
	}
	return SessionRunner{
		Connection:    NewConnection(connectionID),
		Handshake:     handshake,
		Stream:        config.Stream,
		Handler:       config.Handler,
		Authenticator: authenticator,
	}
}

// SendHandshake writes the initial server greeting and advances the connection state.
func (r *SessionRunner) SendHandshake(ctx context.Context) error {
	if r.Stream == nil {
		return fmt.Errorf("mysql session runner stream is nil")
	}
	payload, err := r.Handshake.Payload()
	if err != nil {
		return err
	}
	connection, err := r.Connection.WithHandshakeSent()
	if err != nil {
		return err
	}
	if err := r.Stream.WritePacket(ctx, Packet{SequenceID: 0, Payload: payload}); err != nil {
		return err
	}
	r.Connection = connection
	return nil
}

// AcceptHandshakeResponse reads a client response, performs permissive auth, and writes an OK packet.
func (r *SessionRunner) AcceptHandshakeResponse(ctx context.Context) (CommandResponse, error) {
	if r.Stream == nil {
		return CommandResponse{}, fmt.Errorf("mysql session runner stream is nil")
	}
	packet, err := r.Stream.ReadPacket(ctx)
	if err != nil {
		return CommandResponse{}, err
	}
	response, err := DecodeHandshakeResponse41(packet.Payload)
	if err != nil {
		return CommandResponse{}, err
	}
	connection, err := r.Connection.AcceptHandshakeResponse(response)
	if err != nil {
		return CommandResponse{}, err
	}
	authenticator := r.Authenticator
	if authenticator == nil {
		authenticator = PermissiveAuthenticator{}
	}
	decision, err := authenticator.Authenticate(ctx, authRequestFromHandshake(connection, response))
	if err != nil {
		return CommandResponse{}, err
	}
	if !decision.Accepted {
		failure := ErrorResponse(decision.Failure)
		failure.Packets = clonePacketsWithSequence(failure.Packets, packet.SequenceID+1)
		if err := writeResponsePackets(ctx, r.Stream, failure); err != nil {
			return CommandResponse{}, err
		}
		r.Connection = connection
		return failure, nil
	}
	connection.Username = decision.Username
	connection.Database = decision.Database
	connection.AuthPluginName = decision.AuthPluginName
	connection, authOK, err := connection.AcceptPermissiveAuth()
	if err != nil {
		return CommandResponse{}, err
	}
	authOK.Packets = clonePacketsWithSequence(authOK.Packets, packet.SequenceID+1)
	if err := writeResponsePackets(ctx, r.Stream, authOK); err != nil {
		return CommandResponse{}, err
	}
	r.Connection = connection
	return authOK, nil
}

// Serve performs handshake, authentication, and command processing until the session closes.
func (r *SessionRunner) Serve(ctx context.Context) error {
	if err := r.SendHandshake(ctx); err != nil {
		return err
	}
	authResponse, err := r.AcceptHandshakeResponse(ctx)
	if err != nil {
		return err
	}
	if authResponse.Kind == CommandResponseError {
		r.Connection = r.Connection.WithClosing()
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		response, err := r.ServeNextCommand(ctx)
		if err != nil {
			return err
		}
		if response.Close || r.Connection.State == ConnectionStateClosing {
			return nil
		}
	}
}

// ServeNextCommand reads, handles, and writes one command response.
func (r *SessionRunner) ServeNextCommand(ctx context.Context) (CommandResponse, error) {
	if r.Stream == nil {
		return CommandResponse{}, fmt.Errorf("mysql session runner stream is nil")
	}
	if r.Handler == nil {
		return CommandResponse{}, fmt.Errorf("mysql session runner handler is nil")
	}
	if !r.Connection.CanAcceptCommand() {
		return CommandResponse{}, fmt.Errorf("mysql connection is not ready for commands: %s", r.Connection.State)
	}
	response, err := (CommandLoop{
		Reader:       r.Stream,
		Writer:       r.Stream,
		Handler:      r.Handler,
		ConnectionID: r.Connection.ID,
		Database:     r.Connection.Database,
	}).ServeNext(ctx)
	if err != nil {
		return response, err
	}
	if response.Close {
		r.Connection = r.Connection.WithClosing()
	}
	return response, nil
}

func clonePacketsWithSequence(packets []Packet, sequenceID byte) []Packet {
	cloned := make([]Packet, len(packets))
	for i, packet := range packets {
		cloned[i] = Packet{
			SequenceID: sequenceID + byte(i),
			Payload:    append([]byte(nil), packet.Payload...),
		}
	}
	return cloned
}
